"""
SQLite database for progress tracking and deduplication.

This module provides:
- Download record storage and retrieval
- SHA256 deduplication index
- Scan progress persistence for resumable downloads
"""

import sqlite3
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class DownloadStatus:
    """Download status constants."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED_DUPLICATE = "skipped_duplicate"
    SKIPPED_LICENSE = "skipped_license"


@dataclass
class ImageRecord:
    """Represents a downloaded image with full metadata."""
    id: str                           # Unique identifier: {source}_{native_id}
    source: str                       # lubw, flickr, inaturalist, waarnemingen
    source_url: str                   # Original URL
    class_id: int                     # 0-3
    class_name: str                   # Species name
    license: str                      # CC0, CC-BY, etc.
    local_path: Optional[str] = None  # Relative path in raw/
    sha256: Optional[str] = None      # Hash for deduplication
    photographer: Optional[str] = None
    downloaded_at: Optional[datetime] = None
    size_bytes: int = 0
    width: Optional[int] = None
    height: Optional[int] = None
    status: str = DownloadStatus.PENDING
    error_message: Optional[str] = None
    retry_count: int = 0
    metadata: dict = field(default_factory=dict)


@dataclass
class ProgressStats:
    """Statistics for a data source."""
    source: str
    total_found: int
    completed: int
    failed: int
    skipped_duplicate: int
    skipped_license: int
    success_rate: float
    last_run: Optional[datetime] = None


class Database:
    """
    SQLite database for pipeline state management.
    
    Handles:
    - Download tracking and progress
    - SHA256 deduplication
    - Scan position persistence
    """
    
    SCHEMA = """
    -- Main download tracking table
    CREATE TABLE IF NOT EXISTS downloads (
        id TEXT PRIMARY KEY,
        source TEXT NOT NULL,
        source_url TEXT NOT NULL UNIQUE,
        local_path TEXT,
        sha256 TEXT,
        class_id INTEGER NOT NULL,
        class_name TEXT NOT NULL,
        license TEXT,
        photographer TEXT,
        status TEXT NOT NULL,
        error_message TEXT,
        retry_count INTEGER DEFAULT 0,
        size_bytes INTEGER,
        width INTEGER,
        height INTEGER,
        downloaded_at TIMESTAMP,
        metadata_json TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_downloads_source ON downloads(source);
    CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);
    CREATE INDEX IF NOT EXISTS idx_downloads_class ON downloads(class_id);
    CREATE INDEX IF NOT EXISTS idx_downloads_sha256 ON downloads(sha256);

    -- Scan progress tracking
    CREATE TABLE IF NOT EXISTS scan_progress (
        source TEXT PRIMARY KEY,
        last_scanned_id TEXT,
        last_scanned_date TEXT,
        total_found INTEGER DEFAULT 0,
        total_downloaded INTEGER DEFAULT 0,
        last_run TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- SHA256 deduplication index
    CREATE TABLE IF NOT EXISTS sha256_index (
        sha256 TEXT PRIMARY KEY,
        first_download_id TEXT REFERENCES downloads(id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    def __init__(self, db_path: str | Path):
        """Initialize database connection and create tables if needed."""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()
    
    @contextmanager
    def _get_cursor(self):
        """Get a database cursor with automatic commit/rollback."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
        return self._conn
    
    def _init_db(self):
        """Initialize database schema."""
        with self._get_cursor() as cursor:
            cursor.executescript(self.SCHEMA)
        logger.info(f"Database initialized at {self.db_path}")
    
    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
    
    # === Download Record Operations ===
    
    def record_download(self, record: ImageRecord) -> None:
        """Insert or update a download record."""
        with self._get_cursor() as cursor:
            cursor.execute("""
                INSERT OR REPLACE INTO downloads (
                    id, source, source_url, local_path, sha256, class_id,
                    class_name, license, photographer, status, error_message,
                    retry_count, size_bytes, width, height, downloaded_at,
                    metadata_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                record.id, record.source, record.source_url, record.local_path,
                record.sha256, record.class_id, record.class_name, record.license,
                record.photographer, record.status, record.error_message,
                record.retry_count, record.size_bytes, record.width, record.height,
                record.downloaded_at.isoformat() if record.downloaded_at else None,
                json.dumps(record.metadata)
            ))
            
            # Update SHA256 index if we have a hash
            if record.sha256 and record.status == DownloadStatus.COMPLETED:
                cursor.execute("""
                    INSERT OR IGNORE INTO sha256_index (sha256, first_download_id)
                    VALUES (?, ?)
                """, (record.sha256, record.id))
    
    def get_download(self, download_id: str) -> Optional[Dict]:
        """Get a download record by ID."""
        with self._get_cursor() as cursor:
            cursor.execute("SELECT * FROM downloads WHERE id = ?", (download_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_download_by_url(self, url: str) -> Optional[Dict]:
        """Get a download record by source URL."""
        with self._get_cursor() as cursor:
            cursor.execute("SELECT * FROM downloads WHERE source_url = ?", (url,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_downloads_by_status(self, source: str, status: str) -> List[Dict]:
        """Get all downloads for a source with a specific status."""
        with self._get_cursor() as cursor:
            cursor.execute(
                "SELECT * FROM downloads WHERE source = ? AND status = ?",
                (source, status)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    # === Deduplication Operations ===
    
    def sha256_exists(self, sha256: str) -> bool:
        """Check if a SHA256 hash already exists in the database."""
        with self._get_cursor() as cursor:
            cursor.execute("SELECT 1 FROM sha256_index WHERE sha256 = ?", (sha256,))
            return cursor.fetchone() is not None
    
    def url_exists(self, url: str) -> bool:
        """Check if a URL has already been downloaded."""
        with self._get_cursor() as cursor:
            cursor.execute("SELECT 1 FROM downloads WHERE source_url = ?", (url,))
            return cursor.fetchone() is not None
    
    # === Scan Progress Operations ===
    
    def save_scan_progress(
        self, 
        source: str, 
        last_id: Optional[str] = None,
        last_date: Optional[str] = None,
        total_found: Optional[int] = None,
        total_downloaded: Optional[int] = None
    ) -> None:
        """Save scan progress for a source."""
        with self._get_cursor() as cursor:
            # Build update query dynamically
            updates = ["updated_at = CURRENT_TIMESTAMP", "last_run = CURRENT_TIMESTAMP"]
            params = []
            
            if last_id is not None:
                updates.append("last_scanned_id = ?")
                params.append(last_id)
            if last_date is not None:
                updates.append("last_scanned_date = ?")
                params.append(last_date)
            if total_found is not None:
                updates.append("total_found = ?")
                params.append(total_found)
            if total_downloaded is not None:
                updates.append("total_downloaded = ?")
                params.append(total_downloaded)
            
            params.append(source)
            
            cursor.execute(f"""
                INSERT INTO scan_progress (source) VALUES (?)
                ON CONFLICT(source) DO UPDATE SET {', '.join(updates)}
            """, params)
    
    def get_scan_progress(self, source: str) -> Optional[Dict]:
        """Get scan progress for a source."""
        with self._get_cursor() as cursor:
            cursor.execute("SELECT * FROM scan_progress WHERE source = ?", (source,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    # === Statistics ===
    
    def get_stats(self, source: str) -> ProgressStats:
        """Get statistics for a source."""
        with self._get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = 'skipped_duplicate' THEN 1 ELSE 0 END) as dup,
                    SUM(CASE WHEN status = 'skipped_license' THEN 1 ELSE 0 END) as lic
                FROM downloads WHERE source = ?
            """, (source,))
            row = cursor.fetchone()
            
            # Get last run time
            progress = self.get_scan_progress(source)
            last_run = None
            if progress and progress.get("last_run"):
                try:
                    last_run = datetime.fromisoformat(progress["last_run"])
                except (ValueError, TypeError):
                    pass
            
            total = row["total"] or 0
            completed = row["completed"] or 0
            failed = row["failed"] or 0
            dup = row["dup"] or 0
            lic = row["lic"] or 0
            success_rate = completed / total if total > 0 else 0.0
            
            return ProgressStats(
                source=source,
                total_found=total,
                completed=completed,
                failed=failed,
                skipped_duplicate=dup,
                skipped_license=lic,
                success_rate=success_rate,
                last_run=last_run
            )
    
    def get_all_stats(self) -> Dict[str, ProgressStats]:
        """Get statistics for all sources."""
        return {source: self.get_stats(source) for source in ["lubw", "flickr", "inaturalist", "waarnemingen"]}
    
    def print_summary(self):
        """Print a summary table to console."""
        print("\n" + "=" * 75)
        print("DOWNLOAD SUMMARY")
        print("=" * 75)
        print(f"{'Source':<15} {'Found':>8} {'OK':>8} {'Fail':>8} {'Skip':>8} {'Rate':>8}")
        print("-" * 75)
        
        total_found = 0
        total_ok = 0
        total_fail = 0
        total_skip = 0
        
        for source in ["lubw", "flickr", "inaturalist", "waarnemingen"]:
            stats = self.get_stats(source)
            if stats.total_found > 0:
                skip = stats.skipped_duplicate + stats.skipped_license
                print(f"{stats.source:<15} {stats.total_found:>8} "
                      f"{stats.completed:>8} {stats.failed:>8} "
                      f"{skip:>8} {stats.success_rate:>7.1%}")
                total_found += stats.total_found
                total_ok += stats.completed
                total_fail += stats.failed
                total_skip += skip
        
        print("-" * 75)
        if total_found > 0:
            total_rate = total_ok / total_found
            print(f"{'TOTAL':<15} {total_found:>8} {total_ok:>8} {total_fail:>8} {total_skip:>8} {total_rate:>7.1%}")
        print("=" * 75 + "\n")


class ProgressTracker:
    """
    High-level progress tracking helper.
    
    Wraps Database with convenience methods for tracking downloads.
    """
    
    def __init__(self, db_path: str | Path):
        self.db = Database(db_path)
    
    def start_download(self, record: ImageRecord) -> ImageRecord:
        """Mark a download as in progress."""
        record.status = DownloadStatus.IN_PROGRESS
        self.db.record_download(record)
        return record
    
    def complete_download(self, record: ImageRecord) -> ImageRecord:
        """Mark a download as completed."""
        record.status = DownloadStatus.COMPLETED
        record.downloaded_at = datetime.now()
        self.db.record_download(record)
        return record
    
    def fail_download(self, record: ImageRecord, error: str) -> ImageRecord:
        """Mark a download as failed."""
        record.status = DownloadStatus.FAILED
        record.error_message = error
        record.retry_count += 1
        self.db.record_download(record)
        return record
    
    def skip_duplicate(self, record: ImageRecord) -> ImageRecord:
        """Mark a download as skipped due to duplicate."""
        record.status = DownloadStatus.SKIPPED_DUPLICATE
        self.db.record_download(record)
        return record
    
    def skip_license(self, record: ImageRecord) -> ImageRecord:
        """Mark a download as skipped due to license."""
        record.status = DownloadStatus.SKIPPED_LICENSE
        self.db.record_download(record)
        return record
    
    def close(self):
        """Close the database connection."""
        self.db.close()