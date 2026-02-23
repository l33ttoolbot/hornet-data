"""
Base downloader module with retry and rate limiting support.

This module provides:
- Abstract base class for all downloaders
- Common download logic with error handling
- Rate limiting and retry integration
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Dict, Any

from .config import SourceConfig, PipelineConfig, get_config
from .database import Database, ImageRecord, DownloadStatus, ProgressStats
from .utils import (
    RateLimiter,
    retry_with_backoff,
    compute_sha256,
    LicenseValidator,
    create_session,
    download_file,
    check_url_exists,
    RetryConfig
)

logger = logging.getLogger(__name__)


@dataclass
class DownloadResult:
    """Result of a download attempt."""
    success: bool
    record: ImageRecord
    local_path: Optional[Path] = None
    error_message: Optional[str] = None


class BaseDownloader(ABC):
    """
    Abstract base class for all image downloaders.
    
    Provides:
    - Rate limiting
    - Retry with backoff
    - License validation
    - Duplicate detection
    - Progress tracking
    
    Subclasses must implement:
    - scan(): Yield candidate ImageRecords
    - download_single(): Download a single image
    """
    
    def __init__(
        self, 
        config: SourceConfig, 
        pipeline: Optional[PipelineConfig] = None,
        db: Optional[Database] = None
    ):
        """
        Initialize downloader.
        
        Args:
            config: Source-specific configuration
            pipeline: Pipeline configuration (uses defaults if None)
            db: Database instance for tracking (created if None)
        """
        self.config = config
        self.pipeline = pipeline or get_config()
        
        # Initialize database
        self.db = db or Database(self.pipeline.db_path)
        
        # Initialize helpers
        self.rate_limiter = RateLimiter(config.rate_limit_per_second)
        self.license_validator = LicenseValidator(config.license_whitelist)
        self.retry_config = RetryConfig(max_retries=config.max_retries)
        
        # HTTP session
        self.session = create_session(timeout=config.timeout_seconds)
        
        # Setup directories
        self._setup_directories()
        
        logger.info(f"Initialized {self.config.name} downloader")
    
    def _setup_directories(self):
        """Ensure output directories exist."""
        for class_name in self.pipeline.classes.values():
            output_dir = self.pipeline.raw_dir / self.source_id / class_name
            output_dir.mkdir(parents=True, exist_ok=True)
    
    @property
    def source_id(self) -> str:
        """Short source identifier (lowercase, for directory names)."""
        return self.config.name.lower().replace("/", "-").replace(" ", "-")
    
    @abstractmethod
    def scan(self, **kwargs) -> Iterator[ImageRecord]:
        """
        Scan for candidate images to download.
        
        Yields:
            ImageRecord for each potential download
        
        This method should gather URLs and metadata but NOT download.
        Use process() to actually download.
        """
        pass
    
    @abstractmethod
    def download_single(self, record: ImageRecord) -> Optional[Path]:
        """
        Download a single image.
        
        Args:
            record: ImageRecord with source_url and class info
        
        Returns:
            Local Path if successful, None otherwise
        
        Raises:
            May raise exceptions which will be handled by process()
        """
        pass
    
    def should_download(self, record: ImageRecord) -> tuple[bool, str]:
        """
        Check if a record should be downloaded.
        
        Args:
            record: ImageRecord to check
        
        Returns:
            Tuple of (should_download: bool, reason: str)
        """
        # Check if already downloaded (by URL)
        if self.db.url_exists(record.source_url):
            return False, "Already downloaded (URL exists)"
        
        # Check license
        if not self.license_validator.is_allowed(record.license):
            return False, f"License not allowed: {record.license}"
        
        return True, "OK"
    
    def process(self, **kwargs) -> Iterator[DownloadResult]:
        """
        Main processing loop with rate limiting, retry, dedup, and tracking.
        
        Yields:
            DownloadResult for each processed record
        
        This is the main entry point for downloading.
        """
        logger.info(f"Starting {self.config.name} download process")
        
        for record in self.scan(**kwargs):
            # Check if we should download
            should_dl, reason = self.should_download(record)
            
            if not should_dl:
                if "URL exists" in reason:
                    record.status = DownloadStatus.SKIPPED_DUPLICATE
                else:
                    record.status = DownloadStatus.SKIPPED_LICENSE
                
                record.error_message = reason
                self.db.record_download(record)
                
                yield DownloadResult(
                    success=False,
                    record=record,
                    error_message=reason
                )
                continue
            
            # Rate limit
            self.rate_limiter.wait()
            
            # Download with retry
            try:
                result = retry_with_backoff(
                    lambda: self.download_single(record),
                    config=self.retry_config
                )
                
                if result:
                    # Compute hash for deduplication
                    record.sha256 = compute_sha256(result)
                    
                    # Check for SHA256 duplicate
                    if self.db.sha256_exists(record.sha256):
                        record.status = DownloadStatus.SKIPPED_DUPLICATE
                        record.error_message = "SHA256 duplicate"
                        result.unlink()  # Delete the duplicate file
                        logger.debug(f"Skipping SHA256 duplicate: {record.source_url}")
                    else:
                        record.status = DownloadStatus.COMPLETED
                        record.local_path = str(result.relative_to(self.pipeline.raw_dir))
                        record.downloaded_at = datetime.now()
                        logger.info(f"Downloaded: {record.source_url} -> {record.local_path}")
                    
                    self.db.record_download(record)
                    
                    yield DownloadResult(
                        success=record.status == DownloadStatus.COMPLETED,
                        record=record,
                        local_path=result if record.status == DownloadStatus.COMPLETED else None,
                        error_message=record.error_message
                    )
                else:
                    # Download returned None without raising
                    record.status = DownloadStatus.FAILED
                    record.error_message = "Download returned None"
                    record.retry_count += 1
                    self.db.record_download(record)
                    
                    yield DownloadResult(
                        success=False,
                        record=record,
                        error_message=record.error_message
                    )
            
            except Exception as e:
                record.status = DownloadStatus.FAILED
                record.error_message = str(e)
                record.retry_count += 1
                self.db.record_download(record)
                
                logger.error(f"Failed to download {record.source_url}: {e}")
                
                yield DownloadResult(
                    success=False,
                    record=record,
                    error_message=str(e)
                )
    
    def get_stats(self) -> ProgressStats:
        """Get download statistics for this source."""
        return self.db.get_stats(self.source_id)
    
    def print_summary(self):
        """Print download summary for this source."""
        self.db.print_summary()
    
    def close(self):
        """Clean up resources."""
        self.session.close()
        self.db.close()


class SequentialScanner:
    """
    Helper class for sequential URL scanning.
    
    Used by LUBW and other sources with numbered/patterned URLs.
    """
    
    def __init__(
        self,
        url_pattern: str,
        extensions: list,
        start_year: int = 2023,
        start_month: int = 1,
        start_number: int = 1,
        max_consecutive_404: int = 10,
        session: Optional[Any] = None
    ):
        """
        Initialize sequential scanner.
        
        Args:
            url_pattern: URL pattern with placeholders
                        (e.g., "https://example.com/{year:04d}-{month:02d}-{num:04d}.{ext}")
            extensions: List of file extensions to try
            start_year/year/month/number: Starting values
            max_consecutive_404: Stop after this many consecutive misses
            session: requests Session to use
        """
        self.url_pattern = url_pattern
        self.extensions = extensions
        self.start_year = start_year
        self.start_month = start_month
        self.start_number = start_number
        self.max_consecutive_404 = max_consecutive_404
        self.session = session or create_session()
    
    def scan_month(
        self,
        year: int,
        month: int,
        rate_limiter: Optional[RateLimiter] = None
    ) -> Iterator[tuple[str, dict]]:
        """
        Scan all possible URLs for a given month.
        
        Yields:
            Tuple of (url, headers_dict) for each existing URL
        """
        consecutive_404 = 0
        number = 1
        
        while consecutive_404 < self.max_consecutive_404:
            for ext in self.extensions:
                url = self.url_pattern.format(
                    year=year, month=month, number=number, ext=ext
                )
                
                if rate_limiter:
                    rate_limiter.wait()
                
                exists, headers = check_url_exists(url, self.session)
                
                if exists:
                    consecutive_404 = 0
                    yield url, headers
                else:
                    consecutive_404 += 1
            
            number += 1
    
    def scan_range(
        self,
        start_date: tuple[int, int],  # (year, month)
        end_date: Optional[tuple[int, int]] = None,  # (year, month)
        rate_limiter: Optional[RateLimiter] = None
    ) -> Iterator[tuple[str, dict, tuple[int, int]]]:
        """
        Scan all months in a date range.
        
        Yields:
            Tuple of (url, headers_dict, (year, month))
        """
        import datetime
        
        start_year, start_month = start_date
        
        if end_date:
            end_year, end_month = end_date
        else:
            # Current date
            now = datetime.datetime.now()
            end_year, end_month = now.year, now.month
        
        year, month = start_year, start_month
        
        while (year, month) <= (end_year, end_month):
            logger.info(f"Scanning {year}-{month:02d}")
            
            for url, headers in self.scan_month(year, month, rate_limiter):
                yield url, headers, (year, month)
            
            # Next month
            month += 1
            if month > 12:
                month = 1
                year += 1


class ImageDownloader:
    """
    Simple image downloader with progress tracking.
    
    Downloads images from URLs to local storage with duplicate detection.
    """
    
    def __init__(
        self,
        output_dir: Path,
        db: Database,
        rate_limiter: RateLimiter,
        session: Optional[Any] = None
    ):
        """
        Initialize image downloader.
        
        Args:
            output_dir: Base output directory
            db: Database for deduplication
            rate_limiter: Rate limiter instance
            session: requests Session
        """
        self.output_dir = Path(output_dir)
        self.db = db
        self.rate_limiter = rate_limiter
        self.session = session or create_session()
    
    def download(
        self,
        url: str,
        filename: str,
        subfolder: str = "",
        expected_class: str = ""
    ) -> Optional[Path]:
        """
        Download an image.
        
        Args:
            url: Image URL
            filename: Output filename (without extension)
            subfolder: Subfolder within output_dir
            expected_class: Class subfolder
        
        Returns:
            Path to downloaded file, or None if failed
        """
        # Build output path
        if expected_class:
            output_path = self.output_dir / expected_class / subfolder
        else:
            output_path = self.output_dir / subfolder
        
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Rate limit
        self.rate_limiter.wait()
        
        # Check URL exists and get content type
        exists, headers = check_url_exists(url, self.session)
        if not exists:
            logger.warning(f"URL does not exist: {url}")
            return None
        
        # Determine extension
        content_type = headers.get("content_type", "")
        ext = self._get_extension(content_type, url)
        
        # Full output path
        full_path = output_path / f"{filename}.{ext}"
        
        # Download
        success, size = download_file(url, full_path, self.session)
        
        if success:
            return full_path
        return None
    
    def _get_extension(self, content_type: str, url: str) -> str:
        """Get file extension from content type or URL."""
        from .utils import get_file_extension
        
        # Try content type first
        if content_type:
            ext = get_file_extension(content_type)
            if ext != "jpg":  # Default
                return ext
        
        # Fall back to URL
        url_lower = url.lower()
        for ext in ["jpg", "jpeg", "png", "gif", "webp"]:
            if f".{ext}" in url_lower:
                return ext
        
        return "jpg"  # Default