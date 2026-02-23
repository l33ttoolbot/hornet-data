"""
LUBW/Convotis Sequential URL Scanner.

This module downloads images from the LUBW (Landesamt für Umwelt Baden-Württemberg)
Asian Hornet reporting portal hosted at gmp.convotis.com.

URL Pattern: https://gmp.convotis.com/documents/d/global/anhang_YYYY-MM-NNNN-[ext]

All images are presumed to be Vespa velutina (Asian Hornet) as this is the 
reporting portal for Asian hornet sightings.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Tuple

import requests

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.config import SourceConfig, SOURCES, LUBW_START_DATE, LUBW_EXPECTED_CLASS, CLASS_NAME_TO_ID
from core.database import ImageRecord, DownloadStatus, Database
from core.downloader import BaseDownloader, DownloadResult, SequentialScanner
from core.utils import (
    RateLimiter,
    LicenseValidator,
    create_session,
    download_file,
    check_url_exists,
    get_file_extension,
    generate_lubw_url
)

logger = logging.getLogger(__name__)


class LUBWScanner(BaseDownloader):
    """
    Downloader for LUBW/Convotis sequential URLs.
    
    Scans URLs in the format:
    https://gmp.convotis.com/documents/d/global/anhang_YYYY-MM-NNNN.ext
    
    Strategy:
    1. Iterate through months from start date to present
    2. For each month, try numbers 0001-9999 until consecutive 404s
    3. Try multiple extensions (jpg, jpeg, png, gif)
    4. Use HEAD requests first, only GET if 200
    """
    
    SOURCE_ID = "lubw"
    URL_BASE = "https://gmp.convotis.com/documents/d/global"
    URL_PATTERN = "https://gmp.convotis.com/documents/d/global/anhang_{year:04d}-{month:02d}-{number:04d}.{ext}"
    
    EXTENSIONS = ["jpg", "jpeg", "png", "gif", "JPG", "JPEG", "PNG", "GIF"]
    
    # All LUBW images are Asian Hornets
    CLASS_ID = 0
    CLASS_NAME = "vespa_velutina"
    
    # Government data, assume OK for research
    LICENSE = "unknown"
    
    def __init__(
        self,
        config: Optional[SourceConfig] = None,
        pipeline_config=None,
        db: Optional[Database] = None,
        start_date: Optional[Tuple[int, int]] = None,
        end_date: Optional[Tuple[int, int]] = None,
        max_consecutive_404: int = 10
    ):
        """
        Initialize LUBW scanner.
        
        Args:
            config: Source configuration (uses default if None)
            pipeline_config: Pipeline configuration
            db: Database instance
            start_date: Start scanning from (year, month)
            end_date: End scanning at (year, month)
            max_consecutive_404: Stop month after this many consecutive misses
        """
        if config is None:
            config = SOURCES.get("lubw")
        
        super().__init__(config, pipeline_config, db)
        
        self.start_date = start_date or LUBW_START_DATE
        self.end_date = end_date
        self.max_consecutive_404 = max_consecutive_404
        
        # Sequential scanner helper
        self.scanner = SequentialScanner(
            url_pattern=self.URL_PATTERN,
            extensions=self.EXTENSIONS,
            start_year=self.start_date[0],
            start_month=self.start_date[1],
            max_consecutive_404=max_consecutive_404,
            session=self.session
        )
    
    @property
    def source_id(self) -> str:
        return self.SOURCE_ID
    
    def scan(self, **kwargs) -> Iterator[ImageRecord]:
        """
        Scan for LUBW images.
        
        Yields:
            ImageRecord for each discovered URL
        """
        logger.info(f"Starting LUBW scan from {self.start_date[0]}-{self.start_date[1]:02d}")
        
        # Get last scanned position for resumable scans
        progress = self.db.get_scan_progress(self.source_id)
        resume_date = None
        
        if progress and progress.get("last_scanned_date"):
            try:
                date_str = progress["last_scanned_date"]
                year, month = map(int, date_str.split("-"))
                resume_date = (year, month)
                logger.info(f"Resuming from {date_str}")
            except (ValueError, TypeError):
                pass
        
        # Determine date range
        start = resume_date or self.start_date
        end = self.end_date or (datetime.now().year, datetime.now().month)
        
        # Scan each month
        current_year, current_month = start
        end_year, end_month = end
        
        while (current_year, current_month) <= (end_year, end_month):
            logger.info(f"Scanning {current_year}-{current_month:02d}")
            
            found_in_month = 0
            consecutive_404 = 0
            number = 1
            
            while consecutive_404 < self.max_consecutive_404:
                found_number = False
                
                for ext in self.EXTENSIONS:
                    url = self.URL_PATTERN.format(
                        year=current_year,
                        month=current_month,
                        number=number,
                        ext=ext
                    )
                    
                    # Check if URL exists
                    self.rate_limiter.wait()
                    exists, headers = check_url_exists(url, self.session)
                    
                    if exists:
                        found_number = True
                        consecutive_404 = 0
                        found_in_month += 1
                        
                        # Create record
                        record = ImageRecord(
                            id=f"lubw_{current_year}{current_month:02d}_{number:04d}",
                            source=self.SOURCE_ID,
                            source_url=url,
                            class_id=self.CLASS_ID,
                            class_name=self.CLASS_NAME,
                            license=self.LICENSE,
                            metadata={
                                "year": current_year,
                                "month": current_month,
                                "number": number,
                                "extension": ext,
                                "content_type": headers.get("content_type", ""),
                                "content_length": headers.get("content_length", 0)
                            }
                        )
                        
                        logger.debug(f"Found: {url}")
                        yield record
                        break  # Found this number, move to next
                
                if not found_number:
                    consecutive_404 += 1
                else:
                    found_in_month += 1
                
                number += 1
            
            # Update scan progress
            self.db.save_scan_progress(
                source=self.source_id,
                last_date=f"{current_year}-{current_month:02d}",
                last_id=f"{number:04d}"
            )
            
            logger.info(f"Found {found_in_month} images in {current_year}-{current_month:02d}")
            
            # Move to next month
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
    
    def download_single(self, record: ImageRecord) -> Optional[Path]:
        """
        Download a single LUBW image.
        
        Args:
            record: ImageRecord with source_url
        
        Returns:
            Local Path if successful
        """
        # Determine output path
        metadata = record.metadata
        year = metadata.get("year")
        month = metadata.get("month")
        number = metadata.get("number", 1)
        ext = metadata.get("extension", "jpg")
        
        # Create subfolder by year-month
        subfolder = f"{year}-{month:02d}" if year and month else ""
        
        # Output path: raw/lubw/vespa_velutina/YYYY-MM/NNNN.ext
        output_dir = self.pipeline.raw_dir / self.source_id / self.CLASS_NAME / subfolder
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = output_dir / f"{number:04d}.{ext}"
        
        # Download
        success, size = download_file(
            record.source_url,
            output_path,
            self.session
        )
        
        if success:
            record.size_bytes = size
            return output_path
        
        return None
    
    def scan_and_download(self, **kwargs) -> Iterator[DownloadResult]:
        """
        Convenience method to scan and download in one pass.
        
        Yields:
            DownloadResult for each processed image
        """
        yield from self.process(**kwargs)


def main():
    """CLI entry point for LUBW scanner."""
    import argparse
    
    parser = argparse.ArgumentParser(description="LUBW/Convotis Image Scanner")
    parser.add_argument(
        "--start-year", type=int, default=2023,
        help="Start year (default: 2023)"
    )
    parser.add_argument(
        "--start-month", type=int, default=1,
        help="Start month (default: 1)"
    )
    parser.add_argument(
        "--end-year", type=int, default=None,
        help="End year (default: current)"
    )
    parser.add_argument(
        "--end-month", type=int, default=None,
        help="End month (default: current)"
    )
    parser.add_argument(
        "--max-404", type=int, default=10,
        help="Max consecutive 404s before stopping month (default: 10)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Scan only, don't download"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # Determine end date
    if args.end_year and args.end_month:
        end_date = (args.end_year, args.end_month)
    else:
        end_date = None
    
    # Create scanner
    scanner = LUBWScanner(
        start_date=(args.start_year, args.start_month),
        end_date=end_date,
        max_consecutive_404=args.max_404
    )
    
    try:
        if args.dry_run:
            # Just scan and count
            count = 0
            for record in scanner.scan():
                count += 1
                print(f"Found: {record.source_url}")
            print(f"\nTotal found: {count}")
        else:
            # Scan and download
            for result in scanner.process():
                if result.success:
                    print(f"✓ Downloaded: {result.local_path}")
                else:
                    print(f"✗ Failed: {result.record.source_url} - {result.error_message}")
            
            scanner.print_summary()
    
    finally:
        scanner.close()


if __name__ == "__main__":
    main()