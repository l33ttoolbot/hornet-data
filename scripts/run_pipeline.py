"""
Main orchestration script for Hornet-Data Pipeline.

Run the complete data acquisition pipeline:
    python -m scripts.run_pipeline

Or use the CLI for more control:
    python -m scripts.cli --help
"""

import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

# Setup path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent))

from core.config import PipelineConfig, SOURCES, CLASSES, get_config
from core.database import Database


def setup_logging(verbose: bool = False, log_file: str = None):
    """Setup logging configuration."""
    handlers = [logging.StreamHandler()]
    
    if log_file:
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        handlers.append(logging.FileHandler(log_dir / log_file))
    
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers
    )


def run_lubw_scanner(config: PipelineConfig, db: Database, start_date: tuple, dry_run: bool = False):
    """Run the LUBW/Convotis scanner."""
    from downloaders.lubw_scanner import LUBWScanner
    
    logging.info("Starting LUBW/Convotis scan")
    
    scanner = LUBWScanner(
        config=SOURCES["lubw"],
        pipeline_config=config,
        db=db,
        start_date=start_date
    )
    
    try:
        if dry_run:
            count = 0
            for record in scanner.scan():
                count += 1
                logging.debug(f"Found: {record.source_url}")
            logging.info(f"LUBW dry-run complete: {count} images found")
        else:
            for result in scanner.process():
                if result.success:
                    logging.info(f"Downloaded: {result.local_path}")
                else:
                    logging.warning(f"Failed: {result.record.source_url} - {result.error_message}")
            
            stats = scanner.get_stats()
            logging.info(f"LUBW complete: {stats.completed}/{stats.total_found} downloaded")
    
    finally:
        scanner.close()


def run_flickr_downloader(config: PipelineConfig, db: Database, api_key: str, per_class: int, dry_run: bool = False):
    """Run the Flickr downloader."""
    from downloaders.flickr_downloader import FlickrDownloader
    
    logging.info("Starting Flickr download")
    
    downloader = FlickrDownloader(
        api_key=api_key,
        config=SOURCES["flickr"],
        pipeline_config=config,
        db=db,
        per_class=per_class
    )
    
    try:
        if dry_run:
            count = 0
            for record in downloader.scan():
                count += 1
                logging.debug(f"Found: {record.source_url}")
            logging.info(f"Flickr dry-run complete: {count} images found")
        else:
            for result in downloader.process():
                if result.success:
                    logging.info(f"Downloaded: {result.local_path}")
                else:
                    logging.warning(f"Failed: {result.record.source_url} - {result.error_message}")
            
            stats = downloader.get_stats()
            logging.info(f"Flickr complete: {stats.completed}/{stats.total_found} downloaded")
    
    finally:
        downloader.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Hornet-Data Pipeline")
    
    parser.add_argument(
        "--source", "-s",
        choices=["lubw", "flickr", "all"],
        default="all",
        help="Source to download from (default: all)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan only, don't download"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Log file name"
    )
    
    # LUBW options
    parser.add_argument(
        "--start-year",
        type=int,
        default=2023,
        help="Start year for LUBW scan (default: 2023)"
    )
    parser.add_argument(
        "--start-month",
        type=int,
        default=1,
        help="Start month for LUBW scan (default: 1)"
    )
    
    # Flickr options
    parser.add_argument(
        "--flickr-api-key",
        type=str,
        default=None,
        help="Flickr API key (or set FLICKR_API_KEY)"
    )
    parser.add_argument(
        "--per-class",
        type=int,
        default=200,
        help="Images per class for Flickr (default: 200)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_file = args.log_file or f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    setup_logging(verbose=args.verbose, log_file=log_file)
    
    logging.info("=" * 60)
    logging.info("Hornet-Data Pipeline")
    logging.info("=" * 60)
    
    # Initialize configuration
    config = get_config()
    config.ensure_directories()
    
    logging.info(f"Raw directory: {config.raw_dir}")
    logging.info(f"Processed directory: {config.processed_dir}")
    logging.info(f"Database: {config.db_path}")
    
    # Initialize database
    db = Database(config.db_path)
    
    try:
        # Run requested sources
        if args.source in ["lubw", "all"]:
            run_lubw_scanner(
                config=config,
                db=db,
                start_date=(args.start_year, args.start_month),
                dry_run=args.dry_run
            )
        
        if args.source in ["flickr", "all"]:
            api_key = args.flickr_api_key or os.getenv("FLICKR_API_KEY")
            if not api_key:
                logging.warning("Flickr API key not provided, skipping Flickr")
            else:
                run_flickr_downloader(
                    config=config,
                    db=db,
                    api_key=api_key,
                    per_class=args.per_class,
                    dry_run=args.dry_run
                )
        
        # Print summary
        logging.info("=" * 60)
        logging.info("Pipeline Complete - Summary")
        logging.info("=" * 60)
        db.print_summary()
    
    finally:
        db.close()


if __name__ == "__main__":
    main()