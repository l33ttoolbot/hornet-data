"""
Command-line interface for Hornet-Data Pipeline.

Usage:
    python -m scripts.cli download --source lubw
    python -m scripts.cli download --source flickr --api-key KEY
    python -m scripts.cli stats
    python -m scripts.cli scan --source lubw --dry-run
"""

import click
import logging
from pathlib import Path

# Setup path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent))

from core.config import PipelineConfig, SOURCES, CLASSES, get_config
from core.database import Database

# Configure logging
def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
@click.option("--db", type=click.Path(), default="metadata/pipeline_state.db", help="Database path")
@click.pass_context
def cli(ctx, verbose, db):
    """Hornet-Data Pipeline CLI."""
    setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["db_path"] = Path(db)


@cli.command()
@click.option("--source", type=click.Choice(["lubw", "flickr", "all"]), default="all", help="Source to download from")
@click.option("--dry-run", is_flag=True, help="Scan only, don't download")
@click.option("--start-year", type=int, default=2023, help="Start year (LUBW)")
@click.option("--start-month", type=int, default=1, help="Start month (LUBW)")
@click.option("--per-class", type=int, default=200, help="Images per class (Flickr)")
@click.option("--api-key", type=str, default=None, help="Flickr API key")
@click.pass_context
def download(ctx, source, dry_run, start_year, start_month, per_class, api_key):
    """Download images from sources."""
    config = get_config()
    config.ensure_directories()
    
    if source in ["lubw", "all"]:
        click.echo("=" * 50)
        click.echo("LUBW/Convotis Scanner")
        click.echo("=" * 50)
        
        from downloaders.lubw_scanner import LUBWScanner
        
        scanner = LUBWScanner(
            start_date=(start_year, start_month),
            db=Database(ctx.obj["db_path"])
        )
        
        try:
            if dry_run:
                count = 0
                for record in scanner.scan():
                    count += 1
                    click.echo(f"Found: {record.source_url}")
                click.echo(f"\nTotal found: {count}")
            else:
                for result in scanner.process():
                    if result.success:
                        click.secho(f"✓ Downloaded: {result.local_path}", fg="green")
                    else:
                        click.secho(f"✗ Failed: {result.error_message}", fg="red")
                scanner.print_summary()
        finally:
            scanner.close()
    
    if source in ["flickr", "all"]:
        click.echo("=" * 50)
        click.echo("Flickr Downloader")
        click.echo("=" * 50)
        
        import os
        flickr_key = api_key or os.getenv("FLICKR_API_KEY")
        
        if not flickr_key:
            click.secho("Error: Flickr API key required. Set FLICKR_API_KEY or use --api-key", fg="red")
            return
        
        from downloaders.flickr_downloader import FlickrDownloader
        
        downloader = FlickrDownloader(
            api_key=flickr_key,
            per_class=per_class,
            db=Database(ctx.obj["db_path"])
        )
        
        try:
            if dry_run:
                count = 0
                for record in downloader.scan():
                    count += 1
                    click.echo(f"Found: {record.source_url} ({record.class_name}, {record.license})")
                click.echo(f"\nTotal found: {count}")
            else:
                for result in downloader.process():
                    if result.success:
                        click.secho(f"✓ Downloaded: {result.local_path}", fg="green")
                    else:
                        click.secho(f"✗ Failed: {result.error_message}", fg="red")
                downloader.print_summary()
        finally:
            downloader.close()


@cli.command()
@click.pass_context
def stats(ctx):
    """Show download statistics."""
    db = Database(ctx.obj["db_path"])
    db.print_summary()
    db.close()


@cli.command()
@click.option("--source", type=click.Choice(["lubw", "flickr"]), required=True, help="Source to scan")
@click.option("--start-year", type=int, default=2023, help="Start year (LUBW)")
@click.option("--start-month", type=int, default=1, help="Start month (LUBW)")
@click.option("--class", "class_name", type=click.Choice(list(CLASSES.values())), help="Class to search (Flickr)")
@click.pass_context
def scan(ctx, source, start_year, start_month, class_name):
    """Scan for images without downloading (dry run)."""
    config = get_config()
    
    if source == "lubw":
        from downloaders.lubw_scanner import LUBWScanner
        
        scanner = LUBWScanner(start_date=(start_year, start_month))
        
        count = 0
        for record in scanner.scan():
            count += 1
            click.echo(f"Found: {record.source_url}")
        
        click.echo(f"\nTotal found: {count}")
    
    elif source == "flickr":
        import os
        api_key = os.getenv("FLICKR_API_KEY")
        
        if not api_key:
            click.secho("Error: Set FLICKR_API_KEY environment variable", fg="red")
            return
        
        from downloaders.flickr_downloader import FlickrDownloader
        
        downloader = FlickrDownloader(api_key=api_key)
        
        count = 0
        for record in downloader.scan(class_name=class_name):
            count += 1
            click.echo(f"Found: {record.source_url} ({record.class_name}, {record.license})")
        
        click.echo(f"\nTotal found: {count}")


@cli.command()
def classes():
    """List available classes."""
    click.echo("\nTarget Classes:")
    click.echo("-" * 40)
    for class_id, name in CLASSES.items():
        click.echo(f"  {class_id}: {name}")
    click.echo()


@cli.command()
def sources():
    """List available sources."""
    click.echo("\nData Sources:")
    click.echo("-" * 40)
    for source_id, config in SOURCES.items():
        status = "enabled" if config.enabled else "disabled"
        click.echo(f"  {config.name} ({source_id}): {status}")
    click.echo()


@cli.command()
@click.option("--force", is_flag=True, help="Force initialization (reset database)")
@click.pass_context
def init(ctx, force):
    """Initialize the pipeline (create directories, database)."""
    config = get_config()
    config.ensure_directories()
    
    db = Database(ctx.obj["db_path"])
    
    if force:
        click.echo("Database initialized (tables created/reset)")
    else:
        click.echo("Pipeline initialized")
    
    click.echo(f"  Raw directory: {config.raw_dir}")
    click.echo(f"  Processed directory: {config.processed_dir}")
    click.echo(f"  Metadata directory: {config.metadata_dir}")
    click.echo(f"  Database: {config.db_path}")
    
    db.close()


if __name__ == "__main__":
    cli()