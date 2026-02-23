"""
Flickr API Downloader.

This module downloads Creative Commons licensed images from Flickr API
for the hornet dataset.

Supported license filters:
- CC0 (license_id=9,10): Public Domain
- CC-BY (license_id=4): Attribution
- CC-BY-SA (license_id=5): Attribution-ShareAlike

Search queries target species names: Vespa velutina, Vespa crabro, etc.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Dict, List

import requests

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.config import (
    SourceConfig, SOURCES, CLASSES, CLASS_NAME_TO_ID,
    FLICKR_ALLOWED_LICENSE_IDS, FLICKR_LICENSES
)
from core.database import ImageRecord, DownloadStatus, Database
from core.downloader import BaseDownloader, DownloadResult
from core.utils import (
    RateLimiter,
    LicenseValidator,
    create_session,
    download_file,
    RetryConfig
)

logger = logging.getLogger(__name__)


# Species search queries
SPECIES_QUERIES = {
    "vespa_velutina": [
        "Vespa velutina",
        "Asian hornet",
        "Asiatische Hornisse",
        "Vespa velutina nigrithorax",
        "yellow-legged hornet",
    ],
    "vespa_crabro": [
        "Vespa crabro",
        "European hornet",
        "Europäische Hornisse",
    ],
    "vespula_vulgaris": [
        "Vespula vulgaris",
        "Common wasp",
        "Gemeine Wespe",
    ],
    "apis_mellifera": [
        "Apis mellifera",
        "Honey bee",
        "Honigbiene",
        "Western honey bee",
    ],
}


class FlickrDownloader(BaseDownloader):
    """
    Downloader for Flickr API images.
    
    Uses Flickr's REST API to search for CC-licensed insect images
    and download them for the training dataset.
    
    Features:
    - License filtering (CC0, CC-BY, CC-BY-SA only)
    - Per-class image limits
    - Rate limiting (Flickr is strict)
    - Full metadata preservation
    """
    
    SOURCE_ID = "flickr"
    API_BASE = "https://www.flickr.com/services/rest/"
    
    # Default per-class image target
    DEFAULT_PER_CLASS = 200
    MAX_PER_CLASS = 500
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        config: Optional[SourceConfig] = None,
        pipeline_config=None,
        db: Optional[Database] = None,
        per_class: int = DEFAULT_PER_CLASS
    ):
        """
        Initialize Flickr downloader.
        
        Args:
            api_key: Flickr API key (reads from env if not provided)
            config: Source configuration
            pipeline_config: Pipeline configuration
            db: Database instance
            per_class: Target images per class
        """
        if config is None:
            config = SOURCES.get("flickr")
        
        super().__init__(config, pipeline_config, db)
        
        # API key
        self.api_key = api_key or os.getenv("FLICKR_API_KEY")
        if not self.api_key:
            logger.warning("No Flickr API key provided. Set FLICKR_API_KEY environment variable.")
        
        self.per_class = min(per_class, self.MAX_PER_CLASS)
        
        # Track downloaded counts per class
        self._counts = {name: 0 for name in CLASSES.values()}
    
    @property
    def source_id(self) -> str:
        return self.SOURCE_ID
    
    def _api_request(self, method: str, params: Dict) -> Optional[Dict]:
        """
        Make a Flickr API request.
        
        Args:
            method: Flickr API method name
            params: API parameters
        
        Returns:
            Response dict or None on failure
        """
        if not self.api_key:
            raise ValueError("Flickr API key required")
        
        request_params = {
            "method": method,
            "api_key": self.api_key,
            "format": "json",
            "nojsoncallback": 1,
            **params
        }
        
        self.rate_limiter.wait()
        
        try:
            response = self.session.get(self.API_BASE, params=request_params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get("stat") != "ok":
                logger.error(f"Flickr API error: {data.get('message', 'Unknown error')}")
                return None
            
            return data
        
        except requests.RequestException as e:
            logger.error(f"Flickr API request failed: {e}")
            return None
    
    def search_photos(
        self,
        query: str,
        license_ids: Optional[List[int]] = None,
        per_page: int = 100,
        page: int = 1,
        sort: str = "relevance"
    ) -> Optional[Dict]:
        """
        Search for photos on Flickr.
        
        Args:
            query: Search query
            license_ids: List of license IDs to filter
            per_page: Results per page (max 500)
            page: Page number
            sort: Sort order (relevance, interestingness-desc, date-posted-desc)
        
        Returns:
            API response dict or None
        """
        if license_ids is None:
            license_ids = FLICKR_ALLOWED_LICENSE_IDS
        
        params = {
            "text": query,
            "license": ",".join(map(str, license_ids)),
            "per_page": per_page,
            "page": page,
            "sort": sort,
            "media": "photos",
            "extras": "license,owner_name,url_o,url_l,url_m,date_upload,tags"
        }
        
        return self._api_request("flickr.photos.search", params)
    
    def get_photo_info(self, photo_id: str) -> Optional[Dict]:
        """
        Get detailed info for a photo.
        
        Args:
            photo_id: Flickr photo ID
        
        Returns:
            Photo info dict or None
        """
        params = {
            "photo_id": photo_id,
            "extras": "license,owner_name,url_o,url_l,url_m,date_upload,tags,description"
        }
        
        data = self._api_request("flickr.photos.getInfo", params)
        if data and "photo" in data:
            return data["photo"]
        return None
    
    def get_best_url(self, photo: Dict) -> Optional[str]:
        """
        Get the best available image URL from a photo dict.
        
        Prefers original (url_o), then large (url_l), then medium (url_m).
        
        Args:
            photo: Photo dict from API
        
        Returns:
            Image URL or None
        """
        # Try original size first
        if "url_o" in photo:
            return photo["url_o"]
        
        # Try large size
        if "url_l" in photo:
            return photo["url_l"]
        
        # Try medium size
        if "url_m" in photo:
            return photo["url_m"]
        
        # Construct from photo ID and farm/server info
        if all(k in photo for k in ["farm", "server", "id", "secret"]):
            # Original if we have original secret
            if "originalsecret" in photo:
                return f"https://farm{photo['farm']}.staticflickr.com/{photo['server']}/{photo['id']}_{photo['originalsecret']}_o.jpg"
            # Large
            return f"https://farm{photo['farm']}.staticflickr.com/{photo['server']}/{photo['id']}_{photo['secret']}_b.jpg"
        
        return None
    
    def get_license_name(self, license_id: int) -> str:
        """Convert Flickr license ID to name."""
        return FLICKR_LICENSES.get(license_id, "unknown")
    
    def scan(self, class_name: Optional[str] = None, **kwargs) -> Iterator[ImageRecord]:
        """
        Scan for Flickr images by searching for species.
        
        Args:
            class_name: Specific class to search (None = all classes)
        
        Yields:
            ImageRecord for each found image
        """
        if not self.api_key:
            logger.error(" Flickr API key required. Set FLICKR_API_KEY.")
            return
        
        classes_to_search = [class_name] if class_name else list(CLASSES.values())
        
        for species in classes_to_search:
            if self._counts.get(species, 0) >= self.per_class:
                logger.info(f"Already have {self.per_class} images for {species}, skipping")
                continue
            
            class_id = CLASS_NAME_TO_ID.get(species)
            if class_id is None:
                logger.warning(f"Unknown class: {species}")
                continue
            
            queries = SPECIES_QUERIES.get(species, [species])
            logger.info(f"Searching for {species} with queries: {queries}")
            
            for query in queries:
                if self._counts.get(species, 0) >= self.per_class:
                    break
                
                # Paginate through results
                page = 1
                per_page = 100
                total_found = 0
                
                while True:
                    data = self.search_photos(
                        query=query,
                        per_page=per_page,
                        page=page,
                        sort="relevance"
                    )
                    
                    if not data or "photos" not in data:
                        break
                    
                    photos = data["photos"].get("photo", [])
                    total = data["photos"].get("total", 0)
                    
                    if not photos:
                        break
                    
                    logger.info(f"Page {page}: Found {len(photos)} photos (total: {total})")
                    
                    for photo in photos:
                        # Check license
                        license_id = photo.get("license", 0)
                        if license_id not in FLICKR_ALLOWED_LICENSE_IDS:
                            logger.debug(f"Skipping photo {photo['id']}: license {license_id} not allowed")
                            continue
                        
                        # Get URL
                        url = self.get_best_url(photo)
                        if not url:
                            logger.debug(f"No URL for photo {photo['id']}")
                            continue
                        
                        # License name
                        license_name = self.get_license_name(license_id)
                        
                        # Photographer
                        photographer = photo.get("ownername", photo.get("owner", "unknown"))
                        
                        record = ImageRecord(
                            id=f"flickr_{photo['id']}",
                            source=self.SOURCE_ID,
                            source_url=url,
                            class_id=class_id,
                            class_name=species,
                            license=license_name,
                            photographer=photographer,
                            metadata={
                                "flickr_id": photo["id"],
                                "flickr_owner": photo.get("owner"),
                                "query": query,
                                "license_id": license_id,
                                "date_upload": photo.get("dateupload"),
                                "tags": photo.get("tags", ""),
                            }
                        )
                        
                        total_found += 1
                        self._counts[species] = self._counts.get(species, 0) + 1
                        
                        yield record
                        
                        if self._counts.get(species, 0) >= self.per_class:
                            break
                    
                    # Check if we should continue to next page
                    if len(photos) < per_page:
                        break
                    
                    page += 1
                    
                    # Flickr limits to 4000 results (40 pages of 100)
                    if page > 40:
                        break
    
    def download_single(self, record: ImageRecord) -> Optional[Path]:
        """
        Download a single Flickr image.
        
        Args:
            record: ImageRecord with source_url
        
        Returns:
            Local Path if successful
        """
        # Output path: raw/flickr/{class_name}/{flickr_id}.{ext}
        flickr_id = record.metadata.get("flickr_id", record.id)
        
        # Determine extension from URL
        url = record.source_url
        ext = "jpg"
        if ".png" in url.lower():
            ext = "png"
        elif ".gif" in url.lower():
            ext = "gif"
        
        output_dir = self.pipeline.raw_dir / self.source_id / record.class_name
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = output_dir / f"{flickr_id}.{ext}"
        
        # Skip if already exists
        if output_path.exists():
            logger.debug(f"Skipping existing: {output_path}")
            return output_path
        
        # Download
        success, size = download_file(url, output_path, self.session)
        
        if success:
            record.size_bytes = size
            return output_path
        
        return None
    
    def download_class(self, class_name: str, limit: Optional[int] = None) -> Iterator[DownloadResult]:
        """
        Download images for a specific class.
        
        Args:
            class_name: Species name (e.g., "vespa_velutina")
            limit: Max images to download
        
        Yields:
            DownloadResult for each processed image
        """
        if limit:
            self.per_class = min(limit, self.MAX_PER_CLASS)
        
        yield from self.process(class_name=class_name)


def main():
    """CLI entry point for Flickr downloader."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Flickr Image Downloader")
    parser.add_argument(
        "--class", dest="class_name", type=str,
        choices=list(CLASSES.values()),
        help="Specific class to download"
    )
    parser.add_argument(
        "--per-class", type=int, default=200,
        help="Images per class (default: 200)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Search only, don't download"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--api-key", type=str, default=None,
        help="Flickr API key (or set FLICKR_API_KEY env var)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # Check API key
    api_key = args.api_key or os.getenv("FLICKR_API_KEY")
    if not api_key:
        print("Error: Flickr API key required. Set FLICKR_API_KEY or use --api-key")
        return 1
    
    # Create downloader
    downloader = FlickrDownloader(
        api_key=api_key,
        per_class=args.per_class
    )
    
    try:
        if args.dry_run:
            # Just search and count
            count = 0
            for record in downloader.scan(class_name=args.class_name):
                count += 1
                print(f"Found: {record.source_url} ({record.class_name}, {record.license})")
            print(f"\nTotal found: {count}")
        else:
            # Search and download
            for result in downloader.process(class_name=args.class_name):
                if result.success:
                    print(f"✓ Downloaded: {result.local_path}")
                else:
                    print(f"✗ Failed: {result.record.source_url} - {result.error_message}")
            
            downloader.print_summary()
    
    finally:
        downloader.close()
    
    return 0


if __name__ == "__main__":
    exit(main())