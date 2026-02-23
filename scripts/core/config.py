"""
Central configuration for the Hornet-Data Pipeline.

This module defines all configuration dataclasses and default settings
for data sources, pipeline parameters, and processing options.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class SourceConfig:
    """Configuration for a data source."""
    name: str
    enabled: bool
    rate_limit_per_second: float
    max_retries: int
    timeout_seconds: int
    license_whitelist: List[str]
    base_url: Optional[str] = None
    api_key: Optional[str] = None


@dataclass
class RetryConfig:
    """Configuration for retry with exponential backoff."""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True


@dataclass
class PipelineConfig:
    """Global pipeline configuration."""
    raw_dir: Path = field(default_factory=lambda: Path("raw"))
    processed_dir: Path = field(default_factory=lambda: Path("processed"))
    metadata_dir: Path = field(default_factory=lambda: Path("metadata"))
    db_path: Path = field(default_factory=lambda: Path("metadata/pipeline_state.db"))
    max_image_size: int = 1920
    train_val_test_split: tuple = (0.7, 0.2, 0.1)
    random_seed: int = 42
    classes: Dict[int, str] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.classes:
            self.classes = {
                0: "vespa_velutina",    # Asian Hornet
                1: "vespa_crabro",      # European Hornet
                2: "vespula_vulgaris",  # Common Wasp
                3: "apis_mellifera",    # Honey Bee
            }
        # Convert string paths to Path objects
        self.raw_dir = Path(self.raw_dir)
        self.processed_dir = Path(self.processed_dir)
        self.metadata_dir = Path(self.metadata_dir)
        self.db_path = Path(self.db_path)
    
    def ensure_directories(self):
        """Create all required directories."""
        for base_dir in [self.raw_dir, self.processed_dir, self.metadata_dir]:
            base_dir.mkdir(parents=True, exist_ok=True)
        
        # Create class subdirectories in raw
        for source in ["lubw", "flickr", "inaturalist", "waarnemingen"]:
            for class_name in self.classes.values():
                (self.raw_dir / source / class_name).mkdir(parents=True, exist_ok=True)
        
        # Create processed directories
        for split in ["train", "val", "test"]:
            (self.processed_dir / split / "images").mkdir(parents=True, exist_ok=True)
            (self.processed_dir / split / "labels").mkdir(parents=True, exist_ok=True)


# Class ID to species name mapping
CLASSES = {
    0: "vespa_velutina",    # Asian Hornet (Asiatische Hornisse)
    1: "vespa_crabro",      # European Hornet (Europäische Hornisse)
    2: "vespula_vulgaris",  # Common Wasp (Gemeine Wespe)
    3: "apis_mellifera",    # Honey Bee (Westliche Honigbiene)
}

# Reverse mapping: species name to class ID
CLASS_NAME_TO_ID = {v: k for k, v in CLASSES.items()}

# German names for display
CLASSES_DE = {
    0: "Asiatische Hornisse",
    1: "Europäische Hornisse",
    2: "Gemeine Wespe",
    3: "Westliche Honigbiene",
}


# Source-specific configurations
SOURCES: Dict[str, SourceConfig] = {
    "lubw": SourceConfig(
        name="LUBW/Convotis",
        enabled=True,
        rate_limit_per_second=1.0,
        max_retries=5,
        timeout_seconds=30,
        license_whitelist=["unknown"],  # Government data, assumed OK for research
        base_url="https://gmp.convotis.com/documents/d/global",
    ),
    "flickr": SourceConfig(
        name="Flickr",
        enabled=True,
        rate_limit_per_second=0.5,  # Flickr is strict
        max_retries=3,
        timeout_seconds=20,
        license_whitelist=["CC0", "CC-BY", "CC-BY-SA"],
        api_key=os.getenv("FLICKR_API_KEY"),
    ),
    "inaturalist": SourceConfig(
        name="iNaturalist",
        enabled=True,
        rate_limit_per_second=2.0,
        max_retries=5,
        timeout_seconds=30,
        license_whitelist=["CC0", "CC-BY", "CC-BY-SA", "CC-BY-NC"],
        base_url="https://api.gbif.org/v1",
    ),
    "waarnemingen": SourceConfig(
        name="Waarnemingen.nl",
        enabled=True,
        rate_limit_per_second=1.0,
        max_retries=3,
        timeout_seconds=20,
        license_whitelist=["CC0", "CC-BY"],
        base_url="https://observation.org/api/v1",
    ),
}


# License ID mappings for Flickr API
FLICKR_LICENSES = {
    # License ID -> License Code
    1: "CC-BY-NC-SA",
    2: "CC-BY-NC",
    3: "CC-BY-NC-ND",
    4: "CC-BY",
    5: "CC-BY-SA",
    6: "CC-BY-ND",
    7: "No known copyright restrictions",
    8: "United States government work",
    9: "CC0",
    10: "Public Domain Mark",
}

# Allowed Flickr license IDs for our use (CC0, CC-BY, CC-BY-SA)
FLICKR_ALLOWED_LICENSE_IDS = [4, 5, 9, 10]


# LUBW/Convotis URL pattern components
LUBW_URL_PATTERN = "https://gmp.convotis.com/documents/d/global/anhang_{year:04d}-{month:02d}-{number:04d}-{ext}"
LUBW_EXTENSIONS = ["jpg", "jpeg", "png", "gif", "JPG", "JPEG", "PNG", "GIF"]

# Date range for LUBW scanning
LUBW_START_DATE = (2023, 1)  # Portal launch estimate
LUBW_EXPECTED_CLASS = "vespa_velutina"  # LUBW only has Asian hornet data


def get_config() -> PipelineConfig:
    """Get the default pipeline configuration."""
    return PipelineConfig()


def get_source_config(source: str) -> Optional[SourceConfig]:
    """Get configuration for a specific source."""
    return SOURCES.get(source)