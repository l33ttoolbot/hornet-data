# hornet-data 🐝🦟

Data acquisition and dataset management for the **Hornet3000** project - YOLO-based Asian Hornet detection and tracking.

## Overview

This repository manages all data sources for training a multi-class insect detection model with pose estimation capabilities for flight tracking.

## Classes

| ID | Species | German | English |
|----|---------|--------|---------|
| 0 | *Vespa velutina* | Asiatische Hornisse | Asian Hornet |
| 1 | *Vespa crabro* | Europäische Hornisse | European Hornet |
| 2 | *Vespula vulgaris* | Gemeine Wespe | Common Wasp |
| 3 | *Apis mellifera* | Westliche Honigbiene | Western Honey Bee |

## Data Sources

### 1. LUBW (Landesamt für Umwelt Baden-Württemberg)
- Asian hornet reporting portal images
- URL pattern: `https://gmp.convotis.com/documents/d/global/anhang_YYYY-MM-NNNN.ext`
- Sequential scanning with automatic detection

### 2. Flickr API
- Creative Commons licensed images (CC0, CC-BY, CC-BY-SA only)
- ~200-500 images per class
- Metadata preserved (photographer, license)

### 3. iNaturalist / GBIF (planned)
- CC-licensed observations via GBIF API

### 4. Waarnemingen.nl (planned)
- European observation platform

## Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/l33ttoolbot/hornet-data.git
cd hornet-data

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or: venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Setup environment
cp .env.example .env
# Edit .env and add your API keys
```

### Initialize Pipeline

```bash
# Initialize directories and database
python -m scripts.cli init

# View available sources
python -m scripts.cli sources

# View available classes
python -m scripts.cli classes
```

### Download Images

```bash
# Download from all sources
python -m scripts.cli download

# Download from LUBW only
python -m scripts.cli download --source lubw

# Download from Flickr (requires API key)
export FLICKR_API_KEY=your_key_here
python -m scripts.cli download --source flickr --per-class 200

# Dry run (scan only, no download)
python -m scripts.cli download --source lubw --dry-run
```

### View Statistics

```bash
python -m scripts.cli stats
```

## Directory Structure

```
hornet-data/
├── raw/                    # Original, unprocessed data
│   ├── lubw/              # LUBW/Convotis images
│   │   └── vespa_velutina/
│   │       └── YYYY-MM/   # Sorted by month
│   ├── flickr/            # Flickr images
│   │   └── {class_name}/
│   │       └── {photo_id}.jpg
│   ├── inaturalist/       # iNaturalist images (planned)
│   └── waarnemingen/      # Waarnemingen images (planned)
├── processed/              # Ready for training
│   ├── train/
│   │   ├── images/
│   │   └── labels/
│   ├── val/
│   └── test/
├── scripts/               # Download & preprocessing
│   ├── core/              # Core utilities
│   │   ├── config.py      # Configuration
│   │   ├── database.py    # SQLite progress tracking
│   │   ├── downloader.py  # Base downloader class
│   │   └── utils.py       # Utility functions
│   ├── downloaders/       # Source-specific downloaders
│   │   ├── lubw_scanner.py
│   │   └── flickr_downloader.py
│   ├── process/           # Processing scripts (Phase 4)
│   ├── cli.py             # Command-line interface
│   └── run_pipeline.py    # Main orchestration
├── metadata/              # Source attribution
│   └── pipeline_state.db  # SQLite database
├── logs/                  # Pipeline logs
├── requirements.txt
├── .env.example
└── README.md
```

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                                │
├─────────────────┬─────────────────┬─────────────────────────────┤
│  LUBW/Convotis  │  Flickr API     │  iNaturalist / Waarnemingen │
│  (Sequential)   │  (CC Licensed)  │  (planned)                  │
└────────┬────────┴────────┬────────┴─────────────────────────────┘
         │                 │
         ▼                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                   DOWNLOAD LAYER                                 │
│  - Rate Limiting (1 req/sec per source)                         │
│  - License Validation (CC0, CC-BY only)                         │
│  - SHA256 Deduplication                                          │
│  - Retry with Exponential Backoff                                │
│  - SQLite Progress Tracking                                      │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                      RAW DATA STORAGE                            │
│  raw/{source}/{class_name}/...                                   │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  PROCESSING LAYER (Phase 4)                      │
│  - Image Validation & Format Check                               │
│  - Size Normalization (max 1920px)                               │
│  - YOLO Label Generation                                         │
│  - Train/Val/Test Split (70/20/10)                               │
└─────────────────────────────────────────────────────────────────┘
```

## API Keys

### Flickr API

1. Go to https://www.flickr.com/services/apps/create/apply/
2. Request a non-commercial key
3. Add to `.env`:
```
FLICKR_API_KEY=your_key_here
FLICKR_API_SECRET=your_secret_here
```

## Features

- ✅ Sequential URL scanning (LUBW)
- ✅ Flickr API integration with CC license filtering
- ✅ SQLite progress tracking (resumable downloads)
- ✅ SHA256 deduplication
- ✅ Rate limiting per source
- ✅ Automatic retry with exponential backoff
- ✅ CLI with rich progress output
- 🔄 iNaturalist/GBIF integration (planned)
- 🔄 Waarnemingen.nl integration (planned)
- 🔄 Image processing pipeline (planned)
- 🔄 YOLO dataset generation (planned)

## YOLO Label Formats

### Bounding Box (Detection)
```
class_id center_x center_y width height
0 0.5 0.5 0.3 0.4
```

### Pose Estimation (Keypoints)
```
class_id cx cy w h kp1_x kp1_y kp1_v kp2_x kp2_y kp2_v ...
```

Keypoints: head, thorax, abdomen, left_wing, right_wing

## Related Repositories

- **hornet3000**: Main detection/tracking project - https://github.com/l33ttoolbot/hornet3000

## License

Individual image licenses preserved in `metadata/sources.json`. Dataset compilation under CC-BY-4.0 where applicable.

## Architecture Documentation

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed technical documentation.