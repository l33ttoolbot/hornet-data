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
- Date-based folder structure
- Sequential numbering

### 2. Flickr API
- Creative Commons licensed images
- ~200-500 images per class
- Metadata preserved (photographer, license)

### 3. Kaggle - hornet3000 Dataset
- Base dataset with 3 classes (missing honey bee)
- https://www.kaggle.com/datasets/marcoryvandijk/vespa-velutina-v-crabro-vespulina-vulgaris

### 4. Custom Field Recordings
- Video footage from Raspberry Pi camera setups
- Flight sequences for tracking training

## Directory Structure

```
hornet-data/
├── raw/                    # Original, unprocessed data
│   ├── lubw/
│   ├── flickr/
│   ├── kaggle/
│   └── custom/
├── processed/              # Ready for training
│   ├── train/
│   ├── val/
│   └── test/
├── annotations/            # Label files
│   ├── bbox/              # YOLO format bounding boxes
│   └── pose/              # YOLO-Pose keypoints
├── scripts/               # Download & preprocessing
├── metadata/              # Source attribution
└── README.md
```

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