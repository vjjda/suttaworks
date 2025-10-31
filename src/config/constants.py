# Path: src/config/constants.py
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


DATA_PATH = PROJECT_ROOT / "data"
RAW_DATA_PATH = DATA_PATH / "raw"
PROCESSED_DATA_PATH = DATA_PATH / "processed"

CONFIG_PATH = PROJECT_ROOT / "src" / "config"