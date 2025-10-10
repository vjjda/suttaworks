# Path: src/config/constants.py
from pathlib import Path

# Thư mục gốc của toàn bộ dự án (suttacentral-vj)
# Path(__file__) là file constants.py hiện tại
# .parents[2] sẽ đi ngược lên 2 cấp: /config -> /src -> / (PROJECT_ROOT)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Định nghĩa các đường dẫn quan trọng khác dựa trên PROJECT_ROOT
DATA_PATH = PROJECT_ROOT / "data"
RAW_DATA_PATH = DATA_PATH / "raw"
PROCESSED_DATA_PATH = DATA_PATH / "processed"

CONFIG_PATH = PROJECT_ROOT / "src" / "config"