# Path: src/db_updater/config_parser.py
import yaml
from pathlib import Path
import logging

log = logging.getLogger(__name__)


def load_config(config_path: Path) -> dict | None:
    log.info(f"Đang đọc cấu hình từ: {config_path}")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        log.error(f"Không tìm thấy file cấu hình tại: {config_path}")
        return None