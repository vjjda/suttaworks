# Path: src/db_updater/db_updater_config_parser.py
import logging
from pathlib import Path

import yaml

log = logging.getLogger(__name__)


def load_config(config_path: Path) -> dict | None:
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:

        log.error(f"Không tìm thấy file cấu hình tại: {config_path}")
        return None
