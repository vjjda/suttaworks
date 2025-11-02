# Path: src/db_updater/post_tasks/cips/cips_utils.py
import json
import logging
from pathlib import Path
from typing import Dict

__all__ = ["write_json_file"]

log = logging.getLogger(__name__)


def write_json_file(data: Dict, output_file: Path, file_type: str):
    if not data:
        log.warning(f"Không có dữ liệu để ghi cho file {file_type}.")
        return

    log.info(f"Đang ghi {len(data)} mục vào file {file_type}: {output_file}")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log.info(f"✅ Đã tạo file {file_type} thành công.")
    except IOError as e:
        log.error(f"Không thể ghi file {file_type}: {e}")
