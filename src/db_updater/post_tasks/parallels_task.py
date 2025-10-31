# Path: src/db_updater/post_tasks/parallels_task.py
import json
import logging
from pathlib import Path

from src.config import constants
from . import parallels

__all__ = ["run"]

log = logging.getLogger(__name__)

def run(task_config: dict):
    """Orchestrates the parallels data processing task."""
    try:
        project_root = constants.PROJECT_ROOT
        input_path = project_root / task_config["path"]
        output_config = task_config.get("output", {})
        replacements = task_config.get("replacements", [])

        paths = {
            key: project_root / path
            for key, path in output_config.items()
            if key in ["category", "segment", "flat_segment", "book"]
        }

        if not any(paths.values()):
            log.error("Lỗi cấu hình 'parallels': không có đường dẫn output hợp lệ.")
            return

        log.info(f"Bắt đầu xử lý file parallels: {input_path}")
        if not input_path.exists():
            log.error(f"Không tìm thấy file input: {input_path}")
            return

        with open(input_path, "r", encoding="utf-8") as f:
            raw_content = f.read()

        if replacements:
            log.info(f"Thực hiện {len(replacements)} thay thế văn bản từ config...")
            for find, replace in replacements:
                raw_content = raw_content.replace(find, replace)

        data = json.loads(raw_content)

        # 1. Build initial map
        sutta_map = parallels.build_initial_map(data)

        # 2. Save category data if requested
        if "category" in paths:
            category_data = parallels.sort_data_naturally(sutta_map)
            _write_json(category_data, paths["category"], "Category")

        # 3. Transform and save other formats if requested
        if any(key in paths for key in ["segment", "flat_segment", "book"]):
            segment_map = parallels.invert_to_segment_structure(sutta_map)
            segment_data = parallels.sort_data_naturally(segment_map)

            if "segment" in paths:
                _write_json(segment_data, paths["segment"], "Segment")

            if "flat_segment" in paths:
                flat_map = parallels.flatten_segment_map(segment_data)
                flat_data = parallels.sort_data_naturally(flat_map)
                _write_json(flat_data, paths["flat_segment"], "Flat Segment")

            if "book" in paths:
                book_map = parallels.create_book_structure(segment_data)
                book_data = parallels.sort_data_naturally(book_map)
                _write_json(book_data, paths["book"], "Book")

    except Exception as e:
        log.exception(f"Đã xảy ra lỗi không mong muốn khi xử lý parallels: {e}")

def _write_json(data: dict, path: Path, file_type: str):
    """Helper to write data to a JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info(f"✅ Đã lưu file {file_type} vào: {path}")
