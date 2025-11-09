# Path: src/db_updater/post_tasks/cips_task.py
import logging
from typing import Any, Dict

from src.config import constants

from .cips import (
    process_tsv,
    sort_sutta_index,
    sort_topic_index,
    write_json_file,
)

__all__ = ["run"]

log = logging.getLogger(__name__)


def run(task_config: Dict[str, Any]):
    try:
        project_root = constants.PROJECT_ROOT

        path_str = task_config.get("path")
        if not isinstance(path_str, str):
            log.error(
                "Lỗi cấu hình 'cips-json': 'path' bị thiếu hoặc không phải string."
            )
            return
        tsv_path = project_root / path_str

        output_config = task_config.get("output")
        if not isinstance(output_config, dict):
            log.error(
                "Lỗi cấu hình 'cips-json': 'output' bị thiếu hoặc không phải dict."
            )
            return

        topic_path_str = output_config.get("topic-index")
        sutta_path_str = output_config.get("sutta-index")

        if not isinstance(topic_path_str, str) or not isinstance(sutta_path_str, str):
            log.error(
                "Lỗi cấu hình 'cips-json': 'output' thiếu 'topic-index' hoặc 'sutta-index'."
            )
            return

        topic_output_file = project_root / topic_path_str
        sutta_output_file = project_root / sutta_path_str

        if not tsv_path.is_file():
            log.error(f"File TSV nguồn không tồn tại: {tsv_path}")
            return

    except KeyError as e:
        log.error(f"Lỗi cấu hình 'cips-json': thiếu key {e}")
        return

    log.info(f"Bắt đầu xử lý file TSV để tạo 2 chỉ mục từ: {tsv_path}")

    topic_index, sutta_index = process_tsv(tsv_path)

    if not topic_index and not sutta_index:
        log.warning("Không có dữ liệu nào được xử lý từ file TSV.")
        return

    log.info("Đang sắp xếp dữ liệu chỉ mục...")

    sorted_topic_index = sort_topic_index(topic_index)
    sorted_sutta_index = sort_sutta_index(sutta_index)

    write_json_file(sorted_topic_index, topic_output_file, "topic-index")
    write_json_file(sorted_sutta_index, sutta_output_file, "sutta-index")

    log.info("Hoàn tất tác vụ CIPS.")
