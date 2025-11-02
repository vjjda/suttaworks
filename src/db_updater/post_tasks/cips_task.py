# Path: src/db_updater/post_tasks/cips_task.py
import logging
from typing import Dict

from src.config import constants

from .cips import (
    process_tsv,
    sort_topic_index,
    sort_sutta_index,
    write_json_file,
)

__all__ = ["run"]

log = logging.getLogger(__name__)


def run(task_config: Dict):
    try:

        project_root = constants.PROJECT_ROOT
        tsv_path = project_root / task_config["path"]
        topic_output_file = project_root / task_config["output"]["topic-index"]
        sutta_output_file = project_root / task_config["output"]["sutta-index"]

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
