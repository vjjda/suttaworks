# Path: src/db_updater/handlers/api_handler.py
import logging
import requests
import json
from pathlib import Path
from typing import Dict, List


from src.db_updater.post_tasks import suttaplex_json_task

from src.config import constants

log = logging.getLogger(__name__)


def _fetch_and_save(url: str, filepath: Path):

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(response.json(), f, ensure_ascii=False, indent=2)
        log.info(f"Đã lưu thành công: {filepath.name}")
        return True
    except requests.exceptions.RequestException as e:
        log.error(f"Lỗi khi tải {url}: {e}")
        return False


def process_api_data(
    handler_config: Dict,
    destination_dir: Path,
    run_update: bool = True,
    run_post_process: bool = True,
    tasks_to_run: List[str] | None = None,
):
    all_successful = True

    if run_update:
        log.info("=== GIAI ĐOẠN: CẬP NHẬT DỮ LIỆU TỪ API ===")
        base_url = handler_config.get("base_url")
        groups = handler_config.get("groups", {})

        if not base_url or not groups:
            log.error("Thiếu 'base_url' hoặc 'groups' trong cấu hình api.")
            return

        log.info(f"Bắt đầu tải dữ liệu API từ base_url: {base_url}")

        for group_name, uids in groups.items():
            log.info(f"--> Đang xử lý nhóm: {group_name}")
            for uid in uids:
                url = f"{base_url}{uid}"
                filepath = destination_dir / group_name / f"{uid}.json"
                if not _fetch_and_save(url, filepath):
                    all_successful = False

        if all_successful:
            log.info("Tải dữ liệu API hoàn tất.")
        else:
            log.error("Có lỗi xảy ra trong quá trình tải API, sẽ không chạy hậu xử lý.")
            return
    else:
        log.info("Bỏ qua giai đoạn cập nhật dữ liệu API theo yêu cầu.")

    if run_post_process:
        log.info("=== GIAI ĐOẠN: HẬU XỬ LÝ (POST-PROCESSING) ===")
        if "post_tasks" in handler_config:
            for task_name, task_config in handler_config["post_tasks"].items():
                if tasks_to_run is None or task_name in tasks_to_run:
                    log.info(f"--> Bắt đầu tác vụ: '{task_name}'...")
                    if task_name == "suttaplex-json":

                        suttaplex_json_task.process_suttaplex_json(
                            task_config, constants.PROJECT_ROOT, destination_dir
                        )

                    else:
                        log.warning(
                            f"--> Tác vụ hậu xử lý không được hỗ trợ: {task_name}"
                        )
                else:
                    log.info(f"--> Bỏ qua tác vụ '{task_name}' theo yêu cầu.")
    else:
        log.info("Bỏ qua giai đoạn hậu xử lý theo yêu cầu.")