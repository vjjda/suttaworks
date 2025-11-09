# Path: src/db_updater/handlers/api_handler.py
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

from src.db_updater.handlers.base_handler import BaseHandler

log = logging.getLogger(__name__)


MAX_DOWNLOAD_WORKERS = 20


class ApiHandler(BaseHandler):

    def _fetch_and_save(self, url: str, filepath: Path):
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

    def execute(self):
        log.info("Bắt đầu cập nhật dữ liệu từ API (chế độ song song).")
        base_url = self.handler_config.get("base_url")
        groups = self.handler_config.get("groups", {})

        if not base_url or not groups:
            log.error("Thiếu 'base_url' hoặc 'groups' trong cấu hình api.")
            return

        tasks = []
        for group_name, uids in groups.items():
            log.debug(f"--> Chuẩn bị nhóm: {group_name}")
            for uid in uids:
                url = f"{base_url}{uid}"
                filepath = self.destination_dir / group_name / f"{uid}.json"
                tasks.append((url, filepath))

        if not tasks:
            log.info("Không có file API nào được cấu hình để tải.")
            return

        log.info(
            f"Phát hiện tổng cộng {len(tasks)} file API. Bắt đầu tải với {MAX_DOWNLOAD_WORKERS} luồng..."
        )

        all_successful = True
        with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:

            futures = {
                executor.submit(self._fetch_and_save, url, fp): fp for url, fp in tasks
            }

            for future in as_completed(futures):
                filepath = futures[future]
                try:
                    success = future.result()
                    if not success:
                        all_successful = False
                        log.warning(
                            f"Tác vụ tải {filepath.name} báo cáo không thành công."
                        )
                except Exception:
                    all_successful = False
                    log.error(
                        f"Lỗi nghiêm trọng khi thực thi tác vụ tải {filepath.name}.",
                        exc_info=True,
                    )

        if all_successful:
            log.info("Tải dữ liệu API hoàn tất (song song).")
        else:
            raise RuntimeError("Một hoặc nhiều file API không thể tải về.")
