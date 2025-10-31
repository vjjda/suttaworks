# Path: src/db_updater/handlers/api_handler.py
import logging
import requests
import json
from pathlib import Path

from src.db_updater.handlers.base_handler import BaseHandler

log = logging.getLogger(__name__)


class ApiHandler(BaseHandler):
    """
    Handler để xử lý việc tải dữ liệu từ các điểm cuối (endpoints) API.
    """

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
        """
        Thực thi logic chính: lặp qua các nhóm và UID được định nghĩa trong
        cấu hình, sau đó tải dữ liệu từ API tương ứng.
        """
        log.info("Bắt đầu cập nhật dữ liệu từ API.")
        base_url = self.handler_config.get("base_url")
        groups = self.handler_config.get("groups", {})

        if not base_url or not groups:
            log.error("Thiếu 'base_url' hoặc 'groups' trong cấu hình api.")
            return

        log.info(f"Bắt đầu tải dữ liệu API từ base_url: {base_url}")
        all_successful = True

        for group_name, uids in groups.items():
            log.info(f"--> Đang xử lý nhóm: {group_name}")
            for uid in uids:
                url = f"{base_url}{uid}"
                filepath = self.destination_dir / group_name / f"{uid}.json"
                if not self._fetch_and_save(url, filepath):
                    all_successful = False

        if all_successful:
            log.info("Tải dữ liệu API hoàn tất.")
        else:
            # Ném ra một ngoại lệ để báo hiệu cho quy trình chính rằng có lỗi
            raise RuntimeError("Một hoặc nhiều file API không thể tải về.")