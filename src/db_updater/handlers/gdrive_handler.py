# Path: src/db_updater/handlers/gdrive_handler.py
import os
import re
import json
import logging
from pathlib import Path
from zipfile import ZipFile, is_zipfile
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gdown

from src.db_updater.handlers.base_handler import BaseHandler

log = logging.getLogger(__name__)


class GDriveHandler(BaseHandler):

    def __init__(self, handler_config: dict, destination_dir: Path):
        super().__init__(handler_config, destination_dir)
        load_dotenv()
        self.api_key = os.getenv("GOOGLE_API_KEY")

    def _get_folder_id_from_url(self, url: str) -> str | None:
        match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
        return match.group(1) if match else None

    def _get_local_version(self, version_file: Path) -> int:
        if not version_file.exists():
            return 0
        try:
            with open(version_file, "r") as f:
                data = json.load(f)
                return int(data.get("version", 0))
        except (json.JSONDecodeError, ValueError):
            log.error(
                f"Lỗi đọc file version.json tại {version_file}. Coi như version là 0."
            )
            return 0

    def _write_local_version(self, version_file: Path, version: int, filename: str):
        from datetime import datetime, timezone

        version_data = {
            "version": str(version),
            "download_date": datetime.now(timezone.utc).isoformat(),
            "source_filename": filename,
        }
        with open(version_file, "w") as f:
            json.dump(version_data, f, indent=2)
        log.info(f"Đã cập nhật file version.json với phiên bản {version}.")

    def execute(self):
        if not self.api_key:
            log.error(
                "Không tìm thấy GOOGLE_API_KEY trong file .env. Vui lòng kiểm tra lại."
            )
            return

        folder_url = self.handler_config.get("zip")
        folder_id = self._get_folder_id_from_url(folder_url)
        if not folder_id:
            log.error(f"URL thư mục Google Drive không hợp lệ: {folder_url}")
            return

        try:
            service = build("drive", "v3", developerKey=self.api_key)
            query = f"'{folder_id}' in parents and trashed = false"
            results = service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get("files", [])
            if not files:
                log.warning(
                    f"Không tìm thấy file nào trong thư mục Google Drive: {folder_url}"
                )
                return
        except HttpError as error:
            log.error(f"Lỗi khi gọi Google Drive API: {error}")
            return

        version_regex = self.handler_config.get("version-date")
        latest_version = 0
        latest_file = None
        for file in files:
            match = re.search(version_regex, file["name"])
            if match:
                version = int(match.group(1))
                if version > latest_version and file["name"].endswith(".zip"):
                    latest_version = version
                    latest_file = file

        if not latest_file:
            log.warning("Không tìm thấy file .zip nào có định dạng version hợp lệ.")
            return

        log.info(
            f"Phiên bản mới nhất online: {latest_version} (file: {latest_file['name']})"
        )

        version_file = self.destination_dir / "version.json"
        local_version = self._get_local_version(version_file)
        log.info(f"Phiên bản hiện tại local: {local_version}")

        if latest_version <= local_version:
            log.info("Dữ liệu đã là phiên bản mới nhất. Không cần cập nhật.")
            return

        self.destination_dir.mkdir(parents=True, exist_ok=True)
        zip_path = self.destination_dir / latest_file["name"]
        log.info(f"Đang tải file mới: {latest_file['name']}...")
        gdown.download(id=latest_file["id"], output=str(zip_path), quiet=False)

        extract_dir_name = self.handler_config.get("extract")
        extract_path = self.destination_dir / extract_dir_name
        log.info(f"Đang giải nén vào: {extract_path}...")
        if is_zipfile(zip_path):
            with ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_path)
        else:
            log.error("File tải về không phải là file zip hợp lệ.")

            zip_path.unlink()
            return

        self._write_local_version(version_file, latest_version, latest_file["name"])
        zip_path.unlink()
        log.info(f"Đã xóa file tạm: {zip_path.name}")