# Path: /src/db_updater/handlers/gdrive_handler.py
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

log = logging.getLogger(__name__)

def _get_folder_id_from_url(url: str) -> str | None:
    """Trích xuất ID của thư mục từ URL Google Drive."""
    match = re.search(r'/folders/([a-zA-Z0-9_-]+)', url)
    return match.group(1) if match else None

def _get_local_version(version_file: Path) -> int:
    """Đọc phiên bản hiện tại từ file version.json."""
    if not version_file.exists():
        return 0
    try:
        with open(version_file, 'r') as f:
            data = json.load(f)
            return int(data.get("version", 0))
    except (json.JSONDecodeError, ValueError):
        log.error(f"Lỗi đọc file version.json tại {version_file}. Coi như version là 0.")
        return 0

def _write_local_version(version_file: Path, version: int, filename: str):
    """Ghi phiên bản mới vào file version.json."""
    from datetime import datetime, timezone
    version_data = {
        "version": str(version),
        "download_date": datetime.now(timezone.utc).isoformat(),
        "source_filename": filename
    }
    with open(version_file, 'w') as f:
        json.dump(version_data, f, indent=2)
    log.info(f"Đã cập nhật file version.json với phiên bản {version}.")

def process_gdrive_data(
    handler_config: dict, 
    destination_dir: Path,
    run_update: bool = True,
    run_post_process: bool = True,
    tasks_to_run: list[str] | None = None
):
    """
    Quy trình hoàn chỉnh để tải và cập nhật dữ liệu từ Google Drive,
    hỗ trợ chạy từng phần và hậu xử lý.
    """
    # --- Giai đoạn 1: Cập nhật ---
    if run_update:
        log.info("=== GIAI ĐOẠN: CẬP NHẬT DỮ LIỆU TỪ GOOGLE DRIVE ===")
        load_dotenv()
        API_KEY = os.getenv("GOOGLE_API_KEY")
        if not API_KEY:
            log.error("Không tìm thấy GOOGLE_API_KEY trong file .env. Vui lòng kiểm tra lại.")
            return

        folder_url = handler_config.get('zip')
        folder_id = _get_folder_id_from_url(folder_url)
        if not folder_id:
            log.error(f"URL thư mục Google Drive không hợp lệ: {folder_url}")
            return

        # ... (toàn bộ logic tải và giải nén giữ nguyên ở đây) ...
        # 1. Dùng API để lấy danh sách file online
        try:
            service = build('drive', 'v3', developerKey=API_KEY)
            query = f"'{folder_id}' in parents and trashed = false"
            results = service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get('files', [])
            if not files:
                log.warning(f"Không tìm thấy file nào trong thư mục Google Drive: {folder_url}")
                return
        except HttpError as error:
            log.error(f"Lỗi khi gọi Google Drive API: {error}")
            return

        # 2. Tìm file có version mới nhất
        version_regex = handler_config.get('version-date')
        latest_version = 0
        latest_file = None
        for file in files:
            match = re.search(version_regex, file['name'])
            if match:
                version = int(match.group(1))
                if version > latest_version and file['name'].endswith('.zip'):
                    latest_version = version
                    latest_file = file
        
        if not latest_file:
            log.warning("Không tìm thấy file .zip nào có định dạng version hợp lệ.")
            return

        log.info(f"Phiên bản mới nhất online: {latest_version} (file: {latest_file['name']})")

        # 3. So sánh với version local
        version_file = destination_dir / "version.json"
        local_version = _get_local_version(version_file)
        log.info(f"Phiên bản hiện tại local: {local_version}")

        if latest_version <= local_version:
            log.info("Dữ liệu đã là phiên bản mới nhất. Không cần cập nhật.")
        else:
            # 4. Tải file mới
            destination_dir.mkdir(parents=True, exist_ok=True)
            zip_path = destination_dir / latest_file['name']
            log.info(f"Đang tải file mới: {latest_file['name']}...")
            gdown.download(id=latest_file['id'], output=str(zip_path), quiet=False)

            # 5. Giải nén
            extract_dir_name = handler_config.get('extract')
            extract_path = destination_dir / extract_dir_name
            log.info(f"Đang giải nén vào: {extract_path}...")
            if is_zipfile(zip_path):
                with ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_path)
            else:
                log.error("File tải về không phải là file zip hợp lệ.")
                return

            # 6. Cập nhật version và dọn dẹp
            _write_local_version(version_file, latest_version, latest_file['name'])
            zip_path.unlink() # Xóa file zip
            log.info(f"Đã xóa file tạm: {zip_path.name}")
    else:
        log.info("Bỏ qua giai đoạn cập nhật dữ liệu Google Drive theo yêu cầu.")

    # --- Giai đoạn 2: Hậu xử lý ---
    if run_post_process:
        log.info("=== GIAI ĐOẠN: HẬU XỬ LÝ (POST-PROCESSING) ===")
        if 'post_tasks' in handler_config:
            for task_name in handler_config['post_tasks']:
                if tasks_to_run is None or task_name in tasks_to_run:
                    log.warning(f"--> Tác vụ '{task_name}' là placeholder, bỏ qua.")
                else:
                    log.info(f"--> Bỏ qua tác vụ '{task_name}' theo yêu cầu.")
    else:
        log.info("Bỏ qua giai đoạn hậu xử lý theo yêu cầu.")