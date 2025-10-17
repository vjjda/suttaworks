# Path: src/db_updater/handlers/api_handler.py
import logging
import requests
import json
from pathlib import Path
from typing import Dict

from src.db_updater.post_processors import suttaplex_json_processor
from src.config import constants

log = logging.getLogger(__name__)

def _fetch_and_save(url: str, filepath: Path):
    """
    Tải và lưu một file JSON, đảm bảo thư mục cha tồn tại.
    """
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        # Lệnh này sẽ tạo cả thư mục cha nếu nó chưa tồn tại
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(response.json(), f, ensure_ascii=False, indent=2)
        log.info(f"Đã lưu thành công: {filepath.name}")
        return True
    except requests.exceptions.RequestException as e:
        log.error(f"Lỗi khi tải {url}: {e}")
        return False

def process_api_data(handler_config: Dict, destination_dir: Path):
    """
    Tải dữ liệu từ API, lưu vào các thư mục con theo nhóm,
    và sau đó chạy các tác vụ hậu xử lý nếu có.
    """
    base_url = handler_config.get('base_url')
    groups = handler_config.get('groups', {})

    if not base_url or not groups:
        log.error("Thiếu 'base_url' hoặc 'groups' trong cấu hình api.")
        return

    log.info(f"Bắt đầu tải dữ liệu API từ base_url: {base_url}")
    all_successful = True

    for group_name, uids in groups.items():
        log.info(f"--> Đang xử lý nhóm: {group_name}")
        for uid in uids:
            url = f"{base_url}{uid}"
            
            # --- THAY ĐỔI: Khôi phục lại logic tạo thư mục con cho mỗi nhóm ---
            # Ví dụ: data/raw/suttaplex/sutta/an.json
            filepath = destination_dir / group_name / f"{uid}.json"
            
            if not _fetch_and_save(url, filepath):
                all_successful = False
    
    if not all_successful:
        log.error("Có lỗi xảy ra trong quá trình tải API, sẽ không chạy hậu xử lý.")
        return
        
    log.info("Tải dữ liệu API hoàn tất.")

    if 'post' in handler_config:
        log.info("Bắt đầu các tác vụ hậu xử lý...")
        for task_name, task_config in handler_config['post'].items():
            if task_name == 'suttaplex-json':
                log.info("--> Chạy processor: suttaplex-json")
                # Truyền vào destination_dir (thư mục gốc suttaplex),
                # processor sẽ tự quét các thư mục con bên trong.
                suttaplex_json_processor.process_suttaplex_json(task_config, constants.PROJECT_ROOT, destination_dir)
            else:
                log.warning(f"--> Tác vụ hậu xử lý không được hỗ trợ: {task_name}")