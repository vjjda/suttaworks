# Path: src/db_updater/post_processors/bilara_processor.py
import logging
import json
from pathlib import Path
from typing import Dict

log = logging.getLogger(__name__)

def process_bilara_data(config: Dict, project_root: Path):
    """
    Quét các thư mục con trong kho dữ liệu bilara,
    tạo một map đã được nhóm theo thư mục (comment, html, v.v.),
    và lưu vào một file JSON duy nhất.
    """
    try:
        base_path = project_root / config['path']
        folders_to_scan = config['folders']
        output_file = project_root / config['output']
    except KeyError as e:
        log.error(f"Thiếu key bắt buộc trong cấu hình 'bilara': {e}")
        return

    log.info(f"Bắt đầu quét dữ liệu Bilara từ: {base_path}")
    
    grouped_filepath_map = {folder: {} for folder in folders_to_scan}
    file_count = 0
    
    # --- THAY ĐỔI 1: Xác định thư mục gốc mới cho đường dẫn tương đối ---
    # base_path là '.../suttacentral-data/sc_bilara_data'
    # .parent sẽ là '.../suttacentral-data', đúng như bạn muốn.
    relative_base = base_path.parent.parent

    for folder in folders_to_scan:
        scan_dir = base_path / folder
        if not scan_dir.is_dir():
            log.warning(f"Thư mục không tồn tại, bỏ qua: {scan_dir}")
            continue

        log.debug(f"Đang quét trong {scan_dir}...")
        for json_file in scan_dir.glob('**/*.json'):
            file_key = json_file.stem
            
            # --- THAY ĐỔI 2: Tạo đường dẫn tương đối từ gốc mới ---
            relative_path = json_file.relative_to(relative_base)
            
            grouped_filepath_map[folder][file_key] = str(relative_path)
            file_count += 1

    if file_count > 0:
        log.info(f"Tìm thấy tổng cộng {file_count} file JSON. Đang ghi ra file: {output_file}")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(grouped_filepath_map, f, ensure_ascii=False, indent=2)
            log.info(f"✅ Đã tạo file tổng hợp Bilara theo nhóm thành công.")
        except IOError as e:
            log.error(f"Không thể ghi file JSON: {e}")
    else:
        log.warning("Không tìm thấy file JSON nào để xử lý.")