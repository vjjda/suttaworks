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
    
    # --- THAY ĐỔI 1: Khởi tạo dict lồng nhau dựa trên config ---
    # Thay vì một dict phẳng, ta tạo một dict với các key là tên thư mục.
    grouped_filepath_map = {folder: {} for folder in folders_to_scan}
    file_count = 0

    for folder in folders_to_scan:
        scan_dir = base_path / folder
        if not scan_dir.is_dir():
            log.warning(f"Thư mục không tồn tại, bỏ qua: {scan_dir}")
            continue

        log.debug(f"Đang quét trong {scan_dir}...")
        for json_file in scan_dir.glob('**/*.json'):
            file_key = json_file.stem
            relative_path = json_file.relative_to(project_root)
            
            # --- THAY ĐỔI 2: Thêm dữ liệu vào đúng nhóm ---
            # Thêm cặp key-value vào sub-dictionary tương ứng.
            grouped_filepath_map[folder][file_key] = str(relative_path)
            file_count += 1

    if file_count > 0:
        log.info(f"Tìm thấy tổng cộng {file_count} file JSON. Đang ghi ra file: {output_file}")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                # --- THAY ĐỔI 3: Ghi dict đã được nhóm vào file ---
                json.dump(grouped_filepath_map, f, ensure_ascii=False, indent=2)
            log.info(f"✅ Đã tạo file tổng hợp Bilara theo nhóm thành công.")
        except IOError as e:
            log.error(f"Không thể ghi file JSON: {e}")
    else:
        log.warning("Không tìm thấy file JSON nào để xử lý.")