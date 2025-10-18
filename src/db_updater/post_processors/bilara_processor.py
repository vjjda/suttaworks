# Path: src/db_updater/post_processors/bilara_processor.py
import logging
import json
from pathlib import Path
from typing import Dict, Any

log = logging.getLogger(__name__)


def _write_json_output(output_path: Path, data: Dict[str, Any], data_name: str):
    """
    Hàm phụ trợ để ghi một dictionary ra file JSON,
    loại bỏ các category rỗng trước khi ghi.
    """
    # --- THAY ĐỔI: Lọc bỏ các category không có file nào ---
    # Chỉ giữ lại các cặp (key, value) mà value (dictionary con) không rỗng.
    cleaned_data = {folder: files for folder, files in data.items() if files}

    if cleaned_data:
        total_files = sum(len(files) for files in cleaned_data.values())
        log.info(f"Tìm thấy {total_files} file JSON cho nhóm '{data_name}'. Đang ghi ra file: {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                # Ghi dữ liệu đã được làm sạch
                json.dump(cleaned_data, f, ensure_ascii=False, indent=2)
            log.info(f"✅ Đã tạo file tổng hợp Bilara ({data_name}) thành công.")
        except IOError as e:
            log.error(f"Không thể ghi file JSON ({data_name}): {e}")
    else:
        log.warning(f"Không tìm thấy file JSON nào cho nhóm '{data_name}'.")


def process_bilara_data(config: Dict, project_root: Path):
    """
    Quét các thư mục con trong kho dữ liệu bilara,
    phân loại các file vào nhóm 'site' hoặc 'sutta' dựa trên đường dẫn,
    và lưu vào hai file JSON riêng biệt.
    """
    try:
        base_path = project_root / config['path']
        folders_to_scan = config['folders']
        output_config = config['output']
        site_keywords = set(config.get('site', [])) # Dùng set để tra cứu nhanh hơn
        
        sutta_output_file = project_root / output_config['sutta']
        site_output_file = project_root / output_config['site']

    except KeyError as e:
        log.error(f"Thiếu key bắt buộc trong cấu hình 'bilara': {e}")
        return

    log.info(f"Bắt đầu quét dữ liệu Bilara từ: {base_path}")
    
    # --- THAY ĐỔI: Tạo hai map riêng biệt ---
    sutta_map = {folder: {} for folder in folders_to_scan}
    site_map = {folder: {} for folder in folders_to_scan}
    
    relative_base = base_path.parent.parent

    for folder in folders_to_scan:
        scan_dir = base_path / folder
        if not scan_dir.is_dir():
            log.warning(f"Thư mục không tồn tại, bỏ qua: {scan_dir}")
            continue

        log.debug(f"Đang quét trong {scan_dir}...")
        for json_file in scan_dir.glob('**/*.json'):
            file_key = json_file.stem
            relative_path = json_file.relative_to(relative_base)
            
            # --- THAY ĐỔI: Logic phân luồng (routing) ---
            # Lấy các phần của đường dẫn (ví dụ: ['sc_bilara_data', 'site', ...])
            path_parts = set(relative_path.parts)
            
            # Kiểm tra xem có phần nào của đường dẫn nằm trong danh sách site_keywords không
            if not path_parts.isdisjoint(site_keywords):
                # Nếu có, đây là file thuộc nhóm 'site'
                site_map[folder][file_key] = str(relative_path)
            else:
                # Nếu không, đây là file thuộc nhóm 'sutta'
                sutta_map[folder][file_key] = str(relative_path)

    # --- THAY ĐỔI: Ghi cả hai file output ---
    _write_json_output(sutta_output_file, sutta_map, "Sutta")
    _write_json_output(site_output_file, site_map, "Site")