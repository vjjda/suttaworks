# Path: src/db_updater/post_processors/bilara_processor.py
import logging
import json
from pathlib import Path
from typing import Dict, Any, List
from natsort import natsorted  # <-- THÊM DÒNG NÀY

log = logging.getLogger(__name__)


def _write_json_output(output_path: Path, data: Dict[str, Any], data_name: str):
    """
    Hàm phụ trợ để ghi một dictionary ra file JSON,
    loại bỏ các category rỗng và SẮP XẾP TỰ NHIÊN các key trước khi ghi.
    """
    cleaned_data = {folder: files for folder, files in data.items() if files}

    if cleaned_data:
        # --- BẮT ĐẦU THAY ĐỔI ---
        # Sắp xếp tự nhiên các key bên trong mỗi folder
        sorted_data = {}
        for folder, files in cleaned_data.items():
            # Sử dụng natsorted để lấy danh sách key đã được sắp xếp đúng
            sorted_keys = natsorted(files.keys())
            # Dựng lại dictionary với thứ tự key mới
            sorted_files = {key: files[key] for key in sorted_keys}
            sorted_data[folder] = sorted_files
        # --- KẾT THÚC THAY ĐỔI ---

        total_files = sum(len(files) for files in sorted_data.values())
        log.info(f"Tìm thấy {total_files} file JSON cho nhóm '{data_name}'. Đang ghi ra file: {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                # Ghi dữ liệu đã được sắp xếp
                json.dump(sorted_data, f, ensure_ascii=False, indent=2)
            log.info(f"✅ Đã tạo file tổng hợp Bilara ({data_name}) thành công.")
        except IOError as e:
            log.error(f"Không thể ghi file JSON ({data_name}): {e}")
    else:
        log.warning(f"Không tìm thấy file JSON nào cho nhóm '{data_name}'.")

def process_bilara_data(config: Dict, project_root: Path):
    """
    Quét dữ liệu bilara, phân loại file vào các nhóm dựa trên một danh sách
    ưu tiên được định nghĩa trong config, và lưu vào các file JSON riêng biệt.
    """
    try:
        base_path = project_root / config['path']
        folders_to_scan = config['folders']
        output_config = config['output']
        groups_config = config.get('groups', [])

    except KeyError as e:
        log.error(f"Thiếu key bắt buộc trong cấu hình 'bilara': {e}")
        return

    log.info(f"Bắt đầu quét dữ liệu Bilara từ: {base_path}")
    
    # --- THAY ĐỔI: Khởi tạo các map output một cách động ---
    output_maps = {}
    # Thêm map mặc định 'sutta'
    output_maps['sutta'] = {folder: {} for folder in folders_to_scan}
    # Thêm các map được định nghĩa trong groups
    for group in groups_config:
        group_name = list(group.keys())[0]
        output_maps[group_name] = {folder: {} for folder in folders_to_scan}
    
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
            path_parts = set(relative_path.parts)
            
            # --- THAY ĐỔI: Logic phân luồng ưu tiên mới ---
            matched = False
            # Duyệt qua các group theo đúng thứ tự ưu tiên trong config
            for group in groups_config:
                group_name = list(group.keys())[0]
                keywords = set(list(group.values())[0])
                
                # Nếu tìm thấy sự trùng khớp
                if not path_parts.isdisjoint(keywords):
                    # Phân file vào map tương ứng
                    output_maps[group_name][folder][file_key] = str(relative_path)
                    matched = True
                    # Dừng việc kiểm tra ngay lập tức vì đã tìm thấy group ưu tiên cao nhất
                    break
            
            # Nếu không khớp với bất kỳ group nào, đưa vào nhóm 'sutta' mặc định
            if not matched:
                output_maps['sutta'][folder][file_key] = str(relative_path)

    # --- THAY ĐỔI: Ghi tất cả các file output đã được cấu hình ---
    for group_name, data_map in output_maps.items():
        # Chỉ ghi file nếu có cấu hình output cho group đó
        if group_name in output_config:
            output_file = project_root / output_config[group_name]
            _write_json_output(output_file, data_map, group_name.capitalize())