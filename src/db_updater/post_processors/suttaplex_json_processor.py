# Path: src/db_updater/post_processors/suttaplex_json_processor.py
import logging
import json
from pathlib import Path
from typing import Dict, List, Set

log = logging.getLogger(__name__)

def _is_value_empty(value_dict: dict) -> bool:
    """
    Kiểm tra xem tất cả các giá trị trong một dictionary có rỗng không.
    Hàm này kiểm tra đệ quy cho các dictionary lồng nhau.
    "Rỗng" được định nghĩa là None, [], {}, "", 0.
    """
    for v in value_dict.values():
        if isinstance(v, dict):
            if not _is_value_empty(v):  # Đệ quy
                return False
        elif v not in [None, [], {}, "", 0]:
            return False
    return True

def _process_group(group_name: str, base_dir: Path, target_dict: Dict, existing_keys: Set[str] = None):
    """
    Hàm phụ trợ để xử lý tất cả các file JSON trong một thư mục nhóm.
    """
    group_dir = base_dir / group_name
    if not group_dir.is_dir():
        log.warning(f"Thư mục nhóm '{group_name}' không tồn tại, bỏ qua.")
        return

    log.debug(f"Đang quét nhóm: {group_name}")
    for file_path in group_dir.glob('*.json'):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    log.warning(f"File {file_path.name} không chứa một danh sách, bỏ qua.")
                    continue
                
                for item in data:
                    if not isinstance(item, dict) or 'uid' not in item:
                        log.warning(f"Mục trong {file_path.name} thiếu 'uid', bỏ qua.")
                        continue
                    
                    uid = item['uid']
                    
                    if existing_keys and uid in existing_keys:
                        continue
                    
                    value = item.copy()
                    del value['uid']
                    
                    # Thêm logic kiểm tra giá trị rỗng
                    if _is_value_empty(value):
                        log.debug(f"Mục với uid '{uid}' có tất cả giá trị rỗng, bỏ qua.")
                        continue

                    target_dict[uid] = value

        except (json.JSONDecodeError, IOError) as e:
            log.warning(f"Lỗi khi đọc file {file_path.name}, bỏ qua. Lỗi: {e}")


def process_suttaplex_json(config: Dict, project_root: Path, input_dir: Path):
    """
    Xử lý suttaplex JSON theo quy tắc priority và super-tree.
    """
    try:
        output_file = project_root / config['output']
        priority_groups = config.get('priority', [])
        super_tree_groups = config.get('super-tree', [])
    except KeyError as e:
        log.error(f"Thiếu key bắt buộc trong cấu hình 'suttaplex-json': {e}")
        return

    log.info("Bắt đầu xử lý suttaplex JSON với quy tắc priority và super-tree...")

    # Giai đoạn 1: Xử lý các nhóm priority
    priority_data = {}
    log.info(f"Giai đoạn 1: Đang xử lý các nhóm ưu tiên: {priority_groups}")
    for group in priority_groups:
        _process_group(group, input_dir, priority_data)
    log.info(f"-> Tìm thấy {len(priority_data)} mục ưu tiên.")

    # Giai đoạn 2: Xử lý các nhóm super-tree
    super_tree_only_data = {}
    priority_keys = set(priority_data.keys())
    log.info(f"Giai đoạn 2: Đang xử lý các nhóm super-tree: {super_tree_groups}")
    for group in super_tree_groups:
        _process_group(group, input_dir, super_tree_only_data, existing_keys=priority_keys)
    log.info(f"-> Tìm thấy {len(super_tree_only_data)} mục mới không trùng lặp từ super-tree.")

    # Giai đoạn 3: Gộp và ghi file
    final_data = {**super_tree_only_data, **priority_data}
    
    if final_data:
        log.info(f"Tổng hợp được {len(final_data)} mục. Ghi ra file: {output_file}")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, ensure_ascii=False, indent=2)
            log.info("✅ Hoàn tất xử lý và tạo file suttaplex.json.")
        except IOError as e:
            log.error(f"Không thể ghi file output: {e}")
    else:
        log.warning("Không có dữ liệu suttaplex nào được xử lý.")