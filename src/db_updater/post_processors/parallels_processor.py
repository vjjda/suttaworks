# Path: src/db_updater/post_processors/parallels_processor.py
import json
import logging
import re
from pathlib import Path
from itertools import combinations
from collections import defaultdict

log = logging.getLogger(__name__)


def _natural_sort_key(s: str) -> list:
    """
    Tạo một sort key để sắp xếp chuỗi theo thứ tự tự nhiên.
    Ví dụ: 'mn2' sẽ đứng trước 'mn10'.
    'mn7#2.9' sẽ đứng trước 'mn7#10.3'.
    """
    return [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)]


def _sort_and_clean_map(sutta_map: defaultdict) -> dict:
    """
    Chuyển đổi, sắp xếp và dọn dẹp defaultdict cuối cùng.
    - Sắp xếp các key ở mọi cấp độ theo thứ tự tự nhiên.
    - Sắp xếp các loại quan hệ theo một thứ tự định sẵn.
    - Loại bỏ các mục trùng lặp trong danh sách parallels.
    """
    # Thứ tự mong muốn cho các loại quan hệ
    relation_order = ["parallels", "resembles", "mentions", "retells"]
    final_sorted_map = {}

    # Sắp xếp các base_id (cấp 1)
    for base_id in sorted(sutta_map.keys(), key=_natural_sort_key):
        relations = sutta_map[base_id]
        sorted_relations = {}

        # Sắp xếp các loại quan hệ (cấp 2) theo `relation_order`
        # Các loại không có trong list sẽ được xếp ở cuối
        def get_relation_sort_key(relation_type):
            try:
                return relation_order.index(relation_type)
            except ValueError:
                return len(relation_order)

        for rel_type in sorted(relations.keys(), key=get_relation_sort_key):
            full_id_map = relations[rel_type]
            sorted_full_id_map = {}

            # Sắp xếp các full_id (cấp 3)
            for full_id in sorted(full_id_map.keys(), key=_natural_sort_key):
                parallel_list = full_id_map[full_id]

                # Sắp xếp và loại bỏ trùng lặp trong danh sách parallel (cấp 4)
                unique_sorted_list = sorted(list(dict.fromkeys(parallel_list)), key=_natural_sort_key)
                
                if unique_sorted_list: # Chỉ thêm nếu danh sách không rỗng
                    sorted_full_id_map[full_id] = unique_sorted_list
            
            if sorted_full_id_map: # Chỉ thêm nếu dict không rỗng
                sorted_relations[rel_type] = sorted_full_id_map

        if sorted_relations: # Chỉ thêm nếu dict không rỗng
            final_sorted_map[base_id] = sorted_relations

    return final_sorted_map


def _parse_sutta_id(full_id: str) -> str:
    """Tách mã kinh gốc ra khỏi mã định danh đầy đủ."""
    cleaned_id = full_id.lstrip('~')
    return cleaned_id.split('#')[0]


def process_parallels_data(task_config: dict, project_root: Path):
    """
    Đọc file parallels.json gốc, xử lý và chuyển đổi nó thành một từ điển tra cứu
    với các mối quan hệ một chiều và hai chiều được áp dụng đúng và được sắp xếp.
    """
    try:
        input_path = project_root / task_config['path']
        output_path = project_root / task_config['output']
        
        log.info(f"Bắt đầu xử lý file parallels: {input_path}")

        if not input_path.exists():
            log.error(f"Không tìm thấy file input: {input_path}")
            return

        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        sutta_map = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        for group in data:
            relation_type = list(group.keys())[0]
            id_list = group[relation_type]

            if relation_type == "parallels":
                full_list = [i for i in id_list if not i.startswith('~')]
                resembling_list = [i for i in id_list if i.startswith('~')]

                # 1. Quan hệ 'parallels' hai chiều giữa các kinh gốc (full-full)
                for source_id, target_id in combinations(full_list, 2):
                    base_source = _parse_sutta_id(source_id)
                    base_target = _parse_sutta_id(target_id)
                    sutta_map[base_source]['parallels'][source_id].append(target_id)
                    sutta_map[base_target]['parallels'][target_id].append(source_id)

                # 2. Quan hệ 'resembles' hai chiều giữa kinh gốc và kinh tương tự (full-resembling)
                if full_list and resembling_list:
                    for source_full in full_list:
                        base_source_full = _parse_sutta_id(source_full)
                        for target_resembling in resembling_list:
                            # --- THAY ĐỔI: Bỏ dấu ~ khỏi ID ---
                            cleaned_target = target_resembling.lstrip('~')
                            base_target_resembling = _parse_sutta_id(cleaned_target)
                            
                            sutta_map[base_source_full]['resembles'][source_full].append(cleaned_target)
                            sutta_map[base_target_resembling]['resembles'][cleaned_target].append(source_full)

            elif relation_type in ["mentions", "retells"]:
                full_list = [i for i in id_list if not i.startswith('~')]
                resembling_list = [i for i in id_list if i.startswith('~')]

                # 1. Quan hệ hai chiều giữa các kinh gốc (full-full)
                for source_id, target_id in combinations(full_list, 2):
                    base_source = _parse_sutta_id(source_id)
                    base_target = _parse_sutta_id(target_id)
                    sutta_map[base_source][relation_type][source_id].append(target_id)
                    sutta_map[base_target][relation_type][target_id].append(source_id)

                # 2. Quan hệ hai chiều giữa kinh gốc và kinh tương tự (full-resembling)
                if full_list and resembling_list:
                    for source_full in full_list:
                        base_source_full = _parse_sutta_id(source_full)
                        for target_resembling in resembling_list:
                            # --- THAY ĐỔI: Bỏ dấu ~ khỏi ID ---
                            cleaned_target = target_resembling.lstrip('~')
                            base_target_resembling = _parse_sutta_id(cleaned_target)
                            
                            sutta_map[base_source_full][relation_type][source_full].append(cleaned_target)
                            sutta_map[base_target_resembling][relation_type][cleaned_target].append(source_full)

        # Áp dụng sắp xếp và dọn dẹp trước khi ghi file
        final_data = _sort_and_clean_map(sutta_map)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)
        
        log.info(f"✅ Đã xử lý và lưu thành công file parallels vào: {output_path}")

    except KeyError as e:
        log.error(f"Lỗi cấu hình cho tác vụ 'parallels': thiếu key '{e}'")
    except Exception as e:
        log.exception(f"Đã xảy ra lỗi không mong muốn khi xử lý parallels: {e}")