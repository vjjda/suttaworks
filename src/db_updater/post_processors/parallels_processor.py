# Path: src/db_updater/post_processors/parallels_processor.py
import json
import logging
import re
from pathlib import Path
from itertools import combinations
from collections import defaultdict

log = logging.getLogger(__name__)


def _natural_sort_key(s: str) -> list:
    """Tạo một sort key để sắp xếp chuỗi theo thứ tự tự nhiên."""
    return [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)]


def _sort_category_map(sutta_map: defaultdict) -> dict:
    """Sắp xếp map theo cấu trúc Category: base_id -> rel_type -> full_id."""
    relation_order = ["parallels", "resembles", "mentions", "retells"]
    final_sorted_map = {}

    for base_id in sorted(sutta_map.keys(), key=_natural_sort_key):
        relations = sutta_map[base_id]
        sorted_relations = {}

        def get_relation_sort_key(relation_type):
            try: return relation_order.index(relation_type)
            except ValueError: return len(relation_order)

        for rel_type in sorted(relations.keys(), key=get_relation_sort_key):
            full_id_map = relations[rel_type]
            sorted_full_id_map = {}

            for full_id in sorted(full_id_map.keys(), key=_natural_sort_key):
                parallel_list = full_id_map[full_id]
                unique_sorted_list = sorted(list(dict.fromkeys(parallel_list)), key=_natural_sort_key)
                if unique_sorted_list:
                    sorted_full_id_map[full_id] = unique_sorted_list
            if sorted_full_id_map:
                sorted_relations[rel_type] = sorted_full_id_map
        if sorted_relations:
            final_sorted_map[base_id] = sorted_relations
    return final_sorted_map


def _invert_to_segment_structure(category_map: dict) -> defaultdict:
    """Chuyển đổi map từ view Category sang view Segment."""
    segment_map = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for base_id, relations in category_map.items():
        for rel_type, full_id_map in relations.items():
            for full_id, parallel_list in full_id_map.items():
                segment_map[base_id][full_id][rel_type].extend(parallel_list)
    return segment_map


def _sort_segment_map(segment_map: defaultdict) -> dict:
    """Sắp xếp map theo cấu trúc Segment: base_id -> full_id -> rel_type."""
    relation_order = ["parallels", "resembles", "mentions", "retells"]
    final_sorted_map = {}

    for base_id in sorted(segment_map.keys(), key=_natural_sort_key):
        segments = segment_map[base_id]
        sorted_segments = {}

        for full_id in sorted(segments.keys(), key=_natural_sort_key):
            relations = segments[full_id]
            sorted_relations = {}

            def get_relation_sort_key(relation_type):
                try: return relation_order.index(relation_type)
                except ValueError: return len(relation_order)

            for rel_type in sorted(relations.keys(), key=get_relation_sort_key):
                parallel_list = relations[rel_type]
                unique_sorted_list = sorted(list(dict.fromkeys(parallel_list)), key=_natural_sort_key)
                if unique_sorted_list:
                    sorted_relations[rel_type] = unique_sorted_list
            if sorted_relations:
                sorted_segments[full_id] = sorted_relations
        if sorted_segments:
            final_sorted_map[base_id] = sorted_segments
    return final_sorted_map


def _parse_sutta_id(full_id: str) -> str:
    """Tách mã kinh gốc ra khỏi mã định danh đầy đủ."""
    cleaned_id = full_id.lstrip('~')
    return cleaned_id.split('#')[0]


def process_parallels_data(task_config: dict, project_root: Path):
    """
    Đọc file parallels.json gốc, xử lý và tạo ra các file kết quả theo cấu hình.
    Hỗ trợ output dạng category (sắp xếp theo loại quan hệ) và segment (sắp xếp theo ID đoạn).
    """
    try:
        input_path = project_root / task_config['path']
        output_config = task_config['output']

        # --- THAY ĐỔI: Xử lý cấu hình output linh hoạt ---
        category_output_path, segment_output_path = None, None
        if isinstance(output_config, str):
            category_output_path = project_root / output_config
        elif isinstance(output_config, dict):
            if 'category' in output_config:
                category_output_path = project_root / output_config['category']
            if 'segment' in output_config:
                segment_output_path = project_root / output_config['segment']
        
        if not category_output_path and not segment_output_path:
            log.error("Lỗi cấu hình 'parallels': không có đường dẫn output hợp lệ.")
            return

        log.info(f"Bắt đầu xử lý file parallels: {input_path}")
        if not input_path.exists():
            log.error(f"Không tìm thấy file input: {input_path}")
            return

        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Luôn build map theo cấu trúc Category trước
        sutta_map = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        # ... (toàn bộ logic xử lý for group in data... không thay đổi)
        for group in data:
            relation_type = list(group.keys())[0]
            id_list = group[relation_type]

            if relation_type == "parallels":
                full_list = [i for i in id_list if not i.startswith('~')]
                resembling_list = [i for i in id_list if i.startswith('~')]

                for source_id, target_id in combinations(full_list, 2):
                    base_source = _parse_sutta_id(source_id)
                    base_target = _parse_sutta_id(target_id)
                    sutta_map[base_source]['parallels'][source_id].append(target_id)
                    sutta_map[base_target]['parallels'][target_id].append(source_id)

                if full_list and resembling_list:
                    for source_full in full_list:
                        base_source_full = _parse_sutta_id(source_full)
                        for target_resembling in resembling_list:
                            cleaned_target = target_resembling.lstrip('~')
                            base_target_resembling = _parse_sutta_id(cleaned_target)
                            sutta_map[base_source_full]['resembles'][source_full].append(cleaned_target)
                            sutta_map[base_target_resembling]['resembles'][cleaned_target].append(source_full)

            elif relation_type in ["mentions", "retells"]:
                full_list = [i for i in id_list if not i.startswith('~')]
                resembling_list = [i for i in id_list if i.startswith('~')]

                for source_id, target_id in combinations(full_list, 2):
                    base_source = _parse_sutta_id(source_id)
                    base_target = _parse_sutta_id(target_id)
                    sutta_map[base_source][relation_type][source_id].append(target_id)
                    sutta_map[base_target][relation_type][target_id].append(source_id)

                if full_list and resembling_list:
                    for source_full in full_list:
                        base_source_full = _parse_sutta_id(source_full)
                        for target_resembling in resembling_list:
                            cleaned_target = target_resembling.lstrip('~')
                            base_target_resembling = _parse_sutta_id(cleaned_target)
                            sutta_map[base_source_full][relation_type][source_full].append(cleaned_target)
                            sutta_map[base_target_resembling][relation_type][cleaned_target].append(source_full)

        # --- THAY ĐỔI: Xử lý và ghi các file output ---
        if category_output_path:
            category_data = _sort_category_map(sutta_map)
            category_output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(category_output_path, 'w', encoding='utf-8') as f:
                json.dump(category_data, f, ensure_ascii=False, indent=2)
            log.info(f"✅ Đã lưu file Category vào: {category_output_path}")

        if segment_output_path:
            segment_map = _invert_to_segment_structure(sutta_map)
            segment_data = _sort_segment_map(segment_map)
            segment_output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(segment_output_path, 'w', encoding='utf-8') as f:
                json.dump(segment_data, f, ensure_ascii=False, indent=2)
            log.info(f"✅ Đã lưu file Segment vào: {segment_output_path}")

    except KeyError as e:
        log.error(f"Lỗi cấu hình cho tác vụ 'parallels': thiếu key '{e}'")
    except Exception as e:
        log.exception(f"Đã xảy ra lỗi không mong muốn khi xử lý parallels: {e}")