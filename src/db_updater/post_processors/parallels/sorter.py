# Path: src/db_updater/post_processors/parallels/sorter.py
from . import utils

RELATION_ORDER = ["parallels", "resembles", "mentions", "retells"]

def _get_relation_sort_key(relation_type: str) -> int:
    try:
        return RELATION_ORDER.index(relation_type)
    except ValueError:
        return len(RELATION_ORDER)

def sort_category_map(sutta_map: dict) -> dict:
    """Sắp xếp map theo cấu trúc Category: base_id -> rel_type -> full_id."""
    final_sorted_map = {}
    for base_id in sorted(sutta_map.keys(), key=utils.natural_sort_key):
        relations = sutta_map[base_id]
        sorted_relations = {}
        for rel_type in sorted(relations.keys(), key=_get_relation_sort_key):
            full_id_map = relations[rel_type]
            sorted_full_id_map = {}
            for full_id in sorted(full_id_map.keys(), key=utils.natural_sort_key):
                p_list = full_id_map[full_id]
                unique_sorted_list = sorted(list(dict.fromkeys(p_list)), key=utils.natural_sort_key)
                if unique_sorted_list:
                    sorted_full_id_map[full_id] = unique_sorted_list
            if sorted_full_id_map:
                sorted_relations[rel_type] = sorted_full_id_map
        if sorted_relations:
            final_sorted_map[base_id] = sorted_relations
    return final_sorted_map

def sort_segment_map(segment_map: dict) -> dict:
    """Sắp xếp map theo cấu trúc Segment: base_id -> full_id -> rel_type."""
    final_sorted_map = {}
    for base_id in sorted(segment_map.keys(), key=utils.natural_sort_key):
        segments = segment_map[base_id]
        sorted_segments = {}
        for full_id in sorted(segments.keys(), key=utils.natural_sort_key):
            relations = segments[full_id]
            sorted_relations = {}
            for rel_type in sorted(relations.keys(), key=_get_relation_sort_key):
                p_list = relations[rel_type]
                unique_sorted_list = sorted(list(dict.fromkeys(p_list)), key=utils.natural_sort_key)
                if unique_sorted_list:
                    sorted_relations[rel_type] = unique_sorted_list
            if sorted_relations:
                sorted_segments[full_id] = sorted_relations
        if sorted_segments:
            final_sorted_map[base_id] = sorted_segments
    return final_sorted_map

def sort_flat_segment_map(flat_map: dict) -> dict:
    """Sắp xếp map flat_segment."""
    final_sorted_map = {}
    for full_id in sorted(flat_map.keys(), key=utils.natural_sort_key):
        relations = flat_map[full_id]
        sorted_relations = {}
        for rel_type in sorted(relations.keys(), key=_get_relation_sort_key):
            p_list = relations[rel_type]
            unique_sorted_list = sorted(list(dict.fromkeys(p_list)), key=utils.natural_sort_key)
            if unique_sorted_list:
                sorted_relations[rel_type] = unique_sorted_list
        if sorted_relations:
            final_sorted_map[full_id] = sorted_relations
    return final_sorted_map

def sort_book_map(book_map: dict) -> dict:
    """Sắp xếp map theo cấu trúc Book."""
    final_sorted_map = {}
    for book_id in sorted(book_map.keys(), key=utils.natural_sort_key):
        base_id_map = book_map[book_id]
        sorted_base_id_map = {}
        for base_id in sorted(base_id_map.keys(), key=utils.natural_sort_key):
            sorted_base_id_map[base_id] = base_id_map[base_id]
        final_sorted_map[book_id] = sorted_base_id_map
    return final_sorted_map