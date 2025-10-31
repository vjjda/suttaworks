# Path: src/db_updater/post_tasks/parallels/parallels_transformer.py
from collections import defaultdict
from . import parallels_utils

__all__ = ["invert_to_segment_structure", "flatten_segment_map", "create_book_structure"]

def invert_to_segment_structure(category_map: dict) -> defaultdict:
    segment_map = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for base_id, relations in category_map.items():
        for rel_type, full_id_map in relations.items():
            for full_id, parallel_list in full_id_map.items():
                segment_map[base_id][full_id][rel_type].extend(parallel_list)
    return segment_map


def flatten_segment_map(segment_data: dict) -> dict:
    flat_map = {}
    for base_id, segments in segment_data.items():
        flat_map.update(segments)
    return flat_map


def create_book_structure(segment_data: dict) -> defaultdict:
    book_map = defaultdict(dict)
    for base_id, segments in segment_data.items():
        book_id = parallels_utils.get_book_id(base_id)
        book_map[book_id][base_id] = segments
    return book_map
