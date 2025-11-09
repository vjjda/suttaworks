# Path: src/db_updater/post_tasks/parallels/__init__.py
from .parallels_processor import build_initial_map
from .parallels_transformer import (
    create_book_structure,
    flatten_segment_map,
    invert_to_segment_structure,
)
from .parallels_utils import sort_data_naturally

__all__ = [
    "build_initial_map",
    "create_book_structure",
    "flatten_segment_map",
    "invert_to_segment_structure",
    "sort_data_naturally",
]
