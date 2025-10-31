# Path: src/db_updater/post_tasks/parallels/parallels_utils.py
import re
from collections import defaultdict
from typing import Any
from natsort import natsorted

__all__ = ["get_book_id", "parse_sutta_id", "sort_data_naturally"]

RELATION_ORDER = ["parallels", "resembles", "mentions", "retells"]


def get_book_id(base_id: str) -> str:
    book_id = re.split("([0-9])", base_id, 1)[0]
    return book_id.rstrip()


def parse_sutta_id(full_id: str) -> str:
    cleaned_id = full_id.lstrip("~")
    return cleaned_id.split("#")[0]


def sort_data_naturally(data: Any) -> Any:
    if isinstance(data, (dict, defaultdict)):
        sorted_keys = natsorted(data.keys())

        if all(key in RELATION_ORDER for key in sorted_keys):
            sorted_keys.sort(
                key=lambda k: (
                    RELATION_ORDER.index(k)
                    if k in RELATION_ORDER
                    else len(RELATION_ORDER)
                )
            )

        return {key: sort_data_naturally(data[key]) for key in sorted_keys}

    if isinstance(data, list):
        unique_items = list(dict.fromkeys(data))
        try:
            return natsorted(unique_items)
        except TypeError:
            return [sort_data_naturally(item) for item in unique_items]

    return data