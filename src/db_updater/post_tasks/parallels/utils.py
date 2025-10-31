# Path: src/db_updater/post_tasks/parallels/utils.py
import re


def get_book_id(base_id: str) -> str:
    book_id = re.split("([0-9])", base_id, 1)[0]
    return book_id.rstrip()


def parse_sutta_id(full_id: str) -> str:
    cleaned_id = full_id.lstrip("~")
    return cleaned_id.split("#")[0]