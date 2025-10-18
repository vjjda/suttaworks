# Path: src/db_updater/post_processors/parallels/utils.py
import re

def natural_sort_key(s: str) -> list:
    """Tạo một sort key để sắp xếp chuỗi theo thứ tự tự nhiên."""
    return [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)]

def get_book_id(base_id: str) -> str:
    """Trích xuất ID 'sách' từ base_id (phần chữ trước số đầu tiên)."""
    book_id = re.split('([0-9])', base_id, 1)[0]
    return book_id.rstrip()

def parse_sutta_id(full_id: str) -> str:
    """Tách mã kinh gốc ra khỏi mã định danh đầy đủ."""
    cleaned_id = full_id.lstrip('~')
    return cleaned_id.split('#')[0]