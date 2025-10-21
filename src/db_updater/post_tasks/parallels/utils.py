# Path: src/db_updater/post_tasks/parallels/utils.py
import re

# --- XÓA HÀM natural_sort_key(s: str) KHỎI ĐÂY ---

def get_book_id(base_id: str) -> str:
    """Trích xuất ID 'sách' từ base_id (phần chữ trước số đầu tiên)."""
    book_id = re.split('([0-9])', base_id, 1)[0]
    return book_id.rstrip()

def parse_sutta_id(full_id: str) -> str:
    """Tách mã kinh gốc ra khỏi mã định danh đầy đủ."""
    cleaned_id = full_id.lstrip('~')
    return cleaned_id.split('#')[0]