# Path: src/db_builder/processors/bilara_segment_processor.py
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)

class BilaraSegmentProcessor:
    """Xử lý dữ liệu từ file manifest Bilara và tạo dữ liệu cho bảng Segments."""

    def __init__(self, config: Dict[str, Any]):
        folder_path = PROJECT_ROOT / config.get('folder', '')
        self.base_path = folder_path.parent
        self.manifest_path = PROJECT_ROOT / config.get('json', '')
        
        # --- LOGIC MỚI: TẠO DANH SÁCH TÁC GIẢ HỢP LỆ ---
        self.valid_authors = set()
        
        # 1. Đọc từ file _author.json
        authors_manifest_path = PROJECT_ROOT / config.get('authors', '')
        if authors_manifest_path.exists():
            try:
                with open(authors_manifest_path, 'r', encoding='utf-8') as f:
                    authors_data = json.load(f)
                    # Lấy tất cả các key (chính là author_uid) từ file JSON
                    self.valid_authors.update(authors_data.keys())
            except json.JSONDecodeError:
                logger.error(f"Lỗi khi đọc file JSON tác giả: {authors_manifest_path}")
        else:
            logger.warning(f"Không tìm thấy file manifest tác giả: {authors_manifest_path}")

        # 2. Đọc từ danh sách extra_authors
        extra_authors = config.get('extra_authors', [])
        self.valid_authors.update(extra_authors)
        
        logger.info(f"Khởi tạo BilaraSegmentProcessor với {len(self.valid_authors)} tác giả hợp lệ.")
        # --- KẾT THÚC LOGIC MỚI ---

    def _parse_file(self, full_file_path: Path, relative_path_str: str, type_name: str) -> List[Dict[str, Any]]:
        """
        Phân tích một file JSON, trích xuất metadata, và xác thực author_uid.
        """
        try:
            p = Path(relative_path_str)
            parts = p.parts
            
            potential_author_uid = None
            try:
                type_index = parts.index(type_name)
                lang = parts[type_index + 1]
                potential_author_uid = parts[type_index + 2]
            except (ValueError, IndexError):
                logger.warning(f"Cấu trúc đường dẫn không hợp lệ từ: {relative_path_str}")
                return []

            # --- LOGIC MỚI: XÁC THỰC AUTHOR_UID ---
            author_uid = None # Mặc định là None (sẽ thành NULL trong DB)
            if potential_author_uid in self.valid_authors:
                author_uid = potential_author_uid
            else:
                logger.debug(f"Author UID '{potential_author_uid}' từ path '{relative_path_str}' không hợp lệ. Đặt thành NULL.")
            # --- KẾT THÚC LOGIC MỚI ---

            sutta_uid = full_file_path.stem.split('_')[0]

            with full_file_path.open('r', encoding='utf-8') as f:
                data = json.load(f)

            segments = []
            for segment_uid, content in data.items():
                segments.append({
                    'segment_uid': segment_uid,
                    'sutta_uid': sutta_uid,
                    'type': type_name,
                    'author_uid': author_uid, # <-- Sử dụng author_uid đã được xác thực
                    'lang': lang,
                    'content': content
                })
            return segments
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Lỗi khi xử lý file {full_file_path.name}: {e}")
            return []
        except Exception as e:
            logger.error(f"Lỗi không xác định khi xử lý {full_file_path.name}: {e}")
            return []

    def process(self) -> List[Dict[str, Any]]:
        # ... (Hàm này giữ nguyên không thay đổi) ...
        if not self.manifest_path.exists():
            logger.error(f"File manifest Bilara không tồn tại: {self.manifest_path}")
            return []

        all_segments = []
        logger.info(f"Bắt đầu xử lý dữ liệu Bilara từ file manifest: {self.manifest_path.name}")

        try:
            with open(self.manifest_path, 'r', encoding='utf-8') as f:
                manifest_data = json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Lỗi khi đọc file manifest JSON: {e}")
            return []

        for type_name, group_dict in manifest_data.items():
            if not isinstance(group_dict, dict):
                continue
                
            logger.info(f"--- Đang xử lý group: {type_name} ({len(group_dict)} files) ---")
            
            for _, relative_path_str in group_dict.items():
                full_file_path = self.base_path / relative_path_str
                
                if not full_file_path.exists():
                    logger.warning(f"File được định nghĩa trong manifest không tồn tại: {full_file_path}")
                    continue

                segments_from_file = self._parse_file(full_file_path, relative_path_str, type_name)
                all_segments.extend(segments_from_file)

        logger.info(f"✅ Đã xử lý {self.manifest_path.name}, tìm thấy tổng cộng {len(all_segments)} segment.")
        return all_segments