# Path: src/db_builder/processors/bilara_segment_processor.py
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)

# --- THAY ĐỔI TÊN CLASS ---
class BilaraSegmentProcessor:
    """Xử lý dữ liệu từ file manifest Bilara và tạo dữ liệu cho bảng Segments."""

    def __init__(self, config: Dict[str, Any]):
        folder_path = PROJECT_ROOT / config.get('folder', '')
        self.base_path = folder_path.parent
        self.manifest_path = PROJECT_ROOT / config.get('json', '')
        # --- CẬP NHẬT LOG ---
        logger.info(f"Khởi tạo BilaraSegmentProcessor với manifest: {self.manifest_path.name}")

    # ... (các hàm _parse_file và process không có thay đổi về logic) ...
    def _parse_file(self, full_file_path: Path, relative_path_str: str, type_name: str) -> List[Dict[str, Any]]:
        try:
            p = Path(relative_path_str)
            parts = p.parts
            
            try:
                type_index = parts.index(type_name)
                lang = parts[type_index + 1]
                author_uid = parts[type_index + 2]
            except (ValueError, IndexError):
                logger.warning(f"Cấu trúc đường dẫn không hợp lệ, không thể trích xuất metadata từ: {relative_path_str}")
                return []

            sutta_uid = full_file_path.stem.split('_')[0]

            with full_file_path.open('r', encoding='utf-8') as f:
                data = json.load(f)

            segments = []
            for segment_uid, content in data.items():
                segments.append({
                    'segment_uid': segment_uid,
                    'sutta_uid': sutta_uid,
                    'type': type_name,
                    'author_uid': author_uid,
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