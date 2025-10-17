# Path: src/db_builder/processors/bilara_segment_processor.py
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from src.config.constants import PROJECT_ROOT
# DatabaseManager không còn cần thiết ở đây
# from src.db_builder.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

class BilaraSegmentProcessor:
    """
    Xử lý dữ liệu Bilara bằng cách trích xuất lang và author_alias
    thuần túy dựa trên vị trí trong đường dẫn.
    """

    def __init__(self, config: Dict[str, Any]):
        # __init__ giờ đây rất đơn giản, không cần kết nối DB hay đọc file phụ
        folder_path = PROJECT_ROOT / config.get('folder', '')
        self.base_path = folder_path.parent
        self.manifest_path = PROJECT_ROOT / config.get('json', '')
        logger.info(f"Khởi tạo BilaraSegmentProcessor (chế độ đơn giản) với manifest: {self.manifest_path.name}")

    def _parse_file(self, full_file_path: Path, relative_path_str: str, type_name: str) -> List[Dict[str, Any]]:
        """
        Phân tích file JSON, trích xuất metadata dựa trên vị trí cố định.
        """
        try:
            p = Path(relative_path_str)
            parts = p.parts
            lang = None
            author_alias = None
            
            # --- LOGIC ĐƠN GIẢN: LẤY DỮ LIỆU THEO VỊ TRÍ CỐ ĐỊNH ---
            try:
                type_index = parts.index(type_name)
                # lang là thư mục ở vị trí +1 sau type
                if len(parts) > type_index + 1:
                    lang = parts[type_index + 1]
                # author_alias là thư mục ở vị trí +2 sau type
                if len(parts) > type_index + 2:
                    author_alias = parts[type_index + 2]
            except (ValueError, IndexError):
                logger.warning(f"Cấu trúc đường dẫn không hợp lệ, không thể trích xuất metadata từ: {relative_path_str}")
            # --- KẾT THÚC LOGIC ĐƠN GIẢN ---
            
            sutta_uid = full_file_path.stem.split('_')[0]
            with full_file_path.open('r', encoding='utf-8') as f:
                data = json.load(f)

            segments = []
            for segment_uid, content in data.items():
                segments.append({
                    'segment_uid': segment_uid, 'sutta_uid': sutta_uid,
                    'type': type_name, 'author_alias': author_alias,
                    'lang': lang, 'content': content
                })
            return segments
        except Exception as e:
            logger.error(f"Lỗi khi xử lý file {full_file_path.name}: {e}", exc_info=True)
            return []

    def process(self) -> List[Dict[str, Any]]:
        # Hàm này giữ nguyên không thay đổi
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