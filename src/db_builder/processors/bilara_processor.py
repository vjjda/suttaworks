# Path: src/db_builder/processors/bilara_processor.py
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)

class BilaraProcessor:
    """Xử lý dữ liệu từ kho Bilara và tạo dữ liệu cho bảng Segments."""

    def __init__(self, config: Dict[str, Any]):
        self.base_path = PROJECT_ROOT / config.get('path', '')
        self.types_to_scan = config.get('types', [])
        logger.info(f"Khởi tạo BilaraProcessor với đường dẫn: {self.base_path}")

    def _parse_file(self, file_path: Path, type_name: str) -> List[Dict[str, Any]]:
        """
        Phân tích một file JSON duy nhất để trích xuất metadata và nội dung.
        Cấu trúc đường dẫn dự kiến: .../{type}/{lang}/{author}/.../{filename}.json
        """
        try:
            # Lấy đường dẫn tương đối so với thư mục type (ví dụ: "root", "translation")
            relative_path = file_path.relative_to(self.base_path / type_name)
            parts = relative_path.parts

            if len(parts) < 2:
                logger.warning(f"Cấu trúc thư mục không hợp lệ, bỏ qua file: {file_path}")
                return []

            lang = parts[0]
            author_uid = parts[1]
            
            # sutta_uid được lấy từ tên file, trước dấu "_" đầu tiên
            sutta_uid = file_path.stem.split('_')[0]

            with file_path.open('r', encoding='utf-8') as f:
                data = json.load(f)

            segments = []
            for segment_id, content in data.items():
                segments.append({
                    'segment_id': segment_id,
                    'sutta_uid': sutta_uid,
                    'type': type_name,
                    'author_uid': author_uid,
                    'lang': lang,
                    'content': content
                })
            return segments
        except (json.JSONDecodeError, IndexError, FileNotFoundError) as e:
            logger.error(f"Lỗi khi xử lý file {file_path.name}: {e}")
            return []

    def process(self) -> List[Dict[str, Any]]:
        """Quét tất cả các file JSON trong các thư mục được cấu hình và xử lý chúng."""
        if not self.base_path.exists():
            logger.error(f"Đường dẫn Bilara không tồn tại: {self.base_path}")
            return []

        all_segments = []
        logger.info(f"Bắt đầu quét dữ liệu Bilara cho các loại: {self.types_to_scan}")

        for type_name in self.types_to_scan:
            type_dir = self.base_path / type_name
            if not type_dir.is_dir():
                logger.warning(f"Không tìm thấy thư mục cho loại '{type_name}': {type_dir}")
                continue

            logger.info(f"--- Đang quét thư mục: {type_name} ---")
            json_files = list(type_dir.glob('**/*.json'))
            
            for file_path in json_files:
                segments_from_file = self._parse_file(file_path, type_name)
                all_segments.extend(segments_from_file)

        logger.info(f"✅ Đã xử lý xong dữ liệu Bilara, tìm thấy tổng cộng {len(all_segments)} segment.")
        return all_segments