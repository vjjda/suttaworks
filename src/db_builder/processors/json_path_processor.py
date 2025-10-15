# Path: src/db_builder/processors/json_path_processor.py
import logging
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger(__name__)

class JsonPathProcessor:  # <-- Tên lớp đã đổi
    """Quét các thư mục được chỉ định để tạo một map từ translation_id sang đường dẫn file JSON."""

    def __init__(self, base_path: Path, scan_dirs: List[Path]):
        self.base_path = base_path
        self.scan_dirs = scan_dirs

    def execute(self) -> Dict[str, Path]:
        logger.info(f"Bắt đầu quét {len(self.scan_dirs)} thư mục JSON...")
        filepath_map = {}
        count = 0
        for scan_dir in self.scan_dirs:
            if not scan_dir.is_dir():
                logger.warning(f"Thư mục không tồn tại, bỏ qua: {scan_dir}")
                continue
            
            # Quét tất cả các file .json trong thư mục và các thư mục con
            for json_file in scan_dir.glob('**/*.json'):
                # translation_id thường là tên file không có đuôi .json
                translation_id = json_file.stem
                filepath_map[translation_id] = json_file
                count += 1
        
        logger.info(f"✅ Đã tạo map cho {count} file JSON.")
        return filepath_map