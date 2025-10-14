# Path: src/db_builder/processors/filepath_finder.py
import logging
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger(__name__)

class FilePathFinder:
    """Chuyên tìm kiếm đường dẫn các file bản dịch JSON."""

    def __init__(self, base_path: Path, json_dirs: List[Path]):
        self.base_path = base_path
        self.json_dirs = json_dirs
        self.filepath_map: Dict[str, Path] = {}

    def execute(self) -> Dict[str, Path]:
        """Quét các thư mục json và trả về map."""
        if not self.json_dirs:
            return {}
            
        count = 0
        for scan_dir in self.json_dirs:
            if not scan_dir.is_dir(): continue
            logger.info(f"Đang quét file JSON từ: {scan_dir}")
            for json_file in scan_dir.glob('**/*.json'):
                translation_id = json_file.stem
                self.filepath_map[translation_id] = json_file
                count += 1
        logger.info(f"Đã tạo map cho {count} file JSON.")
        return self.filepath_map