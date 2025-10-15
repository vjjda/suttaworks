# Path: src/db_builder/processors/blurb_processor.py
import logging
from pathlib import Path
from typing import Any, Dict, List
import csv

logger = logging.getLogger(__name__)

class BlurbSupplementProcessor:
    """Đọc các file TSV bổ sung và cập nhật các blurb còn thiếu."""

    def __init__(self, supplement_paths: List[Path]):
        self.supplement_paths = supplement_paths

    def execute(self, suttaplex_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not self.supplement_paths:
            logger.debug("Không có file blurb bổ sung nào được cấu hình. Bỏ qua.")
            return suttaplex_data

        logger.info(f"Bắt đầu áp dụng blurb bổ sung từ {len(self.supplement_paths)} file...")
        
        blurb_map = {}
        for file_path in self.supplement_paths:
            if not file_path.exists():
                logger.warning(f"  -> Không tìm thấy file: {file_path.name}. Bỏ qua.")
                continue
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f, delimiter='\t')
                    for row in reader:
                        if 'uid' in row and 'blurb' in row:
                            blurb_map[row['uid']] = row['blurb']
                logger.info(f"  -> Đã đọc thành công {file_path.name}")
            except Exception as e:
                logger.error(f"Lỗi khi đọc file TSV bổ sung {file_path.name}: {e}")
        
        suttaplex_map = {item['uid']: item for item in suttaplex_data}
        
        update_count = 0
        for uid, new_blurb in blurb_map.items():
            if uid in suttaplex_map and not suttaplex_map[uid].get('blurb'):
                suttaplex_map[uid]['blurb'] = new_blurb.strip()
                update_count += 1
        
        logger.info(f"✅ Đã cập nhật {update_count} blurb từ các file bổ sung.")
        
        return list(suttaplex_map.values())