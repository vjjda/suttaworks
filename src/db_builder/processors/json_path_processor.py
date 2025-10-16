# Path: src/db_builder/processors/json_path_processor.py
import json
import logging
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger(__name__)

class JsonPathProcessor:
    """
    Đọc một file manifest JSON (ví dụ: sc_bilara.json) để tạo map từ
    translation_id sang chuỗi đường dẫn tương đối đã có sẵn.
    """

    def __init__(self, manifest_path: Path, groups: List[str]):
        self.manifest_path = manifest_path
        self.groups = groups

    def execute(self) -> Dict[str, str]:
        logger.info(f"Bắt đầu xử lý file manifest JSON từ: {self.manifest_path.name}")
        
        if not self.manifest_path.exists():
            logger.error(f"Không tìm thấy file manifest: {self.manifest_path}")
            return {}

        filepath_map = {}
        try:
            with open(self.manifest_path, 'r', encoding='utf-8') as f:
                manifest_data = json.load(f)

            for group_name in self.groups:
                group_dict = manifest_data.get(group_name, {})
                
                if not isinstance(group_dict, dict):
                    logger.warning(f"Group '{group_name}' trong file manifest không phải là một dictionary. Bỏ qua.")
                    continue

                # Chỉ cần gộp dictionary từ manifest vào map của chúng ta
                # Key là translation_id, value là chuỗi đường dẫn y hệt trong file
                filepath_map.update(group_dict)

            count = len(filepath_map)
            logger.info(f"✅ Đã tạo map cho {count} file JSON từ file manifest.")
            return filepath_map

        except json.JSONDecodeError:
            logger.error(f"Lỗi khi giải mã file JSON: {self.manifest_path}")
            return {}
        except Exception as e:
            logger.error(f"Đã xảy ra lỗi không mong muốn: {e}")
            return {}