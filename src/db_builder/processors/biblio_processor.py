# Path: src/db_builder/processors/biblio_processor.py
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)

class BiblioProcessor:
    """Xử lý dữ liệu từ biblio.json."""

    def __init__(self, biblio_path: str):
        self.biblio_path = PROJECT_ROOT / biblio_path
        self.biblio_data: List[Dict[str, Any]] = []
        self.text_to_uid_map: Dict[str, str] = {}

    def process(self) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
        """Đọc, trích xuất dữ liệu và tạo map tra cứu ngược."""
        logger.info(f"Bắt đầu xử lý file bibliography từ: {self.biblio_path}")
        if not self.biblio_path.exists():
            logger.error(f"Không tìm thấy file bibliography tại: {self.biblio_path}")
            return [], {}

        try:
            with open(self.biblio_path, 'r', encoding='utf-8') as f:
                # Dữ liệu là một list
                data_list = json.load(f)

            # --- THAY ĐỔI: Duyệt qua danh sách (list) thay vì từ điển (dict) ---
            for biblio_entry in data_list:
                if not isinstance(biblio_entry, dict):
                    continue
                
                # Lấy uid từ bên trong mỗi đối tượng
                uid = biblio_entry.get('uid')
                if not uid:
                    continue

                text_content = biblio_entry.get('text')
                
                self.biblio_data.append({
                    'uid': uid,
                    'name': biblio_entry.get('name'),
                    'text': text_content,
                })

                if text_content:
                    self.text_to_uid_map[text_content] = uid
            
            logger.info(f"✅ Đã trích xuất {len(self.biblio_data)} mục bibliography.")
            return self.biblio_data, self.text_to_uid_map
        except Exception as e:
            logger.error(f"Lỗi khi xử lý file {self.biblio_path.name}: {e}")
            return [], {}