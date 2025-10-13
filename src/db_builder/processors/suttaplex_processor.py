# Path: src/db_builder/processors/suttaplex_processor.py
#!/usr/bin/env python3

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)

class SuttaplexProcessor:
    """Xử lý dữ liệu suttaplex để điền vào các bảng liên quan."""

    def __init__(self, suttaplex_config: List[Dict[str, str]], biblio_map: Dict[str, str]):
        self.suttaplex_dir = PROJECT_ROOT / suttaplex_config[0]['data']
        self.biblio_map = biblio_map
        self.suttaplex_data: List[Dict[str, Any]] = []
        self.sutta_references_data: List[Dict[str, Any]] = []


    def process(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], set]:
        """Quét, đọc, trích xuất dữ liệu và trả về một set các UID hợp lệ."""
        valid_uids = set() # <-- TẠO SET MỚI
        logger.info(f"Bắt đầu quét dữ liệu suttaplex từ thư mục: {self.suttaplex_dir}")

        json_files = list(self.suttaplex_dir.glob('**/*.json'))
        logger.info(f"Tìm thấy {len(json_files)} file JSON để xử lý.")

        for file_path in json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data_list = json.load(f)
                
                for index, suttaplex_card in enumerate(data_list):
                    if not isinstance(suttaplex_card, dict):
                        continue

                    uid = suttaplex_card.get('uid')
                    if not uid:
                        logger.warning(f"Bỏ qua suttaplex card tại index {index} trong file '{file_path.name}' vì thiếu 'uid'.")
                        continue
                    valid_uids.add(uid)

                    def clean_value(value):
                        if isinstance(value, str):
                            stripped_value = value.strip()
                            return stripped_value if stripped_value else None
                        return value
                    
                    # Dữ liệu cho bảng Suttaplex
                    suttaplex = {
                        'uid': uid,
                        'root_lang': clean_value(suttaplex_card.get('root_lang')),
                        'acronym': clean_value(suttaplex_card.get('acronym')),
                        'translated_title': clean_value(suttaplex_card.get('translated_title')),
                        'original_title': clean_value(suttaplex_card.get('original_title')),
                        'blurb': clean_value(suttaplex_card.get('blurb')),
                    }
                    self.suttaplex_data.append(suttaplex)

                    # Dữ liệu cho bảng Sutta_References
                    biblio_text = clean_value(suttaplex_card.get('biblio'))
                    
                    reference_entry = {
                        'uid': uid,
                        'volpages': clean_value(suttaplex_card.get('volpages')),
                        'alt_volpages': clean_value(suttaplex_card.get('alt_volpages')),
                        'biblio_uid': self.biblio_map.get(biblio_text) if biblio_text else None,
                        'verseNo': clean_value(suttaplex_card.get('verseNo')),
                    }

                    has_useful_data = any(value is not None for key, value in reference_entry.items() if key != 'uid')
                    if has_useful_data:
                        self.sutta_references_data.append(reference_entry)

            # --- THÊM LẠI KHỐI EXCEPT BỊ THIẾU ---
            except Exception as e:
                logger.error(f"Lỗi khi xử lý file {file_path.name}: {e}", exc_info=True)
            # ------------------------------------
        
        logger.info(f"✅ Đã trích xuất {len(self.suttaplex_data)} Suttaplex, {len(self.sutta_references_data)} Sutta_References, và tìm thấy {len(valid_uids)} UID hợp lệ.")
        return self.suttaplex_data, self.sutta_references_data, valid_uids