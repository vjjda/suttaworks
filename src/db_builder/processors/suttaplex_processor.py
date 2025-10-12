# Path: src/db_builder/processors/suttaplex_processor.py
#!/usr/bin/env python3

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)

class SuttaplexProcessor:
    """Xử lý dữ liệu suttaplex để điền vào các bảng Suttaplex và Misc."""

    def __init__(self, suttaplex_config: List[Dict[str, str]]):
        self.suttaplex_dir = PROJECT_ROOT / suttaplex_config[0]['data']
        # --- Đảm bảo tên biến nhất quán: suttaplex_data (số ít) ---
        self.suttaplex_data: List[Dict[str, Any]] = []
        self.misc_data: List[Dict[str, Any]] = []

    def process(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Quét, đọc, và trích xuất dữ liệu suttaplex thành 2 danh sách."""
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

                    def clean_value(value):
                        """
                        Làm sạch khoảng trắng thừa và chuyển giá trị rỗng thành None.
                        """
                        # Chỉ xử lý nếu giá trị là một chuỗi
                        if isinstance(value, str):
                            stripped_value = value.strip()
                            # Trả về None nếu sau khi strip, chuỗi trở nên rỗng
                            return stripped_value if stripped_value else None
                        # Trả về giá trị gốc nếu không phải chuỗi (ví dụ: số, None)
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

                    # Dữ liệu cho bảng Misc
                    difficulty_obj = suttaplex_card.get('difficulty')
                    misc = {
                        'uid': uid,
                        'volpages': clean_value(suttaplex_card.get('volpages')),
                        'alt_volpages': clean_value(suttaplex_card.get('alt_volpages')),
                        'parallel_count': suttaplex_card.get('parallel_count'),
                        'biblio_uid': clean_value(suttaplex_card.get('biblio')),
                        'verseNo': clean_value(suttaplex_card.get('verseNo')),
                        'difficulty': difficulty_obj.get('level') if difficulty_obj else None,
                    }
                    self.misc_data.append(misc)

            except Exception as e:
                logger.error(f"Lỗi khi xử lý file {file_path.name}: {e}", exc_info=True)
        
        logger.info(f"✅ Đã trích xuất {len(self.suttaplex_data)} Suttaplex và {len(self.misc_data)} Misc records.")
        return self.suttaplex_data, self.misc_data