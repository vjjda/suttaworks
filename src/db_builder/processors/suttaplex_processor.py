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

    def __init__(self, suttaplex_config: List[Dict[str, Any]], biblio_map: Dict[str, str]):
        self.biblio_map = biblio_map
        
        # --- THAY ĐỔI: Chuyển sang lưu một danh sách các thư mục cần quét ---
        self.suttaplex_dir = Path()
        self.json_segment_dirs: List[Path] = []
        
        for item in suttaplex_config:
            if 'data' in item:
                self.suttaplex_dir = PROJECT_ROOT / item['data']
            if 'translation_files' in item and 'json_segment' in item['translation_files']:
                # Đọc danh sách các đường dẫn
                paths = item['translation_files']['json_segment']
                if isinstance(paths, list):
                    for p in paths:
                        self.json_segment_dirs.append(PROJECT_ROOT / p)
        
        if not self.suttaplex_dir.exists():
            logger.warning(f"Đường dẫn suttaplex không hợp lệ: {self.suttaplex_dir}")

        # Dữ liệu đầu ra và map tra cứu
        self.suttaplex_data = []
        self.sutta_references_data = []
        self.translations_data = []
        self.authors_map = {}
        self.languages_map = {}
        self.filepath_map = {}

    # ... (các hàm _clean_value, _add_author, _add_language không đổi)
    def _clean_value(self, value):
        if isinstance(value, str):
            stripped_value = value.strip()
            return stripped_value if stripped_value else None
        return value

    def _add_author(self, author_data: Dict[str, Any]):
        author_uid = self._clean_value(author_data.get('author_uid'))
        if author_uid and author_uid not in self.authors_map:
            self.authors_map[author_uid] = {
                'author_uid': author_uid,
                'author_name': self._clean_value(author_data.get('author')),
                'author_short': self._clean_value(author_data.get('author_short')),
            }
            
    def _add_language(self, lang_code: str | None, lang_name: str | None):
        code = self._clean_value(lang_code)
        if code and code not in self.languages_map:
            self.languages_map[code] = {
                'lang_code': code,
                'lang_name': self._clean_value(lang_name)
            }
            
    def _build_translation_filepath_map(self):
        """Quét các thư mục trong json_segment và tạo map từ translation_id đến file_path."""
        if not self.json_segment_dirs:
            logger.warning("Không có thư mục 'json_segment' nào được cấu hình. Bỏ qua việc điền file_path.")
            return

        # --- THAY ĐỔI: Định nghĩa base_path để loại bỏ tiền tố 'suttacentral-data' ---
        base_path = PROJECT_ROOT / "data/raw/git/suttacentral-data"
        logger.info(f"Đường dẫn gốc để tính relative path: {base_path}")

        count = 0
        # --- THAY ĐỔI: Lặp qua danh sách các thư mục cần quét ---
        for scan_dir in self.json_segment_dirs:
            if not scan_dir.is_dir():
                logger.warning(f"Bỏ qua vì không phải thư mục: {scan_dir}")
                continue

            logger.info(f"Bắt đầu quét file bản dịch từ: {scan_dir}")
            for json_file in scan_dir.glob('**/*.json'):
                translation_id = json_file.stem
                # Tính toán đường dẫn tương đối so với `suttacentral-data`
                relative_path = str(json_file.relative_to(base_path))
                self.filepath_map[translation_id] = relative_path
                count += 1
        
        logger.info(f"Đã tạo map cho {count} file bản dịch từ {len(self.json_segment_dirs)} thư mục.")

    def process(self) -> Tuple[List[Dict[str, Any]], ...]:
        # (Nội dung hàm này không thay đổi, chỉ gọi hàm _build_translation_filepath_map đã được sửa ở trên)
        self._build_translation_filepath_map()

        valid_uids = set()
        uid_to_type_map = {}
        logger.info(f"Bắt đầu quét dữ liệu suttaplex từ thư mục: {self.suttaplex_dir}")

        json_files = list(self.suttaplex_dir.glob('**/*.json'))
        logger.info(f"Tìm thấy {len(json_files)} file JSON để xử lý.")

        for file_path in json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f: data_list = json.load(f)
                
                for index, card in enumerate(data_list):
                    if not isinstance(card, dict): continue
                    uid = card.get('uid')
                    if not uid: continue
                    valid_uids.add(uid)
                    uid_to_type_map[uid] = card.get('type')
                    self.suttaplex_data.append({
                        'uid': uid, 'root_lang': self._clean_value(card.get('root_lang')),
                        'acronym': self._clean_value(card.get('acronym')),
                        'translated_title': self._clean_value(card.get('translated_title')),
                        'original_title': self._clean_value(card.get('original_title')),
                        'blurb': self._clean_value(card.get('blurb')),
                        'priority_author_uid': self._clean_value(card.get('priority_author_uid')),
                    })
                    self._add_language(card.get('root_lang'), card.get('root_lang_name'))
                    biblio_text = self._clean_value(card.get('biblio'))
                    reference_entry = {
                        'uid': uid, 'volpages': self._clean_value(card.get('volpages')),
                        'alt_volpages': self._clean_value(card.get('alt_volpages')),
                        'biblio_uid': self.biblio_map.get(biblio_text) if biblio_text else None,
                        'verseNo': self._clean_value(card.get('verseNo')),
                    }
                    if any(v is not None for k, v in reference_entry.items() if k != 'uid'):
                        self.sutta_references_data.append(reference_entry)

                    translations = card.get('translations', [])
                    if isinstance(translations, list):
                        for trans in translations:
                            self._add_author(trans)
                            self._add_language(trans.get('lang'), trans.get('lang_name'))
                            
                            translation_id = self._clean_value(trans.get('id'))
                            
                            self.translations_data.append({
                                'translation_id': translation_id,
                                'sutta_uid': uid,
                                'author_uid': self._clean_value(trans.get('author_uid')),
                                'lang': self._clean_value(trans.get('lang')),
                                'title': self._clean_value(trans.get('title')),
                                'publication_date': self._clean_value(trans.get('publication_date')),
                                'segmented': 1 if trans.get('segmented') else 0,
                                'has_comment': 1 if trans.get('has_comment') else 0,
                                'is_root': 1 if trans.get('is_root') else 0,
                                'file_path': self.filepath_map.get(translation_id)
                            })
            
            except Exception as e:
                logger.error(f"Lỗi khi xử lý file {file_path.name}: {e}", exc_info=True)
        
        authors_data = list(self.authors_map.values())
        languages_data = list(self.languages_map.values())
        
        logger.info(f"✅ Đã trích xuất:")
        logger.info(f"  - {len(self.suttaplex_data)} Suttaplex cards")
        logger.info(f"  - {len(self.sutta_references_data)} Sutta References")
        logger.info(f"  - {len(self.translations_data)} Translations")
        logger.info(f"  - {len(authors_data)} Authors (duy nhất)")
        logger.info(f"  - {len(languages_data)} Languages (duy nhất)")
        
        return (self.suttaplex_data, self.sutta_references_data, authors_data, 
                languages_data, self.translations_data, valid_uids, uid_to_type_map)