# Path: src/db_builder/processors/suttaplex_processor.py
#!/usr/bin/env python3

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

# --- MỚI: Import thư viện cần thiết ---
from bs4 import BeautifulSoup

from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)

class SuttaplexProcessor:
    """Xử lý dữ liệu suttaplex để điền vào các bảng liên quan."""

    def __init__(self, suttaplex_config: List[Dict[str, Any]], biblio_map: Dict[str, str]):
        self.biblio_map = biblio_map
        
        self.suttaplex_dir = Path()
        self.json_segment_dirs: List[Path] = []
        # --- MỚI: Thêm danh sách cho html_text ---
        self.html_text_dirs: List[Path] = []
        
        for item in suttaplex_config:
            if 'data' in item:
                self.suttaplex_dir = PROJECT_ROOT / item['data']
            if 'translation_files' in item:
                tf_config = item['translation_files']
                if 'json_segment' in tf_config and isinstance(tf_config['json_segment'], list):
                    for p in tf_config['json_segment']:
                        self.json_segment_dirs.append(PROJECT_ROOT / p)
                if 'html_text' in tf_config and isinstance(tf_config['html_text'], list):
                    for p in tf_config['html_text']:
                        self.html_text_dirs.append(PROJECT_ROOT / p)

        self.suttaplex_data = []
        self.sutta_references_data = []
        self.translations_data = []
        self.authors_map = {}
        self.languages_map = {}
        self.filepath_map = {}

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
    
    # --- THAY ĐỔI: Đổi tên hàm và cập nhật base_path ---
    def _build_json_filepath_map(self):
        """Quét các thư mục json và tạo map."""
        if not self.json_segment_dirs: return
        
        # --- THAY ĐỔI: base_path giờ trỏ đến thư mục cha của 'suttacentral-data' ---
        base_path = (PROJECT_ROOT / "data/raw/git").resolve()
        
        count = 0
        for scan_dir in self.json_segment_dirs:
            if not scan_dir.is_dir(): continue
            logger.info(f"Đang quét file JSON từ: {scan_dir}")
            for json_file in scan_dir.glob('**/*.json'):
                translation_id = json_file.stem
                self.filepath_map[translation_id] = str(json_file.relative_to(base_path))
                count += 1
        logger.info(f"Đã tạo map cho {count} file JSON.")

    # --- MỚI: Hàm xử lý file HTML ---
    def _build_html_filepath_map(self):
        """Quét các thư mục html, trích xuất metadata và cập nhật map."""
        if not self.html_text_dirs: return

        # Tạo map tra cứu ngược: author_name -> author_uid
        author_name_to_uid = {
            v['author_name']: k for k, v in self.authors_map.items() if v.get('author_name')
        }
        if not author_name_to_uid:
            logger.warning("Không có author_name nào trong map, không thể xử lý file HTML.")
            return

        base_path = (PROJECT_ROOT / "data/raw/git").resolve()
        count = 0
        
        for scan_dir in self.html_text_dirs:
            if not scan_dir.is_dir(): continue
            logger.info(f"Đang quét file HTML từ: {scan_dir}")
            for html_file in scan_dir.glob('**/*.html'):
                try:
                    with open(html_file, 'r', encoding='utf-8') as f:
                        soup = BeautifulSoup(f, 'html.parser')

                    # Trích xuất thông tin
                    author_tag = soup.find('meta', {'name': 'author'})
                    article_tag = soup.find('article')

                    if not (author_tag and article_tag and 'content' in author_tag.attrs and article_tag.get('id') and article_tag.get('lang')):
                        logger.debug(f"Bỏ qua file HTML thiếu metadata: {html_file.name}")
                        continue
                    
                    author_name = author_tag['content']
                    sutta_uid = article_tag['id']
                    lang = article_tag['lang']

                    # Tra cứu author_uid
                    author_uid = author_name_to_uid.get(author_name)
                    if not author_uid:
                        logger.warning(f"Không tìm thấy author_uid cho '{author_name}' trong file {html_file.name}")
                        continue
                    
                    # Xây dựng translation_id và cập nhật map
                    translation_id = f"{lang}_{sutta_uid}_{author_uid}" # <-- Logic mới
                    self.filepath_map[translation_id] = str(html_file.relative_to(base_path))
                    count += 1

                except Exception as e:
                    logger.error(f"Lỗi khi xử lý file HTML {html_file.name}: {e}")
        
        logger.info(f"Đã tạo/cập nhật map cho {count} file HTML.")


    def process(self) -> Tuple[List[Dict[str, Any]], ...]:
        """Quét, đọc, trích xuất dữ liệu và trả về dữ liệu cho các bảng."""
        
        valid_uids = set()
        uid_to_type_map = {}
        logger.info(f"Bắt đầu quét dữ liệu suttaplex từ thư mục: {self.suttaplex_dir}")

        json_files = list(self.suttaplex_dir.glob('**/*.json'))
        logger.info(f"Tìm thấy {len(json_files)} file JSON để xử lý.")

        # Bước 1: Quét suttaplex để lấy dữ liệu thô và xây dựng các map tra cứu
        for file_path in json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f: data_list = json.load(f)
                
                for card in data_list:
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
                            self.translations_data.append({
                                'translation_id': self._clean_value(trans.get('id')),
                                'sutta_uid': uid, 'author_uid': self._clean_value(trans.get('author_uid')),
                                'lang': self._clean_value(trans.get('lang')), 'title': self._clean_value(trans.get('title')),
                                'publication_date': self._clean_value(trans.get('publication_date')),
                                'segmented': 1 if trans.get('segmented') else 0,
                                'has_comment': 1 if trans.get('has_comment') else 0,
                                'is_root': 1 if trans.get('is_root') else 0,
                                'file_path': None # Tạm thời để None
                            })
            except Exception as e:
                logger.error(f"Lỗi khi xử lý file suttaplex {file_path.name}: {e}", exc_info=True)

        # Bước 2: Xây dựng bản đồ file_path từ các nguồn (JSON, HTML)
        self._build_json_filepath_map()
        self._build_html_filepath_map()

        # Bước 3: Cập nhật file_path vào self.translations_data
        logger.info("Cập nhật file_path cho các bản dịch...")
        for translation in self.translations_data:
            trans_id = translation.get('translation_id')
            if trans_id in self.filepath_map:
                translation['file_path'] = self.filepath_map[trans_id]
        
        authors_data = list(self.authors_map.values())
        languages_data = list(self.languages_map.values())
        
        logger.info("✅ Đã trích xuất:")
        logger.info(f"  - {len(self.suttaplex_data)} Suttaplex cards")
        logger.info(f"  - {len(self.sutta_references_data)} Sutta References")
        logger.info(f"  - {len(self.translations_data)} Translations")
        logger.info(f"  - {len(authors_data)} Authors (duy nhất)")
        logger.info(f"  - {len(languages_data)} Languages (duy nhất)")
        
        return (self.suttaplex_data, self.sutta_references_data, authors_data, 
                languages_data, self.translations_data, valid_uids, uid_to_type_map)