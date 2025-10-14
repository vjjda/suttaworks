# Path: src/db_builder/processors/suttaplex_extractor.py
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

class SuttaplexExtractor:
    """Chỉ chịu trách nhiệm đọc các file suttaplex JSON chính và trích xuất dữ liệu."""

    def __init__(self, suttaplex_dir: Path, biblio_map: Dict[str, str]):
        self.suttaplex_dir = suttaplex_dir
        self.biblio_map = biblio_map
        self.suttaplex_data: List[Dict[str, Any]] = []
        self.sutta_references_data: List[Dict[str, Any]] = []
        self.translations_data: List[Dict[str, Any]] = []
        self.authors_map: Dict[str, Dict[str, Any]] = {}
        self.languages_map: Dict[str, Dict[str, Any]] = {}
        self.valid_uids = set()
        self.uid_to_type_map: Dict[str, str] = {}

    def _clean_value(self, value):
        return value.strip() if isinstance(value, str) and value.strip() else (value if not isinstance(value, str) else None)

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
            self.languages_map[code] = {'lang_code': code, 'lang_name': self._clean_value(lang_name)}

    def execute(self):
        logger.info(f"Bắt đầu trích xuất dữ liệu suttaplex từ: {self.suttaplex_dir}")
        json_files = list(self.suttaplex_dir.glob('**/*.json'))

        for file_path in json_files:
            with open(file_path, 'r', encoding='utf-8') as f:
                data_list = json.load(f)
            
            for card in data_list:
                uid = card.get('uid')
                if not uid: continue

                self.valid_uids.add(uid)
                self.uid_to_type_map[uid] = card.get('type')

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

                for trans in card.get('translations', []):
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
                        'file_path': None
                    })
        
        return self