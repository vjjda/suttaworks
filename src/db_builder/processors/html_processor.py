# Path: src/db_builder/processors/html_processor.py
import logging
from pathlib import Path
from typing import Dict, List, Set
from collections import defaultdict
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class HtmlFileProcessor:
    """Chuyên xử lý logic phức tạp của việc quét và tạo map cho các file HTML."""

    def __init__(self, html_text_dirs: List[Path], ignore_paths: List[Path], 
                 authors_map: Dict, known_translation_ids: Set):
        self.html_text_dirs = html_text_dirs
        self.ignore_paths = ignore_paths
        self.authors_map = authors_map
        self.known_translation_ids = known_translation_ids

    def execute(self) -> Dict[str, Path]:
        """Quét, xử lý file HTML và trả về một filepath_map."""
        if not self.html_text_dirs: 
            return {}

        author_name_to_uids = defaultdict(list)
        for uid, data in self.authors_map.items():
            if data.get('author_name'):
                author_name_to_uids[data['author_name']].append(uid)
        
        author_short_to_uids = defaultdict(list)
        for uid, data in self.authors_map.items():
            if data.get('author_short'):
                author_short_to_uids[data['author_short'].lower()].append(uid)

        html_filepath_map = {}
        count = 0
        for scan_dir in self.html_text_dirs:
            if not scan_dir.is_dir(): continue
            for html_file in scan_dir.glob('**/*.html'):
                try:
                    if html_file.stem == 'sf36':
                        translation_id = 'sf36_root'
                        if translation_id in self.known_translation_ids:
                            html_filepath_map[translation_id] = html_file
                            count += 1
                        else:
                            logger.warning(f"Quy tắc đặc biệt cho '{html_file.name}': không tìm thấy translation_id '{translation_id}' trong suttaplex data.")
                        continue

                    is_ignored = any(html_file.is_relative_to(p) for p in self.ignore_paths)
                    if is_ignored:
                        continue

                    sutta_uid = html_file.stem
                    lang = html_file.relative_to(scan_dir).parts[0]
                    with open(html_file, 'r', encoding='utf-8') as f: 
                        soup = BeautifulSoup(f, 'html.parser')

                    author_tag = soup.find('meta', {'name': 'author'})
                    if not (author_tag and 'content' in author_tag.attrs): 
                        continue
                    
                    author_name_from_meta = author_tag['content']
                    
                    potential_uids = author_name_to_uids.get(author_name_from_meta, [])
                    if not potential_uids:
                        potential_uids = author_short_to_uids.get(author_name_from_meta.lower(), [])
                    if not potential_uids:
                        parent_dir_name = html_file.parent.name
                        if parent_dir_name in self.authors_map:
                            potential_uids = [parent_dir_name]

                    if not potential_uids:
                        logger.warning(f"Không tìm thấy author_uid nào cho '{author_name_from_meta}' trong file {html_file}")
                        continue

                    found_match = False
                    for author_uid_candidate in potential_uids:
                        if author_uid_candidate == 'taisho':
                            standard_id = f"{lang}_{sutta_uid}_taisho"
                            if standard_id in self.known_translation_ids:
                                html_filepath_map[standard_id] = html_file
                                count += 1
                                found_match = True
                                break

                            special_id = f"{sutta_uid}_root-lzh-sct"
                            if special_id in self.known_translation_ids:
                                html_filepath_map[special_id] = html_file
                                count += 1
                                found_match = True
                                break
                        else:
                            standard_id = f"{lang}_{sutta_uid}_{author_uid_candidate}"
                            if standard_id in self.known_translation_ids:
                                html_filepath_map[standard_id] = html_file
                                count += 1
                                found_match = True
                                break
                    
                    if not found_match:
                        logger.debug(f"Đã tạo các ID ứng viên từ author '{author_name_from_meta}' của file {html_file} nhưng không khớp với suttaplex data.")

                except Exception as e:
                    logger.error(f"Lỗi khi xử lý file HTML {html_file}: {e}")
        
        logger.info(f"Đã tạo map cho {count} file HTML.")
        return html_filepath_map