# Path: src/db_builder/processors/suttaplex_processor.py
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple
from collections import defaultdict
from bs4 import BeautifulSoup
import csv

from src.config.constants import PROJECT_ROOT
from .suttaplex_extractor import SuttaplexExtractor
from .filepath_finder import FilePathFinder

logger = logging.getLogger(__name__)

class SuttaplexProcessor:
    """Điều phối viên: sử dụng Extractor và Finder để xử lý dữ liệu suttaplex."""

    def __init__(self, suttaplex_config: List[Dict[str, Any]], biblio_map: Dict[str, str]):
        self.biblio_map = biblio_map
        self.suttaplex_dir = Path()
        self.json_segment_dirs: List[Path] = []
        self.html_text_dirs: List[Path] = []
        self.html_ignore_paths: List[Path] = []
        self.blurb_supplement_paths: List[Path] = []

        for item in suttaplex_config:
            if 'data' in item: self.suttaplex_dir = PROJECT_ROOT / item['data']
            if 'translation_files' in item:
                tf_config = item['translation_files']
                if 'json_segment' in tf_config and isinstance(tf_config['json_segment'], list):
                    for p in tf_config['json_segment']: self.json_segment_dirs.append(PROJECT_ROOT / p)
                if 'html_text' in tf_config and isinstance(tf_config['html_text'], list):
                    for p in tf_config['html_text']:
                        if isinstance(p, str): self.html_text_dirs.append(PROJECT_ROOT / p)
                        elif isinstance(p, dict) and 'ignore' in p:
                            for ignore_path in p['ignore']: self.html_ignore_paths.append(PROJECT_ROOT / "data/raw/git/suttacentral-data" / ignore_path)
            
            if 'blurb_supplement' in item and isinstance(item['blurb_supplement'], list):
                for path_str in item['blurb_supplement']:
                    self.blurb_supplement_paths.append(PROJECT_ROOT / path_str)

    def _apply_blurb_supplement(self, suttaplex_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Đọc các file TSV bổ sung và cập nhật các blurb còn thiếu."""
        if not self.blurb_supplement_paths:
            logger.info("Không có file blurb bổ sung nào được cấu hình. Bỏ qua.")
            return suttaplex_data

        logger.info(f"Bắt đầu áp dụng blurb bổ sung từ {len(self.blurb_supplement_paths)} file...")
        
        blurb_map = {}
        for file_path in self.blurb_supplement_paths:
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
            if uid in suttaplex_map:
                if not suttaplex_map[uid].get('blurb'):
                    suttaplex_map[uid]['blurb'] = new_blurb.strip()
                    update_count += 1
        
        logger.info(f"✅ Đã cập nhật {update_count} blurb từ các file bổ sung.")
        
        return list(suttaplex_map.values())

    def _process_and_map_html_files(self, authors_map: Dict, known_translation_ids: set) -> Dict[str, Path]:
        # ... (phần này không đổi)
        if not self.html_text_dirs: return {}
        author_name_to_uids = defaultdict(list)
        for uid, data in authors_map.items():
            if data.get('author_name'):
                author_name_to_uids[data['author_name']].append(uid)
        author_short_to_uids = defaultdict(list)
        for uid, data in authors_map.items():
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
                        if translation_id in known_translation_ids:
                            html_filepath_map[translation_id] = html_file
                            count += 1
                        else:
                            logger.warning(f"Quy tắc đặc biệt cho '{html_file.name}': không tìm thấy translation_id '{translation_id}' trong suttaplex data.")
                        continue
                    is_ignored = any(html_file.is_relative_to(p) for p in self.html_ignore_paths)
                    if is_ignored:
                        continue
                    sutta_uid = html_file.stem
                    lang = html_file.relative_to(scan_dir).parts[0]
                    with open(html_file, 'r', encoding='utf-8') as f: soup = BeautifulSoup(f, 'html.parser')
                    author_tag = soup.find('meta', {'name': 'author'})
                    if not (author_tag and 'content' in author_tag.attrs): continue
                    author_name_from_meta = author_tag['content']
                    potential_uids = author_name_to_uids.get(author_name_from_meta, [])
                    if not potential_uids:
                        potential_uids = author_short_to_uids.get(author_name_from_meta.lower(), [])
                    if not potential_uids:
                        parent_dir_name = html_file.parent.name
                        if parent_dir_name in authors_map:
                            potential_uids = [parent_dir_name]
                    if not potential_uids:
                        logger.warning(f"Không tìm thấy author_uid nào cho '{author_name_from_meta}' trong file {html_file}")
                        continue
                    found_match = False
                    for author_uid_candidate in potential_uids:
                        if author_uid_candidate == 'taisho':
                            standard_id = f"{lang}_{sutta_uid}_taisho"
                            if standard_id in known_translation_ids:
                                html_filepath_map[standard_id] = html_file
                                count += 1
                                found_match = True
                                break
                            special_id = f"{sutta_uid}_root-lzh-sct"
                            if special_id in known_translation_ids:
                                html_filepath_map[special_id] = html_file
                                count += 1
                                found_match = True
                                break
                        else:
                            standard_id = f"{lang}_{sutta_uid}_{author_uid_candidate}"
                            if standard_id in known_translation_ids:
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

    def process(self) -> Tuple[List[Dict[str, Any]], ...]:
        # Bước 1: Trích xuất dữ liệu cơ bản
        extractor = SuttaplexExtractor(self.suttaplex_dir, self.biblio_map).execute()
        
        # Bước 1.5: Áp dụng blurb bổ sung
        suttaplex_data = self._apply_blurb_supplement(extractor.suttaplex_data)
        
        # Bước 2: Tìm đường dẫn file JSON
        base_path = (PROJECT_ROOT / "data/raw/git").resolve()
        json_finder = FilePathFinder(base_path, self.json_segment_dirs)
        filepath_map = json_finder.execute()
        
        # Bước 3: Xử lý file HTML và cập nhật filepath_map
        known_translation_ids = {item['translation_id'] for item in extractor.translations_data}
        html_filepath_map = self._process_and_map_html_files(extractor.authors_map, known_translation_ids)
        filepath_map.update(html_filepath_map)

        # Bước 4: Kiểm tra và log các ID không khớp
        for found_id, file_obj in html_filepath_map.items():
            if found_id not in known_translation_ids:
                 logger.warning(f"Tìm thấy file HTML '{file_obj}' cho translation_id '{found_id}', nhưng ID này không có trong suttaplex data.")

        # Bước 5: Cập nhật file_path vào dữ liệu translations
        logger.info("Cập nhật file_path cho các bản dịch...")
        for translation in extractor.translations_data:
            trans_id = translation.get('translation_id')
            if trans_id in filepath_map:
                file_obj = filepath_map[trans_id]
                translation['file_path'] = str(file_obj.relative_to(base_path))
        
        authors_data = list(extractor.authors_map.values())
        languages_data = list(extractor.languages_map.values())
        
        logger.info("✅ Hoàn tất xử lý suttaplex.")
        
        return (
            suttaplex_data, 
            extractor.sutta_references_data, 
            authors_data, 
            languages_data, 
            extractor.translations_data, 
            extractor.valid_uids, 
            extractor.uid_to_type_map
        )