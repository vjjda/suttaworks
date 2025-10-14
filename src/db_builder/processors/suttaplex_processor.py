# Path: src/db_builder/processors/suttaplex_processor.py
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple
from collections import defaultdict
from bs4 import BeautifulSoup

from src.config.constants import PROJECT_ROOT
from .suttaplex_extractor import SuttaplexExtractor
from .filepath_finder import FilePathFinder

logger = logging.getLogger(__name__)

class SuttaplexProcessor:
    """Điều phối viên: sử dụng Extractor và Finder để xử lý dữ liệu suttaplex."""

    def __init__(self, suttaplex_config: List[Dict[str, Any]], biblio_map: Dict[str, str]):
        # ... (phần __init__ không đổi, vẫn đọc config như cũ)
        self.biblio_map = biblio_map
        self.suttaplex_dir = Path()
        self.json_segment_dirs: List[Path] = []
        self.html_text_dirs: List[Path] = []
        self.html_ignore_paths: List[Path] = []
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

    def _process_and_map_html_files(self, authors_map: Dict, known_translation_ids: set) -> Dict[str, Path]:
        """Quét, xử lý file HTML và trả về một filepath_map."""
        if not self.html_text_dirs: return {}

        # --- THAY ĐỔI: Tạo map một-nhiều cho author_name -> [uid1, uid2] ---
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
                # ... (logic ignore, lấy sutta_uid và lang không đổi)
                is_ignored = any(html_file.is_relative_to(p) for p in self.html_ignore_paths)
                if is_ignored: continue

                sutta_uid = html_file.stem
                lang = html_file.relative_to(scan_dir).parts[0]

                with open(html_file, 'r', encoding='utf-8') as f: soup = BeautifulSoup(f, 'html.parser')
                author_tag = soup.find('meta', {'name': 'author'})
                if not (author_tag and 'content' in author_tag.attrs): continue
                
                author_name_from_meta = author_tag['content']
                
                # --- THAY ĐỔI: Tìm danh sách các uid tiềm năng ---
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

                # --- THAY ĐỔI: Logic "Thử và Sai" ---
                found_match = False
                for author_uid_candidate in potential_uids:
                    translation_id_candidate = f"{lang}_{sutta_uid}_{author_uid_candidate}"
                    if translation_id_candidate in known_translation_ids:
                        html_filepath_map[translation_id_candidate] = html_file
                        count += 1
                        found_match = True
                        break # Tìm thấy là dừng ngay
                
                if not found_match:
                     logger.debug(f"Đã tạo các ID {potential_uids} từ file {html_file} nhưng không khớp với suttaplex data.")

        logger.info(f"Đã tạo map cho {count} file HTML.")
        return html_filepath_map


    def process(self) -> Tuple[List[Dict[str, Any]], ...]:
        # Bước 1: Trích xuất dữ liệu cơ bản
        extractor = SuttaplexExtractor(self.suttaplex_dir, self.biblio_map).execute()
        
        # Bước 2: Tìm đường dẫn file JSON
        base_path = (PROJECT_ROOT / "data/raw/git").resolve()
        json_finder = FilePathFinder(base_path, self.json_segment_dirs)
        filepath_map = json_finder.execute()
        
        # Bước 3: Xử lý file HTML và cập nhật filepath_map
        known_translation_ids = {item['translation_id'] for item in extractor.translations_data}
        html_filepath_map = self._process_and_map_html_files(extractor.authors_map, known_translation_ids)
        filepath_map.update(html_filepath_map)

        # Bước 4: Kiểm tra và log các ID không khớp (chỉ cho HTML)
        for found_id, file_obj in html_filepath_map.items():
            if found_id not in known_translation_ids:
                 logger.warning(f"Tìm thấy file HTML '{file_obj}' cho translation_id '{found_id}', nhưng ID này không có trong suttaplex data.")

        # Bước 5: Cập nhật file_path vào dữ liệu translations
        logger.info("Cập nhật file_path cho các bản dịch...")

        # --- THAY ĐỔI: Logic cập nhật file_path ---
        logger.info("Cập nhật file_path cho các bản dịch...")
        for translation in extractor.translations_data:
            trans_id = translation.get('translation_id')
            if trans_id in filepath_map:
                file_obj = filepath_map[trans_id]
                # Chỉ chuyển đổi thành chuỗi tương đối ở bước cuối cùng này
                translation['file_path'] = str(file_obj.relative_to(base_path))
        
        authors_data = list(extractor.authors_map.values())
        languages_data = list(extractor.languages_map.values())
        
        logger.info("✅ Hoàn tất xử lý suttaplex.")
        
        return (
            extractor.suttaplex_data, 
            extractor.sutta_references_data, 
            authors_data, 
            languages_data, 
            extractor.translations_data, 
            extractor.valid_uids, 
            extractor.uid_to_type_map
        )