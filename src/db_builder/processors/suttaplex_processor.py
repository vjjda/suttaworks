# Path: src/db_builder/processors/suttaplex_processor.py
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

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
        # --- MỚI: Thêm danh sách ignore ---
        self.html_ignore_paths: List[Path] = []

        for item in suttaplex_config:
            if 'data' in item:
                self.suttaplex_dir = PROJECT_ROOT / item['data']
            if 'translation_files' in item:
                tf_config = item['translation_files']
                if 'json_segment' in tf_config and isinstance(tf_config['json_segment'], list):
                    for p in tf_config['json_segment']:
                        self.json_segment_dirs.append(PROJECT_ROOT / p)
                
                # --- THAY ĐỔI: Xử lý cấu trúc mới của html_text ---
                if 'html_text' in tf_config and isinstance(tf_config['html_text'], list):
                    for p in tf_config['html_text']:
                        if isinstance(p, str):
                            self.html_text_dirs.append(PROJECT_ROOT / p)
                        elif isinstance(p, dict) and 'ignore' in p:
                            for ignore_path in p['ignore']:
                                self.html_ignore_paths.append(PROJECT_ROOT / "data/raw/git/suttacentral-data" / ignore_path)


    def process(self) -> Tuple[List[Dict[str, Any]], ...]:
        # Bước 1: Trích xuất dữ liệu cơ bản
        extractor = SuttaplexExtractor(self.suttaplex_dir, self.biblio_map).execute()
        
        # Bước 2: Tìm tất cả các đường dẫn file
        base_path = (PROJECT_ROOT / "data/raw/git").resolve()
        author_name_map = {v['author_name']: k for k, v in extractor.authors_map.items() if v.get('author_name')}
        
        # --- THAY ĐỔI: Truyền danh sách ignore vào FilePathFinder ---
        finder = FilePathFinder(base_path, self.json_segment_dirs, self.html_text_dirs, self.html_ignore_paths, author_name_map)
        filepath_map = finder.execute()

        # ... (phần còn lại của hàm không đổi)
        logger.info("Cập nhật file_path cho các bản dịch...")
        for translation in extractor.translations_data:
            trans_id = translation.get('translation_id')
            if trans_id in filepath_map:
                translation['file_path'] = filepath_map[trans_id]
        
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