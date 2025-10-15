# Path: src/db_builder/processors/suttaplex_processor.py
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.config.constants import PROJECT_ROOT
from .suttaplex_extractor import SuttaplexExtractor
from .json_path_processor import JsonPathProcessor 
from .html_processor import HtmlFileProcessor
from .blurb_processor import BlurbSupplementProcessor

logger = logging.getLogger(__name__)

class SuttaplexProcessor:
    """Quản lý dự án: Điều phối các chuyên gia để xử lý dữ liệu suttaplex."""

    def __init__(self, suttaplex_config: List[Dict[str, Any]], biblio_map: Dict[str, str]):
        self.suttaplex_config = suttaplex_config
        self.biblio_map = biblio_map
        self.suttaplex_dir = next((PROJECT_ROOT / item['data'] for item in suttaplex_config if 'data' in item), Path())
        
    def _parse_config_paths(self):
        """Hàm phụ để lấy các loại đường dẫn từ file config."""
        json_dirs, html_dirs, ignore_paths, blurb_paths = [], [], [], []
        
        for item in self.suttaplex_config:
            if 'translation_files' in item:
                tf_config = item['translation_files']
                if 'json_segment' in tf_config:
                    json_dirs.extend([PROJECT_ROOT / p for p in tf_config['json_segment']])
                if 'html_text' in tf_config:
                    for p in tf_config['html_text']:
                        if isinstance(p, str):
                            html_dirs.append(PROJECT_ROOT / p)
                        elif isinstance(p, dict) and 'ignore' in p:
                            ignore_paths.extend([PROJECT_ROOT / "data/raw/git/suttacentral-data" / ip for ip in p['ignore']])
            if 'blurb_supplement' in item:
                blurb_paths.extend([PROJECT_ROOT / p for p in item['blurb_supplement']])
                
        return json_dirs, html_dirs, ignore_paths, blurb_paths

    def process(self) -> Tuple[List[Dict[str, Any]], ...]:
        # 1. Trích xuất dữ liệu thô từ JSON
        extractor = SuttaplexExtractor(self.suttaplex_dir, self.biblio_map).execute()
        
        # Lấy các đường dẫn từ file config
        json_dirs, html_dirs, ignore_paths, blurb_paths = self._parse_config_paths()
        
        # 2. Xử lý dữ liệu blurb bổ sung
        blurb_processor = BlurbSupplementProcessor(blurb_paths)
        suttaplex_data = blurb_processor.execute(extractor.suttaplex_data)

        # 3. Tìm đường dẫn file JSON cho các bản dịch
        base_path = (PROJECT_ROOT / "data/raw/git").resolve()
        json_processor = JsonPathProcessor(base_path, json_dirs)
        filepath_map = json_processor.execute()

        # 4. Xử lý file HTML và cập nhật map
        known_ids = {item['translation_id'] for item in extractor.translations_data}
        html_processor = HtmlFileProcessor(html_dirs, ignore_paths, extractor.authors_map, known_ids)
        html_filepath_map = html_processor.execute()
        filepath_map.update(html_filepath_map)

        # 5. Cập nhật file_path vào dữ liệu translations
        logger.info("Cập nhật file_path cho các bản dịch...")
        for translation in extractor.translations_data:
            trans_id = translation.get('translation_id')
            if trans_id in filepath_map:
                translation['file_path'] = str(filepath_map[trans_id].relative_to(base_path))
        
        logger.info("✅ Hoàn tất xử lý suttaplex.")
        
        return (
            suttaplex_data, 
            extractor.sutta_references_data, 
            list(extractor.authors_map.values()), 
            list(extractor.languages_map.values()), 
            extractor.translations_data, 
            extractor.valid_uids, 
            extractor.uid_to_type_map
        )