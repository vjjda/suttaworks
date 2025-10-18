# Path: src/db_builder/processors/bilara_segment_processor.py
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)

class BilaraSegmentProcessor:
    
    def __init__(self, config: Dict[str, Any]):
        folder_path = PROJECT_ROOT / config.get('folder', '')
        self.base_path = folder_path.parent
        self.manifest_path = PROJECT_ROOT / config.get('json', '')
        
        self.author_remap = config.get('author-remap', {})
        if self.author_remap:
            logger.info(f"Đã tải {len(self.author_remap)} quy tắc author-remap.")

        logger.info(f"Khởi tạo BilaraSegmentProcessor với manifest: {self.manifest_path.name}")

    def _parse_file_generic(self, full_file_path: Path, relative_path_str: str, type_name: str) -> List[Dict[str, Any]]:
        # Đây là logic chung để parse file, gần như không đổi
        try:
            p = Path(relative_path_str)
            parts = p.parts
            lang, author_alias = None, None
            
            try:
                type_index = parts.index(type_name)
                if len(parts) > type_index + 1: lang = parts[type_index + 1]
                if len(parts) > type_index + 2: author_alias = parts[type_index + 2]
            except (ValueError, IndexError): pass

            if not lang or not author_alias:
                return []

            if self.author_remap and author_alias in self.author_remap:
                author_alias = self.author_remap[author_alias]
            
            sc_uid = full_file_path.stem.split('_')[0]
            with full_file_path.open('r', encoding='utf-8') as f: data = json.load(f)

            segments = []
            for composite_uid, content in data.items():
                segment_num = composite_uid.split(':', 1)[1] if ':' in composite_uid else composite_uid
                segments.append({
                    'sc_uid': sc_uid, 'segment': segment_num, 'type': type_name, 
                    'lang': lang, 'author_alias': author_alias, 'content': content
                })
            return segments
        except Exception as e:
            logger.error(f"Lỗi khi xử lý file {full_file_path.name}: {e}", exc_info=True)
            return []

    def process(self, target_table: str) -> List[Dict[str, Any]]:
        if not self.manifest_path.exists():
            logger.error(f"File manifest không tồn tại: {self.manifest_path}")
            return []
        
        all_segments = []
        with open(self.manifest_path, 'r', encoding='utf-8') as f:
            manifest_data = json.load(f)

        for type_name, group_dict in manifest_data.items():
            if not isinstance(group_dict, dict): continue
            for file_uid, relative_path_str in group_dict.items():
                full_file_path = self.base_path / relative_path_str
                if not full_file_path.exists(): continue
                
                parsed_data = self._parse_file_generic(full_file_path, relative_path_str, type_name)
                
                # --- CẤU TRÚC GIỮ CHỖ CHO LOGIC TƯƠNG LAI ---
                if target_table == 'Bilara_names':
                    # TODO: Thêm logic lọc và biến đổi riêng cho Names
                    all_segments.extend(parsed_data)
                
                elif target_table == 'Bilara_blurbs':
                    # TODO: Thêm logic lọc và biến đổi riêng cho Blurbs
                    all_segments.extend(parsed_data)
                
                elif target_table == 'Bilara_sites':
                    # TODO: Thêm logic lọc và biến đổi riêng cho Sites
                    all_segments.extend(parsed_data)
                    
                elif target_table == 'Bilara_segments':
                    # TODO: Thêm logic lọc và biến đổi riêng cho Segments
                    all_segments.extend(parsed_data)
                
                else:
                    all_segments.extend(parsed_data)

        logger.info(f"✅ Đã xử lý {self.manifest_path.name}, tìm thấy tổng cộng {len(all_segments)} segment cho bảng '{target_table}'.")
        return all_segments