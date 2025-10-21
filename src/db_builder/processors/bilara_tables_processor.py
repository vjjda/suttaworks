# Path: src/db_builder/processors/bilara_tables_processor.py

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List

from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)

class BilaraTablesProcessor:
    """
    Xử lý dữ liệu từ các file manifest của Bilara, biến đổi và định dạng lại
    dữ liệu cho các bảng đích khác nhau.
    """
    
    def __init__(self, config: Dict[str, Any]):
        folder_path = PROJECT_ROOT / config.get('folder', '')
        self.base_path = folder_path.parent
        self.manifest_path = PROJECT_ROOT / config.get('json', '')
        self.author_remap = config.get('author-remap', {})

    def _parse_raw_data(self) -> List[Dict[str, Any]]:
        """Đọc file manifest và trích xuất toàn bộ dữ liệu thô từ các file JSON con."""
        raw_data_list = []
        try:
            with self.manifest_path.open('r', encoding='utf-8') as f:
                manifest_data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            logger.error(f"Không thể đọc hoặc file manifest không tồn tại: {self.manifest_path}")
            return []

        for type_name, group_dict in manifest_data.items():
            if not isinstance(group_dict, dict): continue
            
            for file_uid, relative_path_str in group_dict.items():
                full_file_path = self.base_path / relative_path_str
                if not full_file_path.exists(): continue

                try:
                    p = Path(relative_path_str)
                    parts = p.parts
                    lang, author_alias = None, None
                    
                    type_index = parts.index(type_name)
                    if len(parts) > type_index + 1: lang = parts[type_index + 1]
                    if len(parts) > type_index + 2: author_alias = parts[type_index + 2]

                    if self.author_remap and author_alias in self.author_remap:
                        author_alias = self.author_remap.get(author_alias, author_alias)

                    sc_uid = full_file_path.stem.split('_')[0]
                    with full_file_path.open('r', encoding='utf-8') as f:
                        data = json.load(f)

                    for composite_uid, content in data.items():
                        segment_num = composite_uid.split(':', 1)[1] if ':' in composite_uid else composite_uid
                        raw_data_list.append({
                            'sc_uid': sc_uid, 'segment': segment_num, 'type': type_name, 
                            'lang': lang, 'author_alias': author_alias, 'content': content
                        })
                except (ValueError, IndexError, json.JSONDecodeError) as e:
                    logger.warning(f"Bỏ qua file bị lỗi định dạng {relative_path_str}: {e}")
                    continue
        return raw_data_list

    def _transform_for_sites(self, data: List[Dict]) -> List[Dict]:
        """Biến đổi dữ liệu cho bảng Bilara_sites."""
        transformed = []
        for row in data:
            if not all(k in row for k in ['sc_uid', 'segment', 'lang', 'content']): continue
            transformed.append({
                'sc_uid': row['sc_uid'],
                'segment': row['segment'],
                'lang': row['lang'],
                'content': row['content']
            })
        return transformed

    def _transform_for_blurbs(self, data: List[Dict]) -> List[Dict]:
        """Biến đổi dữ liệu cho bảng Bilara_blurbs."""
        transformed = []
        for row in data:
            if not all(k in row for k in ['segment', 'lang', 'content']): continue
            transformed.append({
                'sc_uid': row['segment'],
                'lang': row['lang'],
                'content': row['content']
            })
        return transformed

    def _transform_for_names(self, data: List[Dict]) -> List[Dict]:
        """Biến đổi dữ liệu cho bảng Bilara_names."""
        transformed = []
        for row in data:
            if not all(k in row for k in ['segment', 'lang', 'content']): continue
            modified_segment = re.sub(r'^\d+\.\s*', '', row['segment'])
            transformed.append({
                'sc_uid': modified_segment,
                'lang': row['lang'],
                'content': row['content']
            })
        return transformed

    def _transform_for_segments(self, data: List[Dict]) -> List[Dict]:
        """Giữ nguyên dữ liệu cho bảng Bilara_segments (chưa có yêu cầu biến đổi)."""
        return data

    def process(self, target_table: str) -> List[Dict[str, Any]]:
        """Hàm điều phối: đọc dữ liệu thô và gọi hàm biến đổi phù hợp."""
        logger.info(f"Bắt đầu xử lý dữ liệu cho bảng '{target_table}' từ manifest '{self.manifest_path.name}'.")
        raw_data = self._parse_raw_data()
        if not raw_data:
            logger.warning(f"Không tìm thấy dữ liệu thô nào từ manifest '{self.manifest_path.name}'.")
            return []

        if target_table == 'Bilara_sites':
            final_data = self._transform_for_sites(raw_data)
        elif target_table == 'Bilara_blurbs':
            final_data = self._transform_for_blurbs(raw_data)
        elif target_table == 'Bilara_names':
            final_data = self._transform_for_names(raw_data)
        elif target_table == 'Bilara_segments':
            final_data = self._transform_for_segments(raw_data)
        else:
            logger.warning(f"Không có logic biến đổi nào được định nghĩa cho bảng '{target_table}'.")
            final_data = []

        logger.info(f"✅  Đã xử lý xong, tạo ra {len(final_data)} hàng cho bảng '{target_table}'.")
        return final_data