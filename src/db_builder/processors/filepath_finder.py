# Path: src/db_builder/processors/filepath_finder.py
import logging
from pathlib import Path
from typing import Dict, List
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class FilePathFinder:
    """Chuyên tìm kiếm đường dẫn các file bản dịch JSON và HTML."""

    # --- THAY ĐỔI: Thêm html_ignore_paths vào __init__ ---
    def __init__(self, base_path: Path, json_dirs: List[Path], html_dirs: List[Path], html_ignore_paths: List[Path], author_name_map: Dict[str, str]):
        self.base_path = base_path
        self.json_dirs = json_dirs
        self.html_dirs = html_dirs
        self.html_ignore_paths = html_ignore_paths
        self.author_name_to_uid = author_name_map
        self.filepath_map: Dict[str, str] = {}

    def _find_json_filepaths(self):
        if not self.json_dirs: return
        count = 0
        for scan_dir in self.json_dirs:
            if not scan_dir.is_dir(): continue
            logger.info(f"Đang quét file JSON từ: {scan_dir}")
            for json_file in scan_dir.glob('**/*.json'):
                translation_id = json_file.stem
                self.filepath_map[translation_id] = str(json_file.relative_to(self.base_path))
                count += 1
        logger.info(f"Đã tạo map cho {count} file JSON.")

    def _find_html_filepaths(self):
        if not self.html_dirs: return
        if not self.author_name_to_uid:
            logger.warning("Không có author_name nào trong map, không thể xử lý file HTML.")
            return
        
        count = 0
        for scan_dir in self.html_dirs:
            if not scan_dir.is_dir(): continue
            logger.info(f"Đang quét file HTML từ: {scan_dir}")
            for html_file in scan_dir.glob('**/*.html'):
                # --- THAY ĐỔI: Kiểm tra xem file có nằm trong danh sách ignore không ---
                is_ignored = any(html_file.is_relative_to(p) for p in self.html_ignore_paths)
                if is_ignored:
                    logger.debug(f"Bỏ qua file trong thư mục ignore: {html_file}")
                    continue
                # -----------------------------------------------------------------
                try:
                    # ... (phần còn lại của hàm không đổi)
                    with open(html_file, 'r', encoding='utf-8') as f:
                        soup = BeautifulSoup(f, 'html.parser')

                    author_tag = soup.find('meta', {'name': 'author'})
                    article_tag = soup.find('article')

                    if not (author_tag and article_tag and 'content' in author_tag.attrs and article_tag.get('id') and article_tag.get('lang')):
                        continue
                    
                    author_name = author_tag['content']
                    sutta_uid = article_tag['id']
                    lang = article_tag['lang']
                    author_uid = self.author_name_to_uid.get(author_name)
                    
                    if not author_uid:
                        logger.warning(f"Không tìm thấy author_uid cho '{author_name}' trong file {html_file.name}")
                        continue
                    
                    translation_id = f"{lang}_{sutta_uid}_{author_uid}"
                    self.filepath_map[translation_id] = str(html_file.relative_to(self.base_path))
                    count += 1
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý file HTML {html_file.name}: {e}")
        logger.info(f"Đã tạo/cập nhật map cho {count} file HTML.")

    def execute(self) -> Dict[str, str]:
        self._find_json_filepaths()
        self._find_html_filepaths()
        return self.filepath_map