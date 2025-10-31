# Path: src/db_updater/post_tasks/html_text_authors_task.py
import logging
import json
from pathlib import Path
from typing import Dict
from bs4 import BeautifulSoup

from src.config import constants

log = logging.getLogger(__name__)


def run(task_config: Dict):
    """Adapter function to be called by the BaseHandler."""
    process_html_text_authors_data(task_config, constants.PROJECT_ROOT)


def process_html_text_authors_data(config: Dict, project_root: Path):
    try:
        base_path = project_root / config["path"]
        output_file = project_root / config["output"]
        ignore_list = config.get("ignore", [])
    except KeyError as e:
        log.error(f"Thiếu key bắt buộc trong cấu hình 'html_text': {e}")
        return

    if not base_path.is_dir():
        log.error(f"Thư mục nguồn cho 'html_text' không tồn tại: {base_path}")
        return

    ignore_paths = [base_path.joinpath(p).resolve() for p in ignore_list]
    log.info(f"Các thư mục sẽ bị bỏ qua: {ignore_paths}")

    log.info(f"Bắt đầu quét file HTML từ: {base_path}")
    author_map = {}
    total_files_scanned = 0
    ignored_files_count = 0

    for html_file in base_path.glob("**/*.html"):
        total_files_scanned += 1

        is_ignored = any(
            html_file.resolve().is_relative_to(ignored_dir)
            for ignored_dir in ignore_paths
        )

        if is_ignored:
            ignored_files_count += 1
            log.debug(f"Bỏ qua file: {html_file}")
            continue

        try:
            with open(html_file, "r", encoding="utf-8") as f:
                content = f.read()

            soup = BeautifulSoup(content, "html.parser")
            meta_tag = soup.find("meta", attrs={"name": "author"})

            if meta_tag and meta_tag.get("content"):
                author = meta_tag.get("content").strip()
                relative_path = html_file.relative_to(base_path)
                path_parts = relative_path.parts

                current_level = author_map
                for part in path_parts[:-1]:
                    current_level = current_level.setdefault(part, {})

                current_level[path_parts[-1]] = author
            else:
                log.warning(
                    f"Không tìm thấy thẻ <meta name='author'> trong file: {html_file.name}"
                )

        except Exception as e:
            log.error(f"Lỗi khi xử lý file {html_file.name}: {e}")

    log.info(
        f"Đã quét {total_files_scanned} file HTML, trong đó bỏ qua {ignored_files_count} file."
    )

    if author_map:
        processed_count = total_files_scanned - ignored_files_count
        log.info(
            f"Trích xuất được thông tin từ {processed_count} file. Đang chuẩn bị ghi file..."
        )
        output_file.parent.mkdir(parents=True, exist_ok=True)

        final_output = {"suttacentral-data": {"html_text": author_map}}

        try:
            with open(output_file, "w", encoding="utf-8") as f:

                json.dump(final_output, f, ensure_ascii=False, indent=2)
            log.info(f"✅ Đã tạo file tổng hợp tại: {output_file}")
        except IOError as e:
            log.error(f"Không thể ghi file JSON: {e}")
    else:
        log.warning("Không trích xuất được thông tin tác giả từ bất kỳ file HTML nào.")