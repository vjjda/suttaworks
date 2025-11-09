# Path: src/db_updater/post_tasks/html_text_authors_task.py
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from src.config import constants

log = logging.getLogger(__name__)


def _process_file(
    html_file: Path, base_path: Path
) -> Tuple[Optional[tuple], Optional[str]]:
    try:
        with open(html_file, "r", encoding="utf-8") as f:
            content = f.read()

        soup = BeautifulSoup(content, "html.parser")
        meta_tag = soup.find("meta", attrs={"name": "author"})

        if not meta_tag:
            return (None, None)

        author_content = meta_tag.get("content")

        if not isinstance(author_content, str):
            log.warning(
                f"Content không hợp lệ (không phải string) trong: {html_file.name}"
            )
            return (None, None)

        author = author_content.strip()
        if not author:
            return (None, None)

        relative_path = html_file.relative_to(base_path)
        return (relative_path.parts, author)

    except (IOError, UnicodeDecodeError) as e:
        log.warning(f"Lỗi I/O khi xử lý file {html_file.name}: {e}")
        return (None, None)
    except Exception as e:
        log.error(f"Lỗi không mong muốn khi phân tích {html_file.name}: {e}")
        return (None, None)


def run(task_config: Dict):
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

    log.info(f"Bắt đầu quét file HTML từ: {base_path} (tuần tự)")
    files_to_process: List[Path] = []
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
        files_to_process.append(html_file)

    log.info(
        f"Đã quét {total_files_scanned} file, bỏ qua {ignored_files_count} file. "
        f"Bắt đầu xử lý song song {len(files_to_process)} file."
    )

    if not files_to_process:
        log.warning("Không tìm thấy file HTML nào để xử lý.")
        return

    processed_results: List[Tuple[tuple, str]] = []

    with ThreadPoolExecutor() as executor:

        futures = {
            executor.submit(_process_file, html_file, base_path): html_file
            for html_file in files_to_process
        }

        for future in as_completed(futures):
            result = future.result()

            if result[0] is not None and result[1] is not None:

                processed_results.append((result[0], result[1]))

    log.info(
        f"Đã xử lý xong, thu được {len(processed_results)} kết quả. Đang xây dựng map..."
    )
    author_map: Dict[str, Any] = {}

    for path_parts, author in processed_results:
        current_level = author_map
        for part in path_parts[:-1]:
            current_level = current_level.setdefault(part, {})
        current_level[path_parts[-1]] = author

    if author_map:
        log.info(
            f"Trích xuất được thông tin từ {len(processed_results)} file. Đang chuẩn bị ghi file..."
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
