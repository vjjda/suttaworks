# Path: src/db_updater/post_tasks/bilara_task.py
import logging
import json
from pathlib import Path
from typing import Dict, Any, List
from natsort import natsorted

from src.config import constants

log = logging.getLogger(__name__)


def run(task_config: Dict):
    process_bilara_data(task_config, constants.PROJECT_ROOT)


def _write_json_output(output_path: Path, data: Dict[str, Any], data_name: str):
    cleaned_data = {folder: files for folder, files in data.items() if files}

    if cleaned_data:

        sorted_data = {}
        for folder, files in cleaned_data.items():

            sorted_keys = natsorted(files.keys())

            sorted_files = {key: files[key] for key in sorted_keys}
            sorted_data[folder] = sorted_files

        total_files = sum(len(files) for files in sorted_data.values())
        log.info(
            f"Tìm thấy {total_files} file JSON cho nhóm '{data_name}'. Đang ghi ra file: {output_path}"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(output_path, "w", encoding="utf-8") as f:

                json.dump(sorted_data, f, ensure_ascii=False, indent=2)
            log.info(f"✅ Đã tạo file tổng hợp Bilara ({data_name}) thành công.")
        except IOError as e:
            log.error(f"Không thể ghi file JSON ({data_name}): {e}")
    else:
        log.warning(f"Không tìm thấy file JSON nào cho nhóm '{data_name}'.")


def process_bilara_data(config: Dict, project_root: Path):
    try:
        base_path = project_root / config["path"]
        folders_to_scan = config["folders"]
        output_config = config["output"]
        groups_config = config.get("groups", [])

    except KeyError as e:
        log.error(f"Thiếu key bắt buộc trong cấu hình 'bilara': {e}")
        return

    log.info(f"Bắt đầu quét dữ liệu Bilara từ: {base_path}")

    output_maps = {}

    output_maps["sutta"] = {folder: {} for folder in folders_to_scan}

    for group in groups_config:
        group_name = list(group.keys())[0]
        output_maps[group_name] = {folder: {} for folder in folders_to_scan}

    relative_base = base_path.parent.parent

    for folder in folders_to_scan:
        scan_dir = base_path / folder
        if not scan_dir.is_dir():
            log.warning(f"Thư mục không tồn tại, bỏ qua: {scan_dir}")
            continue

        log.debug(f"Đang quét trong {scan_dir}...")
        for json_file in scan_dir.glob("**/*.json"):
            file_key = json_file.stem
            relative_path = json_file.relative_to(relative_base)
            path_parts = set(relative_path.parts)

            matched = False

            for group in groups_config:
                group_name = list(group.keys())[0]
                keywords = set(list(group.values())[0])

                if not path_parts.isdisjoint(keywords):

                    output_maps[group_name][folder][file_key] = str(relative_path)
                    matched = True

                    break

            if not matched:
                output_maps["sutta"][folder][file_key] = str(relative_path)

    for group_name, data_map in output_maps.items():

        if group_name in output_config:
            output_file = project_root / output_config[group_name]
            _write_json_output(output_file, data_map, group_name.capitalize())
