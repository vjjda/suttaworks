# Path: src/db_updater/post_tasks/suttaplex_json_task.py
import json
import logging
from pathlib import Path
from typing import Dict, Set

from src.config import constants

log = logging.getLogger(__name__)


def run(task_config: Dict):

    project_root = constants.PROJECT_ROOT

    input_module_name = task_config.get("input_module")
    if not input_module_name:
        log.error("Tác vụ 'suttaplex-json' yêu cầu 'input_module' trong cấu hình.")
        return

    input_dir = constants.RAW_DATA_PATH / input_module_name
    process_suttaplex_json(task_config, project_root, input_dir)


def _process_group(
    group_name: str, base_dir: Path, target_dict: Dict, existing_keys: Set[str] = None
):
    group_dir = base_dir / group_name
    if not group_dir.is_dir():
        log.warning(f"Thư mục nhóm '{group_name}' không tồn tại, bỏ qua.")
        return

    log.debug(f"Đang quét nhóm: {group_name}")
    for file_path in group_dir.glob("*.json"):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, list):
                    log.warning(
                        f"File {file_path.name} không chứa một danh sách, bỏ qua."
                    )
                    continue

                for item in data:
                    if not isinstance(item, dict) or "uid" not in item:
                        log.warning(f"Mục trong {file_path.name} thiếu 'uid', bỏ qua.")
                        continue

                    uid = item["uid"]

                    if uid is None or uid == "null":
                        log.debug(f"Phát hiện mục có uid không hợp lệ ({uid}), bỏ qua.")
                        continue

                    if existing_keys and uid in existing_keys:
                        continue

                    value = item.copy()
                    del value["uid"]
                    target_dict[uid] = value

        except (json.JSONDecodeError, IOError) as e:
            log.warning(f"Lỗi khi đọc file {file_path.name}, bỏ qua. Lỗi: {e}")


def process_suttaplex_json(config: Dict, project_root: Path, input_dir: Path):
    try:
        output_file = project_root / config["output"]
        priority_groups = config.get("priority", [])
        super_tree_groups = config.get("super-tree", [])
    except KeyError as e:
        log.error(f"Thiếu key bắt buộc trong cấu hình 'suttaplex-json': {e}")
        return

    log.info("Bắt đầu xử lý suttaplex JSON với quy tắc priority và super-tree...")

    priority_data = {}
    log.info(f"Giai đoạn 1: Đang xử lý các nhóm ưu tiên: {priority_groups}")
    for group in priority_groups:
        _process_group(group, input_dir, priority_data)
    log.info(f"-> Tìm thấy {len(priority_data)} mục ưu tiên.")

    super_tree_only_data = {}
    priority_keys = set(priority_data.keys())
    log.info(f"Giai đoạn 2: Đang xử lý các nhóm super-tree: {super_tree_groups}")
    for group in super_tree_groups:
        _process_group(
            group, input_dir, super_tree_only_data, existing_keys=priority_keys
        )
    log.info(
        f"-> Tìm thấy {len(super_tree_only_data)} mục mới không trùng lặp từ super-tree."
    )

    final_data = {**super_tree_only_data, **priority_data}

    if final_data:
        log.info(f"Tổng hợp được {len(final_data)} mục. Ghi ra file: {output_file}")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(final_data, f, ensure_ascii=False, indent=2)
            log.info("✅ Hoàn tất xử lý và tạo file suttaplex.json.")
        except IOError as e:
            log.error(f"Không thể ghi file output: {e}")
    else:
        log.warning("Không có dữ liệu suttaplex nào được xử lý.")
