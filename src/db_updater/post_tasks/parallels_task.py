# Path: src/db_updater/post_tasks/parallels_task.py
import json
import logging
from pathlib import Path
from itertools import combinations
from collections import defaultdict
from natsort import natsorted
from typing import Any

from src.config import constants
from .parallels import transformer, utils

log = logging.getLogger(__name__)


def run(task_config: dict):
    """Adapter function to be called by the BaseHandler."""
    process_parallels_data(task_config, constants.PROJECT_ROOT)


RELATION_ORDER = ["parallels", "resembles", "mentions", "retells"]


def _sort_data_naturally(data: Any) -> Any:
    if isinstance(data, (dict, defaultdict)):
        sorted_keys = natsorted(data.keys())

        if all(key in RELATION_ORDER for key in sorted_keys):
            sorted_keys.sort(
                key=lambda k: (
                    RELATION_ORDER.index(k)
                    if k in RELATION_ORDER
                    else len(RELATION_ORDER)
                )
            )

        return {key: _sort_data_naturally(data[key]) for key in sorted_keys}

    if isinstance(data, list):

        unique_items = list(dict.fromkeys(data))
        try:
            return natsorted(unique_items)
        except TypeError:
            return [_sort_data_naturally(item) for item in unique_items]

    return data


def _build_initial_map(data: list) -> defaultdict:
    sutta_map = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for group in data:
        relation_type = list(group.keys())[0]
        id_list = group[relation_type]

        full_list = [i for i in id_list if not i.startswith("~")]
        resembling_list = [i for i in id_list if i.startswith("~")]

        if relation_type == "parallels":
            for source, target in combinations(full_list, 2):
                base_s = utils.parse_sutta_id(source)
                base_t = utils.parse_sutta_id(target)
                sutta_map[base_s]["parallels"][source].append(target)
                sutta_map[base_t]["parallels"][target].append(source)
            if full_list and resembling_list:
                for source in full_list:
                    base_s = utils.parse_sutta_id(source)
                    for target in resembling_list:
                        cleaned_t = target.lstrip("~")
                        base_t = utils.parse_sutta_id(cleaned_t)
                        sutta_map[base_s]["resembles"][source].append(cleaned_t)
                        sutta_map[base_t]["resembles"][cleaned_t].append(source)
        elif relation_type in ["mentions", "retells"]:

            for source, target in combinations(full_list, 2):
                base_s = utils.parse_sutta_id(source)
                base_t = utils.parse_sutta_id(target)
                sutta_map[base_s][relation_type][source].append(target)
                sutta_map[base_t][relation_type][target].append(source)
            if full_list and resembling_list:
                for source in full_list:
                    base_s = utils.parse_sutta_id(source)
                    for target in resembling_list:
                        cleaned_t = target.lstrip("~")
                        base_t = utils.parse_sutta_id(cleaned_t)
                        sutta_map[base_s][relation_type][source].append(cleaned_t)
                        sutta_map[base_t][relation_type][cleaned_t].append(source)
    return sutta_map


def process_parallels_data(task_config: dict, project_root: Path):
    try:
        input_path = project_root / task_config["path"]
        output_config = task_config.get("output", {})
        replacements = task_config.get("replacements", [])

        paths = {
            "category": (
                project_root / output_config["category"]
                if "category" in output_config
                else None
            ),
            "segment": (
                project_root / output_config["segment"]
                if "segment" in output_config
                else None
            ),
            "flat": (
                project_root / output_config["flat_segment"]
                if "flat_segment" in output_config
                else None
            ),
            "book": (
                project_root / output_config["book"]
                if "book" in output_config
                else None
            ),
        }

        if not any(paths.values()):
            log.error("Lỗi cấu hình 'parallels': không có đường dẫn output hợp lệ.")
            return

        log.info(f"Bắt đầu xử lý file parallels: {input_path}")
        if not input_path.exists():
            log.error(f"Không tìm thấy file input: {input_path}")
            return

        with open(input_path, "r", encoding="utf-8") as f:
            raw_content = f.read()

        if replacements:
            log.info(f"Thực hiện {len(replacements)} thay thế văn bản từ config...")
            for find, replace in replacements:
                raw_content = raw_content.replace(find, replace)

        data = json.loads(raw_content)

        sutta_map = _build_initial_map(data)

        category_data, segment_data = None, None

        if paths["category"]:
            category_data = _sort_data_naturally(sutta_map)
            paths["category"].parent.mkdir(parents=True, exist_ok=True)
            with open(paths["category"], "w", encoding="utf-8") as f:
                json.dump(category_data, f, ensure_ascii=False, indent=2)
            log.info(f"✅ Đã lưu file Category vào: {paths['category']}")

        if any([paths["segment"], paths["flat"], paths["book"]]):
            segment_map = transformer.invert_to_segment_structure(sutta_map)
            segment_data = _sort_data_naturally(segment_map)
            if paths["segment"]:
                paths["segment"].parent.mkdir(parents=True, exist_ok=True)
                with open(paths["segment"], "w", encoding="utf-8") as f:
                    json.dump(segment_data, f, ensure_ascii=False, indent=2)
                log.info(f"✅ Đã lưu file Segment vào: {paths['segment']}")

        if paths["flat"]:
            flat_map = transformer.flatten_segment_map(segment_data)
            flat_data = _sort_data_naturally(flat_map)
            paths["flat"].parent.mkdir(parents=True, exist_ok=True)
            with open(paths["flat"], "w", encoding="utf-8") as f:
                json.dump(flat_data, f, ensure_ascii=False, indent=2)
            log.info(f"✅ Đã lưu file Flat Segment vào: {paths['flat']}")

        if paths["book"]:
            book_map = transformer.create_book_structure(segment_data)
            book_data = _sort_data_naturally(book_map)
            paths["book"].parent.mkdir(parents=True, exist_ok=True)
            with open(paths["book"], "w", encoding="utf-8") as f:
                json.dump(book_data, f, ensure_ascii=False, indent=2)
            log.info(f"✅ Đã lưu file Book vào: {paths['book']}")

    except Exception as e:
        log.exception(f"Đã xảy ra lỗi không mong muốn khi xử lý parallels: {e}")