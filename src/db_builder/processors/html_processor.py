# Path: src/db_builder/processors/html_processor.py
import json
import logging
from pathlib import Path
from typing import Dict, List, Set, Any
from collections import defaultdict

logger = logging.getLogger(__name__)


class HtmlFileProcessor:

    def __init__(
        self, manifest_path: Path, authors_map: Dict, known_translation_uids: Set
    ):
        self.manifest_path = manifest_path
        self.authors_map = authors_map
        self.known_translation_ids = known_translation_uids
        self.filepath_map: Dict[str, str] = {}

        self.author_name_to_uids = defaultdict(list)
        for uid, data in self.authors_map.items():
            if data.get("author_name"):
                self.author_name_to_uids[data["author_name"]].append(uid)

        self.author_short_to_uids = defaultdict(list)
        for uid, data in self.authors_map.items():
            if data.get("author_short"):
                self.author_short_to_uids[data["author_short"].lower()].append(uid)

    def execute(self) -> Dict[str, str]:
        if not self.manifest_path.exists():
            logger.error(f"Không tìm thấy file manifest HTML: {self.manifest_path}")
            return {}

        logger.info(f"Bắt đầu xử lý manifest HTML từ: {self.manifest_path.name}")
        with open(self.manifest_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self._recursive_parse(data, [])

        logger.info(f"Đã tạo map cho {len(self.filepath_map)} file HTML từ manifest.")
        return self.filepath_map

    def _recursive_parse(self, node: Dict[str, Any], path_parts: List[str]):
        for key, value in node.items():
            current_path_parts = path_parts + [key]

            if isinstance(value, str):
                filename = key
                author_name_from_meta = value
                full_relative_path = "/".join(current_path_parts)
                self._process_file_entry(
                    filename,
                    author_name_from_meta,
                    current_path_parts,
                    full_relative_path,
                )

            elif isinstance(value, dict):
                self._recursive_parse(value, current_path_parts)

    def _process_file_entry(
        self, filename: str, author_name: str, path_parts: List[str], full_path: str
    ):
        try:
            sutta_uid = Path(filename).stem

            if sutta_uid == "sf36":
                translation_id = "sf36_root"
                if translation_id in self.known_translation_ids:
                    self.filepath_map[translation_id] = full_path
                else:
                    logger.warning(
                        f"Quy tắc đặc biệt cho '{filename}': không tìm thấy translation_id '{translation_id}'."
                    )
                return

            if len(path_parts) < 3:
                return
            lang = path_parts[2]

            potential_uids = self.author_name_to_uids.get(author_name, [])
            if not potential_uids:
                potential_uids = self.author_short_to_uids.get(author_name.lower(), [])

            if not potential_uids:
                if len(path_parts) > 2:
                    parent_dir_name = path_parts[-2]
                    if parent_dir_name in self.authors_map:
                        potential_uids = [parent_dir_name]

            if not potential_uids:
                logger.warning(
                    f"Không tìm thấy author_uid nào cho '{author_name}' trong file {full_path}"
                )
                return

            found_match = False
            for author_uid_candidate in potential_uids:
                if author_uid_candidate == "taisho":
                    standard_id = f"{lang}_{sutta_uid}_taisho"
                    if standard_id in self.known_translation_ids:
                        self.filepath_map[standard_id] = full_path
                        found_match = True
                        break
                    special_id = f"{sutta_uid}_root-lzh-sct"
                    if special_id in self.known_translation_ids:
                        self.filepath_map[special_id] = full_path
                        found_match = True
                        break
                else:
                    standard_id = f"{lang}_{sutta_uid}_{author_uid_candidate}"
                    if standard_id in self.known_translation_ids:
                        self.filepath_map[standard_id] = full_path
                        found_match = True
                        break

            if not found_match:
                logger.debug(
                    f"Đã tạo các ID ứng viên từ author '{author_name}' của file {full_path} nhưng không khớp."
                )

        except Exception as e:
            logger.error(f"Lỗi khi xử lý mục {full_path} từ manifest: {e}")
