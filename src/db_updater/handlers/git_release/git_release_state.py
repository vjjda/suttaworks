# Path: src/db_updater/handlers/git_release/git_release_state.py
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Set

log = logging.getLogger(__name__)

__all__ = [
    "get_local_state",
    "save_local_state",
    "check_if_sync_required",
    "normalize_asset_config",
]

VERSION_FILE_NAME = "version.json"


def get_local_state(path: Path) -> Dict[str, Any]:
    default_state = {"tag": None, "assets": []}
    version_file = path / VERSION_FILE_NAME
    if not version_file.exists():
        return default_state

    try:
        with open(version_file, "r", encoding="utf-8") as f:
            state = json.load(f)
            if "tag" not in state or "assets" not in state:
                log.warning(
                    f"File {VERSION_FILE_NAME} không hợp lệ. Coi như trạng thái rỗng."
                )
                return default_state
            return state
    except (json.JSONDecodeError, TypeError):
        log.warning(f"Lỗi đọc file {VERSION_FILE_NAME}. Coi như trạng thái rỗng.")
        return default_state


def save_local_state(path: Path, state: Dict[str, Any]):
    version_file = path / VERSION_FILE_NAME
    with open(version_file, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def normalize_asset_config(
    asset_config_list: List[Any],
) -> List[Dict[str, Any]]:
    normalized = []
    for asset_item in asset_config_list:
        if isinstance(asset_item, str):
            normalized.append(
                {
                    "name": asset_item,
                    "extract": "auto",
                    "extract_to_folder": False,
                }
            )
        elif isinstance(asset_item, dict) and "name" in asset_item:
            normalized.append(
                {
                    "name": asset_item["name"],
                    "extract": asset_item.get("extract", "auto"),
                    "extract_to_folder": asset_item.get("extract_to_folder", False),
                }
            )
    return normalized


def check_if_sync_required(
    local_state: Dict[str, Any],
    remote_tag: str,
    requested_assets_set: Set[str],
    requested_version: str,
) -> bool:
    local_assets_set = set(local_state.get("assets", []))

    is_version_mismatch = (
        requested_version == "latest" and remote_tag != local_state.get("tag")
    ) or (requested_version != "latest" and requested_version != local_state.get("tag"))

    if is_version_mismatch:
        log.info(
            f"Phiên bản không khớp (local: {local_state.get('tag')}, remote: {remote_tag}). Cần đồng bộ lại."
        )
        return True
    elif requested_assets_set != local_assets_set:
        log.info("Danh sách assets trong config đã thay đổi. Cần đồng bộ lại.")
        return True

    return False
