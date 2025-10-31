# Path: src/db_updater/handlers/git_release_handler.py
import json
import logging
import os
import re
import shutil
import tarfile
import zipfile
from pathlib import Path

import requests
from dotenv import load_dotenv
from tqdm import tqdm

log = logging.getLogger(__name__)


def _get_github_headers() -> dict | None:
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN")
    if token:
        log.info("Đã tìm thấy GITHUB_TOKEN. Sử dụng request đã xác thực.")
        return {"Authorization": f"token {token}"}
    log.info("Không tìm thấy GITHUB_TOKEN. Sử dụng request ẩn danh.")
    return None


def _parse_repo_url(url: str) -> tuple[str, str] | None:
    match = re.search(r"github\.com/([^/]+)/([^/]+)", url)
    if match:
        return match.groups()
    return None


def _get_release_info(
    owner: str, repo: str, version: str, headers: dict | None
) -> dict | None:
    if version == "latest":
        url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
    else:
        url = f"https://api.github.com/repos/{owner}/{repo}/releases/tags/{version}"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        log.error(f"Lỗi khi gọi GitHub API cho repo '{owner}/{repo}': {e}")
        return None


def _get_local_state(path: Path) -> dict:
    default_state = {"tag": None, "assets": []}
    version_file = path / ".version"
    if not version_file.exists():
        return default_state

    try:
        with open(version_file, "r", encoding="utf-8") as f:
            state = json.load(f)
            if "tag" not in state or "assets" not in state:
                log.warning("File .version không hợp lệ. Coi như trạng thái rỗng.")
                return default_state
            return state
    except (json.JSONDecodeError, TypeError):
        log.warning("Lỗi đọc file .version. Coi như trạng thái rỗng.")
        return default_state


def _save_local_state(path: Path, state: dict):
    version_file = path / ".version"
    with open(version_file, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def _download_file(url: str, dest_path: Path, headers: dict | None):
    log.info(f"Đang tải về: {url}")
    try:
        with requests.get(url, stream=True, headers=headers) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))
            with open(dest_path, "wb") as f, tqdm(
                total=total_size, unit="iB", unit_scale=True, desc=dest_path.name
            ) as bar:
                for chunk in r.iter_content(chunk_size=8192):
                    size = f.write(chunk)
                    bar.update(size)
    except requests.exceptions.RequestException as e:
        log.error(f"Tải file thất bại: {e}")
        raise


def _decompress_archive(
    archive_path: Path, extract_dir: Path, force_extract: bool, auto_extract: bool
):
    file_name = archive_path.name
    should_delete_archive = True

    log.info(f"Đang xử lý file: {file_name}")

    if file_name.endswith((".zip", ".epub")):
        if force_extract or (auto_extract and file_name.endswith(".zip")):
            log.info(f"Đang giải nén (zip/epub): {file_name}")
            with zipfile.ZipFile(archive_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)
        else:
            should_delete_archive = False
    elif file_name.endswith((".tar.gz", ".tgz")):
        if force_extract or auto_extract:
            log.info(f"Đang giải nén (tar.gz): {file_name}")
            with tarfile.open(archive_path, "r:gz") as tar:
                tar.extractall(path=extract_dir)
        else:
            should_delete_archive = False
    elif file_name.endswith(".tar.bz2"):
        if force_extract or auto_extract:
            log.info(f"Đang giải nén (tar.bz2): {file_name}")
            with tarfile.open(archive_path, "r:bz2") as tar:
                tar.extractall(path=extract_dir)
        else:
            should_delete_archive = False
    else:
        log.info(
            f"Giữ nguyên file (không phải định dạng nén được hỗ trợ tự động): {file_name}"
        )
        should_delete_archive = False

    if should_delete_archive:
        os.remove(archive_path)
        log.info("Giải nén hoàn tất và đã xóa file nén.")


def process_git_release_data(
    handler_config: dict,
    base_dest_dir: Path,
    run_update: bool = True,
    run_post_process: bool = True,
    tasks_to_run: list[str] | None = None,
):

    if run_update:
        log.info("=== GIAI ĐOẠN: CẬP NHẬT DỮ LIỆU TỪ GITHUB RELEASES ===")
        headers = _get_github_headers()

        repo_configs = {k: v for k, v in handler_config.items() if k != "post_tasks"}

        for item_name, item_config in repo_configs.items():

            log.info(f"--- Bắt đầu xử lý module release: '{item_name}' ---")
            dest_path = base_dest_dir / item_name
            dest_path.mkdir(parents=True, exist_ok=True)

            repo_info = _parse_repo_url(item_config["link"])
            if not repo_info:
                log.error(f"URL repo không hợp lệ: {item_config['link']}")
                continue
            owner, repo = repo_info

            release_info = _get_release_info(
                owner, repo, item_config["version"], headers
            )
            if not release_info:
                continue

            release_tag = release_info["tag_name"]

            normalized_assets = []
            for asset_item in item_config["assets"]:
                if isinstance(asset_item, str):
                    normalized_assets.append(
                        {
                            "name": asset_item,
                            "extract": "auto",
                            "extract_to_folder": False,
                        }
                    )
                elif isinstance(asset_item, dict) and "name" in asset_item:
                    normalized_assets.append(
                        {
                            "name": asset_item["name"],
                            "extract": asset_item.get("extract", "auto"),
                            "extract_to_folder": asset_item.get(
                                "extract_to_folder", False
                            ),
                        }
                    )

            requested_assets_names = [asset["name"] for asset in normalized_assets]
            requested_assets_set = set(requested_assets_names)

            local_state = _get_local_state(dest_path)
            local_assets_set = set(local_state["assets"])

            should_resync = False
            is_version_mismatch = (
                item_config["version"] == "latest" and release_tag != local_state["tag"]
            ) or (
                item_config["version"] != "latest"
                and item_config["version"] != local_state["tag"]
            )

            if is_version_mismatch:
                log.info(f"Phiên bản không khớp. Cần đồng bộ lại.")
                should_resync = True
            elif requested_assets_set != local_assets_set:
                log.info(f"Danh sách assets đã thay đổi. Cần đồng bộ lại.")
                should_resync = True

            if not should_resync:
                log.info(
                    f"✅ Phiên bản '{release_tag}' và các assets đã được cập nhật đầy đủ."
                )
                continue

            log.info("Thực hiện đồng bộ hóa: Dọn dẹp và tải lại.")
            shutil.rmtree(dest_path)
            dest_path.mkdir()

            downloaded_assets_this_run = []
            for asset_config in normalized_assets:
                asset_name = asset_config["name"]
                target_asset_info = next(
                    (
                        asset
                        for asset in release_info["assets"]
                        if asset["name"] == asset_name
                    ),
                    None,
                )

                if not target_asset_info:
                    log.warning(
                        f"Không tìm thấy asset '{asset_name}' trong release '{release_tag}'."
                    )
                    continue

                try:
                    download_url = target_asset_info["browser_download_url"]
                    archive_path = dest_path / asset_name
                    _download_file(download_url, archive_path, headers)

                    extract_policy = asset_config["extract"]
                    extract_to_folder_policy = asset_config["extract_to_folder"]
                    final_extract_dir = dest_path
                    if extract_policy and extract_to_folder_policy:
                        final_extract_dir = dest_path / (
                            asset_name
                            if extract_to_folder_policy is True
                            else extract_to_folder_policy
                        )
                        final_extract_dir.mkdir(parents=True, exist_ok=True)

                    _decompress_archive(
                        archive_path,
                        final_extract_dir,
                        extract_policy is True,
                        extract_policy == "auto",
                    )
                    downloaded_assets_this_run.append(asset_name)
                except Exception as e:
                    log.error(f"Lỗi khi xử lý asset '{asset_name}': {e}")

            if downloaded_assets_this_run:
                new_state = {
                    "tag": release_tag,
                    "assets": sorted(list(set(downloaded_assets_this_run))),
                }
                _save_local_state(dest_path, new_state)
                log.info(f"🎉 Đồng bộ hóa thành công '{item_name}'.")

    else:
        log.info("Bỏ qua giai đoạn cập nhật dữ liệu GitHub Releases theo yêu cầu.")

    if run_post_process:
        log.info("=== GIAI ĐOẠN: HẬU XỬ LÝ (POST-PROCESSING) ===")
        if "post_tasks" in handler_config:
            for task_name in handler_config["post_tasks"]:
                if tasks_to_run is None or task_name in tasks_to_run:
                    log.warning(f"--> Tác vụ '{task_name}' là placeholder, bỏ qua.")
                else:
                    log.info(f"--> Bỏ qua tác vụ '{task_name}' theo yêu cầu.")
    else:
        log.info("Bỏ qua giai đoạn hậu xử lý theo yêu cầu.")