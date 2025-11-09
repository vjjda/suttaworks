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

from src.db_updater.handlers.base_handler import BaseHandler

log = logging.getLogger(__name__)


class GitReleaseHandler(BaseHandler):

    def __init__(self, handler_config: dict, destination_dir: Path):
        super().__init__(handler_config, destination_dir)
        self.headers = self._get_github_headers()

    def _get_github_headers(self) -> dict | None:
        load_dotenv()
        token = os.getenv("GITHUB_TOKEN")
        if token:
            log.info("Đã tìm thấy GITHUB_TOKEN. Sử dụng request đã xác thực.")
            return {"Authorization": f"token {token}"}
        log.info("Không tìm thấy GITHUB_TOKEN. Sử dụng request ẩn danh.")
        return None

    def _parse_repo_url(self, url: str) -> tuple[str, str] | None:
        match = re.search(r"github\.com/([^/]+)/([^/]+)", url)
        if match:
            groups = match.groups()

            if len(groups) == 2:
                return (groups[0], groups[1])
        return None

    def _get_release_info(self, owner: str, repo: str, version: str) -> dict | None:
        if version == "latest":
            url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
        else:
            url = f"https://api.github.com/repos/{owner}/{repo}/releases/tags/{version}"

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            log.error(f"Lỗi khi gọi GitHub API cho repo '{owner}/{repo}': {e}")
            return None

    def _get_local_state(self, path: Path) -> dict:
        default_state = {"tag": None, "assets": []}

        version_file = path / "version.json"
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

    def _save_local_state(self, path: Path, state: dict):

        version_file = path / "version.json"
        with open(version_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

    def _download_file(self, url: str, dest_path: Path):
        log.info(f"Đang tải về: {url}")
        try:
            with requests.get(url, stream=True, headers=self.headers) as r:
                r.raise_for_status()
                total_size = int(r.headers.get("content-length", 0))
                with (
                    open(dest_path, "wb") as f,
                    tqdm(
                        total=total_size,
                        unit="iB",
                        unit_scale=True,
                        desc=dest_path.name,
                    ) as bar,
                ):
                    for chunk in r.iter_content(chunk_size=8192):
                        size = f.write(chunk)
                        bar.update(size)
        except requests.exceptions.RequestException as e:
            log.error(f"Tải file thất bại: {e}")
            raise

    def _decompress_archive(
        self,
        archive_path: Path,
        original_asset_name: str,
        extract_dir: Path,
        force_extract: bool,
        auto_extract: bool,
    ):
        file_name_on_disk = archive_path.name
        should_delete_archive = True

        log.info(f"Đang xử lý file: {file_name_on_disk}")

        if force_extract:
            log.info(f"Ép buộc giải nén (zip): {file_name_on_disk} -> {extract_dir}")
            try:
                with zipfile.ZipFile(archive_path, "r") as zip_ref:
                    zip_ref.extractall(extract_dir)
            except zipfile.BadZipFile:
                log.error(
                    f"Lỗi: Đã ép buộc giải nén nhưng '{file_name_on_disk}' (tên gốc: {original_asset_name}) không phải file zip hợp lệ."
                )
                should_delete_archive = False
            except Exception as e:
                log.error(f"Lỗi khi ép buộc giải nén zip: {e}")
                should_delete_archive = False

        elif auto_extract:
            if original_asset_name.endswith(".zip"):
                log.info(
                    f"Tự động giải nén (zip): {file_name_on_disk} -> {extract_dir}"
                )
                with zipfile.ZipFile(archive_path, "r") as zip_ref:
                    zip_ref.extractall(extract_dir)

            elif original_asset_name.endswith((".tar.gz", ".tgz")):
                log.info(
                    f"Tự động giải nén (tar.gz): {file_name_on_disk} -> {extract_dir}"
                )
                with tarfile.open(archive_path, "r:gz") as tar:
                    tar.extractall(path=extract_dir)

            elif original_asset_name.endswith(".tar.bz2"):
                log.info(
                    f"Tự động giải nén (tar.bz2): {file_name_on_disk} -> {extract_dir}"
                )
                with tarfile.open(archive_path, "r:bz2") as tar:
                    tar.extractall(path=extract_dir)

            else:

                log.info(
                    f"Giữ nguyên file (auto-extract, không khớp loại): {file_name_on_disk}"
                )
                should_delete_archive = False

        else:
            log.info(f"Giữ nguyên file (không giải nén): {file_name_on_disk}")
            should_delete_archive = False

        if should_delete_archive:
            os.remove(archive_path)
            log.info(f"Giải nén hoàn tất và đã xóa file nén: {file_name_on_disk}")

    def execute(self):
        log.info("Bắt đầu cập nhật dữ liệu từ GitHub Releases.")
        repo_configs = {
            k: v for k, v in self.handler_config.items() if k != "post_tasks"
        }

        for item_name, item_config in repo_configs.items():
            log.info(f"--- Bắt đầu xử lý module release: '{item_name}' ---")
            dest_path = self.destination_dir / item_name
            dest_path.mkdir(parents=True, exist_ok=True)

            repo_info = self._parse_repo_url(item_config["link"])
            if not repo_info:
                log.error(f"URL repo không hợp lệ: {item_config['link']}")
                continue
            owner, repo = repo_info

            release_info = self._get_release_info(owner, repo, item_config["version"])
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

            local_state = self._get_local_state(dest_path)
            local_assets_set = set(local_state["assets"])

            should_resync = False
            is_version_mismatch = (
                item_config["version"] == "latest" and release_tag != local_state["tag"]
            ) or (
                item_config["version"] != "latest"
                and item_config["version"] != local_state["tag"]
            )

            if is_version_mismatch:
                log.info("Phiên bản không khớp. Cần đồng bộ lại.")
                should_resync = True
            elif requested_assets_set != local_assets_set:
                log.info("Danh sách assets đã thay đổi. Cần đồng bộ lại.")
                should_resync = True

            if not should_resync:
                log.info(
                    f"✅ Phiênbản '{release_tag}' và các assets đã được cập nhật đầy đủ."
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

                    extract_policy = asset_config["extract"]
                    extract_to_folder_policy = asset_config["extract_to_folder"]
                    final_extract_dir = dest_path

                    is_extracting_to_folder = (
                        extract_policy and extract_to_folder_policy
                    )

                    if is_extracting_to_folder:

                        if extract_to_folder_policy is True:
                            folder_name = Path(asset_name).stem
                            final_extract_dir = dest_path / folder_name
                        else:
                            final_extract_dir = dest_path / str(
                                extract_to_folder_policy
                            )

                    archive_path = dest_path / asset_name

                    if (
                        is_extracting_to_folder
                        and final_extract_dir.resolve() == archive_path.resolve()
                    ):
                        archive_path = dest_path / (asset_name + "._temp_download")
                        log.warning(
                            f"Phát hiện xung đột tên: Sẽ giải nén vào '{final_extract_dir.name}'. "
                            f"Đang tải về file tạm: {archive_path.name}"
                        )

                    self._download_file(download_url, archive_path)

                    if is_extracting_to_folder:
                        final_extract_dir.mkdir(parents=True, exist_ok=True)

                    self._decompress_archive(
                        archive_path,
                        asset_name,
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
                self._save_local_state(dest_path, new_state)
                log.info(f"🎉 Đồng bộ hóa thành công '{item_name}'.")
