# Path: src/db_updater/handlers/git_release/git_release_handler.py
import logging
import shutil
from pathlib import Path

from src.db_updater.handlers.base_handler import BaseHandler

from . import git_release_api, git_release_file, git_release_state

log = logging.getLogger(__name__)

__all__ = ["GitReleaseHandler"]


class GitReleaseHandler(BaseHandler):

    def __init__(self, handler_config: dict, destination_dir: Path):
        super().__init__(handler_config, destination_dir)
        self.headers = git_release_api.get_github_headers()

    def _determine_paths(
        self,
        base_dir: Path,
        asset_name: str,
        extract_policy: bool | str,
        extract_to_folder_policy: bool | str,
    ) -> tuple[Path, Path]:
        final_extract_dir = base_dir
        archive_path = base_dir / asset_name
        is_extracting_to_folder = extract_policy and extract_to_folder_policy

        if is_extracting_to_folder:
            if extract_to_folder_policy is True:
                folder_name = Path(asset_name).stem
                final_extract_dir = base_dir / folder_name
            else:
                final_extract_dir = base_dir / str(extract_to_folder_policy)

            if final_extract_dir.resolve() == archive_path.resolve():
                archive_path = base_dir / (asset_name + "._temp_download")
                log.warning(
                    f"Phát hiện xung đột tên: Sẽ giải nén vào '{final_extract_dir.name}'. "
                    f"Đang tải về file tạm: {archive_path.name}"
                )

        return archive_path, final_extract_dir

    def execute(self):
        log.info("Bắt đầu cập nhật dữ liệu từ GitHub Releases.")
        repo_configs = {
            k: v for k, v in self.handler_config.items() if k != "post_tasks"
        }

        for item_name, item_config in repo_configs.items():
            log.info(f"--- Bắt đầu xử lý module release: '{item_name}' ---")
            dest_path = self.destination_dir / item_name
            dest_path.mkdir(parents=True, exist_ok=True)

            repo_info = git_release_api.parse_repo_url(item_config["link"])
            if not repo_info:
                continue
            owner, repo = repo_info

            release_info = git_release_api.get_release_info(
                owner, repo, item_config["version"], self.headers
            )
            if not release_info:
                continue

            release_tag = release_info["tag_name"]
            remote_assets_map = {
                asset["name"]: asset for asset in release_info.get("assets", [])
            }

            normalized_assets = git_release_state.normalize_asset_config(
                item_config["assets"]
            )
            requested_assets_set = {asset["name"] for asset in normalized_assets}
            local_state = git_release_state.get_local_state(dest_path)

            if not git_release_state.check_if_sync_required(
                local_state,
                release_tag,
                requested_assets_set,
                item_config["version"],
            ):
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
                target_asset_info = remote_assets_map.get(asset_name)

                if not target_asset_info:
                    log.warning(
                        f"Không tìm thấy asset '{asset_name}' trong release '{release_tag}'."
                    )
                    continue

                try:

                    archive_path, final_extract_dir = self._determine_paths(
                        dest_path,
                        asset_name,
                        asset_config["extract"],
                        asset_config["extract_to_folder"],
                    )

                    git_release_file.download_file(
                        target_asset_info["browser_download_url"],
                        archive_path,
                        self.headers,
                    )

                    if asset_config["extract"] and asset_config["extract_to_folder"]:
                        final_extract_dir.mkdir(parents=True, exist_ok=True)

                    git_release_file.decompress_archive(
                        archive_path,
                        asset_name,
                        final_extract_dir,
                        force_extract=(asset_config["extract"] is True),
                        auto_extract=(asset_config["extract"] == "auto"),
                    )
                    downloaded_assets_this_run.append(asset_name)
                except Exception as e:
                    log.error(f"Lỗi khi xử lý asset '{asset_name}': {e}")

            if downloaded_assets_this_run:
                new_state = {
                    "tag": release_tag,
                    "assets": sorted(list(set(downloaded_assets_this_run))),
                }
                git_release_state.save_local_state(dest_path, new_state)
                log.info(f"🎉 Đồng bộ hóa thành công '{item_name}'.")
