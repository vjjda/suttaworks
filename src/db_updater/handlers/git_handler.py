# Path: src/db_updater/handlers/git_handler.py
import logging
import subprocess
import configparser
from pathlib import Path

from src.config import constants
from src.db_updater.handlers.base_handler import BaseHandler

log = logging.getLogger(__name__)


class GitHandler(BaseHandler):

    def __init__(self, handler_config: dict, destination_dir: Path):
        super().__init__(handler_config, destination_dir)
        self.project_root = constants.PROJECT_ROOT

    def _run_command(self, command: list[str], cwd: Path):
        log.info(f"Đang chạy lệnh: {' '.join(command)}...")
        log.info("(Tiến trình này có thể mất vài phút, vui lòng chờ...)")
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                cwd=cwd,
            )

            if result.stdout:
                log.debug(f"STDOUT:\n{result.stdout.strip()}")
            if result.stderr:
                log.debug(f"STDERR:\n{result.stderr.strip()}")

            if "nothing to commit, working tree clean" in result.stdout:
                log.info("Không có thay đổi nào để commit.")
                return True, ""

            if result.returncode != 0:
                log.error(f"Lệnh thất bại với mã lỗi: {result.returncode}")
                log.error(f"Thông báo lỗi:\n{result.stderr.strip()}")
                return False, result.stderr.strip()

            return True, result.stdout.strip()

        except FileNotFoundError:
            log.error(
                "Lỗi: Lệnh 'git' không được tìm thấy. Hãy chắc chắn Git đã được cài đặt và có trong PATH."
            )
            return False, "Git command not found"
        except Exception as e:
            log.exception(f"Một lỗi không mong muốn đã xảy ra: {e}")
            return False, str(e)

    def execute(self):
        log.info("Bắt đầu cập nhật dữ liệu Git Submodule.")
        self.destination_dir.mkdir(parents=True, exist_ok=True)

        gitmodules_path = self.project_root / ".gitmodules"
        config = configparser.ConfigParser()
        if gitmodules_path.exists():
            config.read(gitmodules_path)

        submodule_repos = {
            k: v for k, v in self.handler_config.items() if k != "post_tasks"
        }
        has_new_submodules = False

        for name, url in submodule_repos.items():
            submodule_path = self.destination_dir / name
            submodule_relative_path = Path(
                *submodule_path.parts[len(self.project_root.parts) :]
            )
            section_name = f'submodule "{submodule_relative_path}"'
            if section_name not in config:
                log.info(f"Phát hiện submodule mới '{name}'. Đang thêm...")
                has_new_submodules = True
                command = [
                    "git",
                    "submodule",
                    "add",
                    "--force",
                    url,
                    str(submodule_relative_path),
                ]
                success, _ = self._run_command(command, cwd=self.project_root)
                if not success:
                    raise RuntimeError(
                        f"Không thể thêm submodule '{name}'. Dừng xử lý."
                    )

        if not has_new_submodules:
            log.info("Không có submodule mới nào để thêm.")

        log.info("Bắt đầu cập nhật tất cả các submodule đã đăng ký...")
        update_command = ["git", "submodule", "update", "--init", "--remote", "--force"]

        success, _ = self._run_command(update_command, cwd=self.project_root)
        if not success:
            raise RuntimeError("Cập nhật submodule thất bại.")

        log.info("Kiểm tra trạng thái sau khi cập nhật để xác định các thay đổi...")
        status_command = ["git", "status", "--porcelain"]

        success, status_output = self._run_command(
            status_command, cwd=self.project_root
        )
        if not success:
            raise RuntimeError("Không thể chạy 'git status' để kiểm tra thay đổi.")

        paths_to_add = []
        if status_output:
            lines = status_output.strip().split("\n")
            for line in lines:

                if line.startswith(" M "):
                    path_str = line.strip().split(" ", 1)[1]

                    is_managed_submodule = any(
                        Path(path_str).is_relative_to(self.destination_dir / name)
                        for name in submodule_repos
                    )
                    if is_managed_submodule:
                        paths_to_add.append(path_str)

        if not paths_to_add:
            log.info("Không có submodule nào thực sự thay đổi. Không cần commit.")
        else:

            changed_submodule_names = sorted(
                list(set([Path(p).name for p in paths_to_add]))
            )
            log.info(
                f"Phát hiện thay đổi trong các submodule: {', '.join(changed_submodule_names)}"
            )
            log.info("Tự động commit các thay đổi...")

            commit_message = f"chore(data): Update data from submodules: {', '.join(changed_submodule_names)}"
            add_command = ["git", "add"] + paths_to_add
            commit_command = ["git", "commit", "-m", commit_message]

            add_success, _ = self._run_command(add_command, cwd=self.project_root)
            if add_success:
                self._run_command(commit_command, cwd=self.project_root)
            else:
                log.error("Lỗi khi thực hiện 'git add', bỏ qua bước commit.")