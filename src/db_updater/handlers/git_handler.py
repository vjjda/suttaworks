# Path: /src/db_updater/handlers/git_handler.py
import logging
import subprocess
import configparser
from pathlib import Path

log = logging.getLogger(__name__)

def _run_command(command: list[str], cwd: Path):
    """
    Chạy một lệnh một cách đơn giản, đợi nó hoàn thành,
    và ghi lại output vào log file.
    """
    log.info(f"Đang chạy lệnh: {' '.join(command)}...")
    log.info("(Tiến trình này có thể mất vài phút, vui lòng chờ...)")
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            cwd=cwd
        )
        
        # Ghi lại toàn bộ output vào log ở mức DEBUG để không làm nhiễu console
        if result.stdout:
            log.debug(f"STDOUT:\n{result.stdout.strip()}")
        if result.stderr:
            log.debug(f"STDERR:\n{result.stderr.strip()}")
        
        # Nếu có lỗi, báo lỗi ra console
        if result.returncode != 0:
            log.error(f"Lệnh thất bại với mã lỗi: {result.returncode}")
            log.error(f"Thông báo lỗi:\n{result.stderr.strip()}")
            return False

    except FileNotFoundError:
        log.error("Lỗi: Lệnh 'git' không được tìm thấy. Hãy chắc chắn Git đã được cài đặt và có trong PATH.")
        return False
    except Exception as e:
        log.exception(f"Một lỗi không mong muốn đã xảy ra: {e}")
        return False
    
    log.info("Lệnh đã thực thi thành công.")
    return True

# Hàm process_git_submodules không thay đổi
def process_git_submodules(submodules_config: list, project_root: Path, base_dir: Path):
    base_dir.mkdir(parents=True, exist_ok=True)
    gitmodules_path = project_root / ".gitmodules"
    config = configparser.ConfigParser()
    if gitmodules_path.exists():
        config.read(gitmodules_path)

    has_new_submodules = False
    for item in submodules_config:
        for name, url in item.items():
            submodule_path = base_dir / name
            submodule_relative_path = Path(*submodule_path.parts[len(project_root.parts):])
            section_name = f'submodule "{submodule_relative_path}"'

            if section_name not in config:
                log.info(f"Phát hiện submodule mới '{name}'. Đang thêm...")
                has_new_submodules = True
                command = ["git", "submodule", "add", "--force", url, str(submodule_relative_path)]
                _run_command(command, cwd=project_root)

    if not has_new_submodules:
        log.info("Không có submodule mới nào để thêm.")

    log.info("Bắt đầu cập nhật tất cả các submodule đã đăng ký...")
    update_command = ["git", "submodule", "update", "--init", "--remote", "--force"]
    _run_command(update_command, cwd=project_root)