# Path: /src/db_updater/handlers/git_handler.py
import logging
import subprocess
import configparser
from pathlib import Path
import select

log = logging.getLogger(__name__)

def _run_command(command: list[str], cwd: Path):
    """
    Chạy một lệnh trong terminal và xử lý stdout/stderr đồng thời để hiển thị output
    theo thời gian thực một cách đáng tin cậy.
    """
    log.info(f"Đang chạy lệnh: {' '.join(command)}")
    try:
        with subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            cwd=cwd
        ) as proc:
            # Dùng select để theo dõi cả stdout và stderr
            while proc.poll() is None:
                # readable, _, _ = select.select([proc.stdout, proc.stderr], [], [])
                if proc.stdout and proc.stdout.readable():
                    line = proc.stdout.readline()
                    if line:
                        print(line.strip())
                        log.debug(line.strip())
                if proc.stderr and proc.stderr.readable():
                    line = proc.stderr.readline()
                    if line:
                        print(line.strip())
                        log.debug(line.strip())

            # Đọc nốt phần output còn lại sau khi tiến trình kết thúc
            if proc.stdout:
                for line in proc.stdout.readlines():
                    print(line.strip())
                    log.debug(line.strip())
            if proc.stderr:
                for line in proc.stderr.readlines():
                    print(line.strip())
                    log.debug(line.strip())

            if proc.returncode != 0:
                log.error(f"Lệnh thất bại với mã lỗi: {proc.returncode}")
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
    """
    Quy trình 2 giai đoạn:
    1. Thêm bất kỳ submodule mới nào được định nghĩa trong config.
    2. Cập nhật tất cả các submodule đã đăng ký.
    """
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
                command = ["git", "submodule", "add", "--force", "--progress", url, str(submodule_relative_path)]
                _run_command(command, cwd=project_root)

    if not has_new_submodules:
        log.info("Không có submodule mới nào để thêm.")

    log.info("Bắt đầu cập nhật tất cả các submodule đã đăng ký...")
    update_command = ["git", "submodule", "update", "--init", "--remote", "--progress", "--force"]
    _run_command(update_command, cwd=project_root)