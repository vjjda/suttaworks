# Path: /src/db_updater/handlers/git_handler.py
import logging
import subprocess
import configparser
from pathlib import Path

log = logging.getLogger(__name__)

def _run_command(command: list[str], cwd: Path):
    """
    Chạy một lệnh và xử lý output thời gian thực, với bufsize=1
    để yêu cầu line-buffering.
    """
    log.info(f"Đang chạy lệnh: {' '.join(command)}")
    try:
        # ---- THAY ĐỔI DUY NHẤT: Thêm bufsize=1 ----
        proc = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            cwd=cwd,
            bufsize=1 # Yêu cầu line-buffering
        )
        # --------------------------------------------

        # Phần code còn lại xử lý output giữ nguyên
        streams = {'stdout': proc.stdout, 'stderr': proc.stderr}
        output_buffers = {'stdout': '', 'stderr': ''}

        while proc.poll() is None:
            for stream_name, stream in streams.items():
                if stream:
                    char = stream.read(1)
                    if char:
                        output_buffers[stream_name] += char
                        if char in ['\n', '\r']:
                            print(output_buffers[stream_name].strip(), end='\r', flush=True)
                            log.debug(output_buffers[stream_name].strip())
                            output_buffers[stream_name] = ''
        
        print() 
        for buffer in output_buffers.values():
            if buffer.strip():
                log.debug(buffer.strip())

        if proc.returncode != 0:
            log.error(f"Lệnh thất bại với mã lỗi: {proc.returncode}")
            return False

    except Exception as e:
        log.exception(f"Một lỗi không mong muốn đã xảy ra: {e}")
        return False
    
    log.info("Lệnh đã thực thi thành công.")
    return True

# Hàm process_git_submodules không thay đổi...
def process_git_submodules(submodules_config: list, project_root: Path, base_dir: Path):
    # ... (code trong hàm này giữ nguyên)
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