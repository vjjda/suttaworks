# Path: /src/db_updater/handlers/git_handler.py
import logging
import subprocess
import configparser
from pathlib import Path

# --- THAY ĐỔI 1: Import processor mới ---
from src.db_updater.post_processors import bilara_processor

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
        
        if result.stdout:
            log.debug(f"STDOUT:\n{result.stdout.strip()}")
        if result.stderr:
            log.debug(f"STDERR:\n{result.stderr.strip()}")
        
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

def process_git_submodules(submodules_config: list, project_root: Path, base_dir: Path):
    base_dir.mkdir(parents=True, exist_ok=True)
    gitmodules_path = project_root / ".gitmodules"
    config = configparser.ConfigParser()
    if gitmodules_path.exists():
        config.read(gitmodules_path)

    has_new_submodules = False
    
    # --- THAY ĐỔI 2: Cập nhật logic đọc config và thêm submodule ---
    for item in submodules_config:
        # item có thể là dict đơn giản {'name': 'url'} hoặc phức tạp hơn
        # Lấy tên và url dựa trên key đầu tiên
        name = list(item.keys())[0]
        url = item[name]

        submodule_path = base_dir / name
        submodule_relative_path = Path(*submodule_path.parts[len(project_root.parts):])
        section_name = f'submodule "{submodule_relative_path}"'

        if section_name not in config:
            log.info(f"Phát hiện submodule mới '{name}'. Đang thêm...")
            has_new_submodules = True
            command = ["git", "submodule", "add", "--force", url, str(submodule_relative_path)]
            if not _run_command(command, cwd=project_root):
                log.error(f"Không thể thêm submodule '{name}'. Dừng xử lý.")
                return # Dừng lại nếu không thêm được submodule

    if not has_new_submodules:
        log.info("Không có submodule mới nào để thêm.")

    log.info("Bắt đầu cập nhật tất cả các submodule đã đăng ký...")
    update_command = ["git", "submodule", "update", "--init", "--remote", "--force"]
    
    # Chỉ chạy hậu xử lý nếu cập nhật thành công
    if _run_command(update_command, cwd=project_root):
        log.info("Cập nhật submodule hoàn tất. Bắt đầu giai đoạn hậu xử lý (post-processing)...")
        
        # --- THAY ĐỔI 3: Thêm logic hậu xử lý ---
        for item in submodules_config:
            submodule_name = list(item.keys())[0]
            
            # Xử lý cho 'bilara'
            if 'bilara' in item:
                log.info(f"🔎 Bắt đầu hậu xử lý 'bilara' cho submodule '{submodule_name}'...")
                bilara_config = item['bilara']
                bilara_processor.process_bilara_data(bilara_config, project_root)
            
            # Khung chờ cho 'html_text'
            if 'html_text' in item:
                log.info(f"🔎 Cấu hình 'html_text' được tìm thấy. Logic xử lý sẽ được thêm vào sau.")
                # html_text_processor.process_html_text_data(...)