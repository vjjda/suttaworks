# Path: /src/db_updater/handlers/git_handler.py
import logging
import subprocess
import configparser
from pathlib import Path

# --- THAY ĐỔI 1: Import tất cả các processor ---
from src.db_updater.post_processors import (
    bilara_processor, 
    html_text_authors_processor,
    cips_processor
)

log = logging.getLogger(__name__)

# Hàm _run_command không thay đổi...
def _run_command(command: list[str], cwd: Path):
    # ... (giữ nguyên)
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
    # Phần thêm và cập nhật submodule không thay đổi...
    base_dir.mkdir(parents=True, exist_ok=True)
    gitmodules_path = project_root / ".gitmodules"
    config = configparser.ConfigParser()
    if gitmodules_path.exists():
        config.read(gitmodules_path)

    has_new_submodules = False
    for item in submodules_config:
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
                return

    if not has_new_submodules:
        log.info("Không có submodule mới nào để thêm.")

    log.info("Bắt đầu cập nhật tất cả các submodule đã đăng ký...")
    update_command = ["git", "submodule", "update", "--init", "--remote", "--force"]
    
    if _run_command(update_command, cwd=project_root):
        log.info("Cập nhật submodule hoàn tất. Bắt đầu giai đoạn hậu xử lý (post-processing)...")
        
        # --- THAY ĐỔI 2: Cập nhật logic hậu xử lý để đọc key 'post' ---
        for item in submodules_config:
            if 'post' in item and isinstance(item['post'], dict):
                submodule_name = list(item.keys())[0]
                log.info(f"🔎 Tìm thấy các tác vụ hậu xử lý cho submodule '{submodule_name}':")
                
                post_tasks = item['post']
                for task_name, task_config in post_tasks.items():
                    log.info(f"  -> Bắt đầu tác vụ: '{task_name}'...")
                    
                    if task_name == "bilara":
                        bilara_processor.process_bilara_data(task_config, project_root)
                    elif task_name == "html_text":
                        html_text_authors_processor.process_html_text_authors_data(task_config, project_root)
                    elif task_name == "cips-json":
                        cips_processor.process_cips_csv_to_json(task_config, project_root)
                    else:
                        log.warning(f"  -> Tác vụ không được hỗ trợ: '{task_name}'. Bỏ qua.")