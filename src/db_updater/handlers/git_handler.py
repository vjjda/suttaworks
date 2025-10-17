import logging
import subprocess
import configparser
from pathlib import Path

from src.db_updater.post_processors import (
    bilara_processor, 
    html_text_authors_processor,
    cips_processor,
    parallels_processor
)

log = logging.getLogger(__name__)

def _run_command(command: list[str], cwd: Path):
    # ... (Hàm này không thay đổi)
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
        
        if "nothing to commit, working tree clean" in result.stdout:
            log.info("Không có thay đổi nào để commit.")
            return True 
        
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


# --- HÀM ĐÃ ĐƯỢC CẬP NHẬT ---
def process_git_submodules(
    submodules_config: list, 
    project_root: Path, 
    base_dir: Path,
    run_update: bool = True,
    run_post_process: bool = True,
    tasks_to_run: list[str] | None = None
):
    # --- Bước 1: Cập nhật (nếu được yêu cầu) ---
    if run_update:
        log.info("=== GIAI ĐOẠN: CẬP NHẬT DỮ LIỆU GIT SUBMODULE ===")
        base_dir.mkdir(parents=True, exist_ok=True)
        gitmodules_path = project_root / ".gitmodules"
        config = configparser.ConfigParser()
        if gitmodules_path.exists():
            config.read(gitmodules_path)

        has_new_submodules = False
        for item in submodules_config:
            # ... (logic thêm submodule mới không đổi)
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
            log.info("Cập nhật submodule hoàn tất. Tự động commit các thay đổi...")
            submodule_names = [list(item.keys())[0] for item in submodules_config]
            commit_message = f"chore(data): Update data from submodules: {', '.join(submodule_names)}"
            add_command = ["git", "add", "."]
            commit_command = ["git", "commit", "-m", commit_message]
            if _run_command(add_command, cwd=project_root):
                _run_command(commit_command, cwd=project_root)
            else:
                log.error("Lỗi khi thực hiện 'git add', bỏ qua bước commit.")
        else:
            log.warning("Cập nhật submodule thất bại. Có thể sẽ ảnh hưởng đến giai đoạn hậu xử lý.")
    else:
        log.info("Bỏ qua giai đoạn cập nhật dữ liệu Git Submodule theo yêu cầu.")


    # --- Bước 2: Hậu xử lý (nếu được yêu cầu) ---
    if run_post_process:
        log.info("=== GIAI ĐOẠN: HẬU XỬ LÝ (POST-PROCESSING) ===")
        for item in submodules_config:
            if 'post' in item and isinstance(item['post'], dict):
                submodule_name = list(item.keys())[0]
                log.info(f"🔎 Tìm thấy các tác vụ hậu xử lý cho submodule '{submodule_name}':")
                
                post_tasks = item['post']
                for task_name, task_config in post_tasks.items():
                    # --- Logic điều khiển chạy tác vụ cụ thể ---
                    if tasks_to_run is None or task_name in tasks_to_run:
                        log.info(f"  -> Bắt đầu tác vụ: '{task_name}'...")
                        
                        if task_name == "bilara":
                            bilara_processor.process_bilara_data(task_config, project_root)
                        elif task_name == "html_text":
                            html_text_authors_processor.process_html_text_authors_data(task_config, project_root)
                        elif task_name == "cips-json":
                            cips_processor.process_cips_csv_to_json(task_config, project_root)
                        elif task_name == "parallels":
                            parallels_processor.process_parallels_data(task_config, project_root)
                        else:
                            log.warning(f"  -> Tác vụ không được hỗ trợ: '{task_name}'. Bỏ qua.")
                    else:
                        log.info(f"  -> Bỏ qua tác vụ '{task_name}' theo yêu cầu.")
    else:
        log.info("Bỏ qua giai đoạn hậu xử lý theo yêu cầu.")