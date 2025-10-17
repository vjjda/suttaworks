import argparse
import sys
import logging
from pathlib import Path

# Thêm PROJECT_ROOT vào sys.path để có thể import từ src
# (Giữ nguyên logic này để đảm bảo script chạy ổn định)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.config.logging_config import setup_logging
from src.db_updater.config_parser import load_config
from src.db_updater.handlers import api_handler, gdrive_handler, git_handler, git_release_handler
from src.config import constants

setup_logging("db_updater.log")
log = logging.getLogger(__name__)

def get_available_tasks(config: dict, module_name: str) -> list[str]:
    """Trích xuất danh sách các tác vụ hậu xử lý có sẵn từ config cho một module."""
    tasks = []
    module_config = config.get(module_name, {})
    
    # --- THAY ĐỔI LOGIC CHO GIT-SUBMODULE ---
    if 'git-submodule' in module_config:
        # Giờ đây 'post' là một key trực tiếp, giống hệt 'api'
        submodule_config = module_config['git-submodule']
        if 'post' in submodule_config and isinstance(submodule_config['post'], dict):
            tasks.extend(submodule_config['post'].keys())
    elif 'api' in module_config:
        api_config = module_config['api']
        if 'post' in api_config and isinstance(api_config['post'], dict):
            tasks.extend(api_config['post'].keys())
    
    return list(dict.fromkeys(tasks))

def main():
    # --- Lượt 1: Parser tối giản để lấy tên module ---
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument('-m', '--module', required=True, help="Tên module cần cập nhật (ví dụ: 'git').")
    # parse_known_args sẽ không báo lỗi nếu có các argument lạ
    args, unknown = pre_parser.parse_known_args()
    
    config = load_config(constants.CONFIG_PATH / "updater_config.yaml")
    if not config: return
    
    module_name = args.module
    if module_name not in config:
        log.error(f"Không tìm thấy module '{module_name}' trong file cấu hình.")
        log.info(f"Các module hiện có: {list(config.keys())}")
        return
        
    available_tasks = get_available_tasks(config, module_name)

    # --- Lượt 2: Parser hoàn chỉnh với tất cả các tùy chọn ---
    parser = argparse.ArgumentParser(
        description="Công cụ dòng lệnh để cập nhật dữ liệu thô.",
        epilog=f"Các tác vụ hậu xử lý có sẵn cho module '{module_name}': {', '.join(available_tasks) or 'Không có'}"
    )
    parser.add_argument('-m', '--module', required=True, help="Tên module cần cập nhật.")
    parser.add_argument('--update-only', action='store_true', help="Chỉ cập nhật dữ liệu, không chạy hậu xử lý.")
    parser.add_argument('--post-process-only', action='store_true', help="Chỉ chạy hậu xử lý, không cập nhật dữ liệu.")
    parser.add_argument('--tasks', type=str, default=None, help="Danh sách các tác vụ hậu xử lý cần chạy, ngăn cách bởi dấu phẩy (ví dụ: bilara,parallels).")
    
    # Parse lại toàn bộ argument
    final_args = parser.parse_args()
    
    tasks_to_run = None
    if final_args.tasks:
        tasks_to_run = [task.strip() for task in final_args.tasks.split(',')]
        # Validate tasks
        for task in tasks_to_run:
            if task not in available_tasks:
                log.error(f"Tác vụ '{task}' không hợp lệ cho module '{module_name}'.")
                log.info(f"Các tác vụ hợp lệ: {available_tasks}")
                return

    if final_args.update_only and final_args.post_process_only:
        log.error("Không thể sử dụng đồng thời '--update-only' và '--post-process-only'.")
        return

    log.info(f"Bắt đầu xử lý module: '{module_name}'")
    module_config = config[module_name]
    destination_dir = constants.RAW_DATA_PATH / module_name
    
    module_type = list(module_config.keys())[0]
    handler_config = module_config[module_type]

    # --- Logic điều khiển luồng chạy ---
    run_update = not final_args.post_process_only
    run_post_process = not final_args.update_only

    if module_type == "git-submodule":
        git_handler.process_git_submodules(
            handler_config, 
            constants.PROJECT_ROOT, 
            destination_dir,
            run_update=run_update,
            run_post_process=run_post_process,
            tasks_to_run=tasks_to_run
        )
    elif module_type == "api":
        api_handler.process_api_data(
            handler_config, 
            destination_dir,
            run_update=run_update,
            run_post_process=run_post_process,
            tasks_to_run=tasks_to_run
        )
    else:
        log.warning(f"Chưa hỗ trợ logic chạy tùy chỉnh cho handler '{module_type}'.")

    log.info("Hoàn tất!")

if __name__ == "__main__":
    main()