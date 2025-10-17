import argparse
import sys
import logging
from pathlib import Path

# Thêm PROJECT_ROOT vào sys.path
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
    
    if not module_config: return []
    handler_type = list(module_config.keys())[0]
    handler_config = module_config[handler_type]
    
    if 'post' in handler_config and isinstance(handler_config['post'], dict):
        tasks.extend(handler_config['post'].keys())
    
    return list(dict.fromkeys(tasks))

def main():
    config = load_config(constants.CONFIG_PATH / "updater_config.yaml")
    if not config: return
    
    available_modules = list(config.keys())

    parser = argparse.ArgumentParser(
        description="Công cụ dòng lệnh để cập nhật dữ liệu thô.",
        formatter_class=argparse.RawTextHelpFormatter # Để hiển thị epilog đẹp hơn
    )
    # THAY ĐỔI 1: Cập nhật argument '-m'
    parser.add_argument(
        '-m', '--module', 
        nargs='?',
        const='_LIST_MODULES_',
        default=None,
        help="Tên module cần cập nhật.\n"
             "Có thể là một module (e.g., 'git'),\n"
             "nhiều module (e.g., 'git,suttaplex'),\n"
             "hoặc 'all' để chạy tất cả.\n"
             "Nếu gọi mà không có giá trị, sẽ liệt kê các module có sẵn."
    )
    parser.add_argument('--update-only', action='store_true', help="Chỉ cập nhật dữ liệu, không chạy hậu xử lý.")
    parser.add_argument('--post-process-only', action='store_true', help="Chỉ chạy hậu xử lý, không cập nhật dữ liệu.")
    parser.add_argument(
        '--tasks', 
        nargs='?', const='_LIST_TASKS_', default=None, type=str, 
        help="Chạy tác vụ hậu xử lý cụ thể. CHỈ hoạt động khi chọn một module duy nhất.\n"
             "Nếu gọi mà không có giá trị, sẽ liệt kê các tác vụ có sẵn cho module đó."
    )
    
    args = parser.parse_args()

    # --- Logic xử lý mới ---

    # Kịch bản 1: Liệt kê các modules
    if args.module == '_LIST_MODULES_':
        log.info("Các module có sẵn trong file cấu hình:")
        for mod in available_modules:
            print(f"- {mod}")
        return

    if args.module is None:
        parser.error("argument -m/--module là bắt buộc (hoặc gọi '-m' không có giá trị để liệt kê).")

    # Kịch bản 2: Xác định danh sách modules cần chạy
    modules_to_run = []
    if args.module == 'all':
        modules_to_run = available_modules
    else:
        modules_to_run = [mod.strip() for mod in args.module.split(',')]
        for mod in modules_to_run:
            if mod not in available_modules:
                log.error(f"Module '{mod}' không tồn tại trong file cấu hình.")
                return

    # Kịch bản 3: Xử lý và kiểm tra tính hợp lệ của --tasks
    tasks_to_run = None
    if args.tasks is not None:
        if len(modules_to_run) > 1:
            log.error("Tùy chọn '--tasks' chỉ có thể được sử dụng khi chạy một module duy nhất.")
            return
        
        single_module = modules_to_run[0]
        available_tasks = get_available_tasks(config, single_module)
        
        if args.tasks == '_LIST_TASKS_':
            log.info(f"Các tác vụ có sẵn cho module '{single_module}':")
            if available_tasks:
                for task in available_tasks:
                    print(f"- {task}")
            else:
                log.info("Không có tác vụ nào.")
            return
        else:
            tasks_to_run = [task.strip() for task in args.tasks.split(',')]
            for task in tasks_to_run:
                if task not in available_tasks:
                    log.error(f"Tác vụ '{task}' không hợp lệ cho module '{single_module}'.")
                    return

    if args.update_only and args.post_process_only:
        log.error("Không thể sử dụng đồng thời '--update-only' và '--post-process-only'.")
        return

    run_update = not args.post_process_only
    run_post_process = not args.update_only

    # --- Vòng lặp chính để chạy các modules ---
    log.info(f"Các module sẽ được xử lý: {', '.join(modules_to_run)}")
    for module_name in modules_to_run:
        log.info(f"--- Bắt đầu xử lý module: '{module_name}' ---")
        module_config = config[module_name]
        destination_dir = constants.RAW_DATA_PATH / module_name
        
        module_type = list(module_config.keys())[0]
        handler_config = module_config[module_type]

        if module_type == "git-submodule":
            git_handler.process_git_submodules(
                handler_config, constants.PROJECT_ROOT, destination_dir,
                run_update=run_update, run_post_process=run_post_process, tasks_to_run=tasks_to_run
            )
        elif module_type == "api":
            api_handler.process_api_data(
                handler_config, destination_dir,
                run_update=run_update, run_post_process=run_post_process, tasks_to_run=tasks_to_run
            )
        elif module_type == "google-drive":
            gdrive_handler.process_gdrive_data(
                handler_config, destination_dir,
                run_update=run_update, run_post_process=run_post_process, tasks_to_run=tasks_to_run
            )
        elif module_type == "git-release":
            git_release_handler.process_git_release_data(
                handler_config, destination_dir,
                run_update=run_update, run_post_process=run_post_process, tasks_to_run=tasks_to_run
            )
        else:
            log.warning(f"Chưa hỗ trợ logic chạy tùy chỉnh cho handler '{module_type}'.")

    log.info("Hoàn tất!")

if __name__ == "__main__":
    main()