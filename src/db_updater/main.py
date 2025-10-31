# Path: src/db_updater/main.py
import argparse
import sys
import logging
from pathlib import Path

from src.config.logging_config import setup_logging
from src.db_updater.config_parser import load_config
from src.db_updater.handlers import (
    api_handler,
    gdrive_handler,
    git_handler,
    git_release_handler,
)
from src.config import constants

setup_logging("db_updater.log")
log = logging.getLogger(__name__)

HANDLER_DISPATCHER = {
    "git-submodule": git_handler.process_git_submodules,
    "api": api_handler.process_api_data,
    "google-drive": gdrive_handler.process_gdrive_data,
    "git-release": git_release_handler.process_git_release_data,
}


def get_available_tasks(config: dict, module_name: str) -> list[str]:
    tasks = []
    module_config = config.get(module_name, {})
    if not module_config:
        return []
    handler_type = list(module_config.keys())[0]
    handler_config = module_config[handler_type]

    if "post_tasks" in handler_config and isinstance(
        handler_config["post_tasks"], dict
    ):
        tasks.extend(handler_config["post_tasks"].keys())
    return list(dict.fromkeys(tasks))


def _setup_and_parse_args(available_modules: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Công cụ dòng lệnh để cập nhật dữ liệu thô.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-m",
        "--module",
        nargs="?",
        const="_LIST_MODULES_",
        default=None,
        help="Tên module cần cập nhật (e.g., 'git', 'git,suttaplex', 'all').\n"
        "Nếu gọi không có giá trị, sẽ liệt kê các module có sẵn.",
    )

    parser.add_argument(
        "-u",
        "--update-only",
        action="store_true",
        help="Chỉ cập nhật dữ liệu, không chạy hậu xử lý.",
    )
    parser.add_argument(
        "-p",
        "--post-tasks-only",
        action="store_true",
        help="Chỉ chạy các tác vụ hậu xử lý, không cập nhật dữ liệu.",
    )
    parser.add_argument(
        "-t",
        "--tasks",
        nargs="?",
        const="_LIST_TASKS_",
        default=None,
        type=str,
        help="Chạy tác vụ hậu xử lý cụ thể. CHỈ hoạt động khi chọn một module.\n"
        "Nếu gọi không có giá trị, sẽ liệt kê các tác vụ có sẵn cho module đó.",
    )

    return parser.parse_args()


def _process_and_validate_args(args: argparse.Namespace, config: dict) -> tuple | None:
    available_modules = list(config.keys())

    if args.module == "_LIST_MODULES_":
        log.info("Các module có sẵn:")
        for mod in available_modules:
            print(f"- {mod}")
        return None

    if args.module is None:
        log.error("Argument -m/--module là bắt buộc.")
        return None

    modules_to_run = (
        available_modules
        if args.module == "all"
        else [mod.strip() for mod in args.module.split(",")]
    )
    if any(mod not in available_modules for mod in modules_to_run):
        log.error(f"Một hoặc nhiều module không hợp lệ: {args.module}")
        return None

    tasks_to_run = None
    if args.tasks is not None:
        if len(modules_to_run) > 1:
            log.error(
                "'-t/--tasks' chỉ có thể được sử dụng khi chạy một module duy nhất."
            )
            return None

        single_module = modules_to_run[0]
        available_tasks = get_available_tasks(config, single_module)

        if args.tasks == "_LIST_TASKS_":
            log.info(f"Các tác vụ có sẵn cho module '{single_module}':")
            for task in available_tasks:
                print(f"- {task}")
            return None
        else:
            tasks_to_run = [task.strip() for task in args.tasks.split(",")]
            if any(task not in available_tasks for task in tasks_to_run):
                log.error(
                    f"Một hoặc nhiều tác vụ không hợp lệ cho module '{single_module}'."
                )
                return None

    if args.update_only and args.post_tasks_only:
        log.error(
            "Không thể dùng đồng thời '-u/--update-only' và '-p/--post-tasks-only'."
        )
        return None

    run_update = not args.post_tasks_only
    run_post_process = not args.update_only

    return modules_to_run, tasks_to_run, run_update, run_post_process


def main():
    config = load_config(constants.CONFIG_PATH / "updater_config.yaml")
    if not config:
        return

    args = _setup_and_parse_args(list(config.keys()))

    processed_args = _process_and_validate_args(args, config)
    if not processed_args:
        return

    modules_to_run, tasks_to_run, run_update, run_post_process = processed_args

    log.info(f"Các module sẽ được xử lý: {', '.join(modules_to_run)}")
    for module_name in modules_to_run:
        log.info(f"--- Bắt đầu xử lý module: '{module_name}' ---")
        module_config = config[module_name]
        destination_dir = constants.RAW_DATA_PATH / module_name

        module_type = list(module_config.keys())[0]
        handler_config = module_config[module_type]

        handler_func = HANDLER_DISPATCHER.get(module_type)
        if handler_func:
            handler_func(
                handler_config,
                destination_dir,
                run_update=run_update,
                run_post_process=run_post_process,
                tasks_to_run=tasks_to_run,
            )
        else:
            log.warning(f"Không tìm thấy handler cho loại module '{module_type}'.")

    log.info("Hoàn tất!")


if __name__ == "__main__":
    main()