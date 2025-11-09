# Path: src/db_updater/db_updater_arg_parser.py
import argparse
import logging
from dataclasses import dataclass


@dataclass
class ParsedArgs:
    modules_to_run: list[str]
    tasks_to_run: list[str] | None
    run_update: bool
    run_post_process: bool


class CliArgsHandler:
    def __init__(self, config: dict, log: logging.Logger):
        self.config = config
        self.log = log
        self.available_modules = list(config.keys()) if config else []
        self.parser = self._setup_parser()

    def _setup_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Công cụ dòng lệnh để cập nhật dữ liệu thô.",
            formatter_class=argparse.RawTextHelpFormatter,
        )
        parser.add_argument(
            "-m",
            "--module",
            default=None,
            choices=self.available_modules + ["all"],
            help="Tên module cần cập nhật (e.g., 'git', 'git,suttaplex', 'all').",
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
        tasks_arg = parser.add_argument(
            "-t",
            "--tasks",
            type=str,
            help="Chạy tác vụ hậu xử lý cụ thể.\nCHỈ hoạt động khi chọn một module.",
        )

        tasks_arg.completer = self._task_completer  # type: ignore [reportAttributeAccessIssue]
        return parser

    def get_available_tasks(self, module_name: str) -> list[str]:
        tasks = []
        module_config = self.config.get(module_name, {})
        if not module_config:
            return []
        handler_type = list(module_config.keys())[0]
        handler_config = module_config[handler_type]

        if "post_tasks" in handler_config and isinstance(
            handler_config["post_tasks"], dict
        ):
            tasks.extend(handler_config["post_tasks"].keys())
        return list(dict.fromkeys(tasks))

    def _task_completer(self, prefix, parsed_args, **kwargs):
        if parsed_args.module and parsed_args.module != "all":
            module_name = parsed_args.module.split(",")[0]
            return self.get_available_tasks(module_name)
        return []

    def parse_args(self) -> argparse.Namespace:
        return self.parser.parse_args()

    def validate_args(self, args: argparse.Namespace) -> ParsedArgs | None:
        if args.module is None:
            self.log.error("Argument -m/--module là bắt buộc.")
            self.parser.print_help()
            return None

        modules_to_run = (
            self.available_modules
            if args.module == "all"
            else [mod.strip() for mod in args.module.split(",")]
        )
        if any(mod not in self.available_modules for mod in modules_to_run):
            self.log.error(f"Một hoặc nhiều module không hợp lệ: {args.module}")
            return None

        tasks_to_run = None
        if args.tasks is not None:
            if len(modules_to_run) > 1:
                self.log.error(
                    "-t/--tasks' chỉ có thể được sử dụng khi chạy một module duy nhất."
                )
                return None

            single_module = modules_to_run[0]
            available_tasks = self.get_available_tasks(single_module)

            tasks_to_run = [task.strip() for task in args.tasks.split(",")]
            if any(task not in available_tasks for task in tasks_to_run):
                self.log.error(
                    f"Một hoặc nhiều tác vụ không hợp lệ cho module '{single_module}'."
                )
                return None

        if args.update_only and args.post_tasks_only:
            self.log.error(
                "Không thể dùng đồng thời '-u/--update-only' và '-p/--post-tasks-only'."
            )
            return None

        run_update = not args.post_tasks_only
        run_post_process = not args.update_only

        return ParsedArgs(
            modules_to_run=modules_to_run,
            tasks_to_run=tasks_to_run,
            run_update=run_update,
            run_post_process=run_post_process,
        )
