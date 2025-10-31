# Path: src/db_updater/handlers/base_handler.py
import logging
from abc import ABC, abstractmethod
from pathlib import Path
import importlib

log = logging.getLogger(__name__)


class BaseHandler(ABC):

    def __init__(self, handler_config: dict, destination_dir: Path):
        self.handler_config = handler_config
        self.destination_dir = destination_dir
        self.post_tasks_config = self.handler_config.get("post_tasks", {})

    @abstractmethod
    def execute(self):
        pass

    def _run_post_task(self, task_name: str):
        task_config = self.post_tasks_config.get(task_name)

        if not task_config:
            log.warning(
                f"Tác vụ '{task_name}' không có cấu hình hoặc là placeholder. Bỏ qua."
            )
            return

        module_name = task_config.get("module")
        if not module_name:
            log.error(f"Cấu hình cho tác vụ '{task_name}' bị thiếu 'module'.")
            return

        try:

            task_module = importlib.import_module(
                f"src.db_updater.post_tasks.{module_name}"
            )
            log.info(f"Đang chạy tác vụ hậu xử lý: {task_name}")

            task_module.run(task_config)
        except ImportError:
            log.error(f"Không thể import module tác vụ: {module_name}")
        except Exception:
            log.critical(
                f"Lỗi nghiêm trọng khi đang chạy tác vụ '{task_name}'.", exc_info=True
            )

    def run_post_tasks(self, tasks_to_run: list[str] | None = None):
        if not self.post_tasks_config:
            log.info("Không có tác vụ hậu xử lý nào được định nghĩa.")
            return

        if tasks_to_run:

            for task_name in tasks_to_run:
                self._run_post_task(task_name)
        else:

            for task_name in self.post_tasks_config.keys():
                self._run_post_task(task_name)

    def process(
        self,
        run_update: bool = True,
        run_post_process: bool = True,
        tasks_to_run: list[str] | None = None,
    ):
        if run_update:
            log.info(f"Bắt đầu thực thi handler: {self.__class__.__name__}")
            self.execute()
            log.info(f"Hoàn tất thực thi handler: {self.__class__.__name__}")
        else:
            log.info("Bỏ qua bước cập nhật chính do cờ 'run_update' là False.")

        if run_post_process:
            log.info(f"Bắt đầu các tác vụ hậu xử lý cho: {self.__class__.__name__}")
            self.run_post_tasks(tasks_to_run)
            log.info(f"Hoàn tất các tác vụ hậu xử lý cho: {self.__class__.__name__}")
        else:
            log.info("Bỏ qua các tác vụ hậu xử lý do cờ 'run_post_process' là False.")