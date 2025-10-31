# Path: src/db_updater/handlers/base_handler.py
import logging
from abc import ABC, abstractmethod
from pathlib import Path
import importlib

log = logging.getLogger(__name__)


class BaseHandler(ABC):
    """
    Lớp cơ sở trừu tượng cho tất cả các trình xử lý (handler).
    Mỗi handler cụ thể phải kế thừa từ lớp này và triển khai phương thức execute().
    """

    def __init__(self, handler_config: dict, destination_dir: Path):
        """
        Khởi tạo handler.

        Args:
            handler_config: Phần cấu hình dành riêng cho handler này.
            destination_dir: Thư mục đích để lưu dữ liệu.
        """
        self.handler_config = handler_config
        self.destination_dir = destination_dir
        self.post_tasks_config = self.handler_config.get("post_tasks", {})

    @abstractmethod
    def execute(self):
        """
        Phương thức trừu tượng để thực thi logic chính của handler (ví dụ: tải dữ liệu).
        Các lớp con phải triển khai phương thức này.
        """
        pass

    def _run_post_task(self, task_name: str):
        """
        Tìm và chạy một tác vụ hậu xử lý cụ thể.
        """
        if task_name not in self.post_tasks_config:
            log.warning(f"Tác vụ '{task_name}' không được định nghĩa trong cấu hình cho handler này.")
            return

        task_config = self.post_tasks_config[task_name]
        module_name = task_config.get("module")
        if not module_name:
            log.error(f"Cấu hình cho tác vụ '{task_name}' bị thiếu 'module'.")
            return

        try:
            # Tự động import module từ src.db_updater.post_tasks
            task_module = importlib.import_module(f"src.db_updater.post_tasks.{module_name}")
            log.info(f"Đang chạy tác vụ hậu xử lý: {task_name}")
            # Giả định mỗi module tác vụ có một hàm run() với config làm tham số
            task_module.run(task_config)
        except ImportError:
            log.error(f"Không thể import module tác vụ: {module_name}")
        except Exception:
            log.critical(f"Lỗi nghiêm trọng khi đang chạy tác vụ '{task_name}'.", exc_info=True)

    def run_post_tasks(self, tasks_to_run: list[str] | None = None):
        """
        Chạy các tác vụ hậu xử lý được định nghĩa trong cấu hình.

        Args:
            tasks_to_run: Một danh sách các tác vụ cụ thể cần chạy.
                          Nếu là None, chạy tất cả các tác vụ.
        """
        if not self.post_tasks_config:
            log.info("Không có tác vụ hậu xử lý nào được định nghĩa.")
            return

        if tasks_to_run:
            # Chỉ chạy các task được chỉ định
            for task_name in tasks_to_run:
                self._run_post_task(task_name)
        else:
            # Chạy tất cả các task theo thứ tự trong config
            for task_name in self.post_tasks_config.keys():
                self._run_post_task(task_name)

    def process(
        self,
        run_update: bool = True,
        run_post_process: bool = True,
        tasks_to_run: list[str] | None = None,
    ):
        """
        Quy trình xử lý hoàn chỉnh cho handler.

        Args:
            run_update: Cờ để quyết định có chạy logic cập nhật chính hay không.
            run_post_process: Cờ để quyết định có chạy các tác vụ hậu xử lý hay không.
            tasks_to_run: Danh sách các tác vụ hậu xử lý cụ thể cần chạy.
        """
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
