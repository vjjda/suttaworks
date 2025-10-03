# Path: /src/config/logging_config.py
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOGS_DIR = PROJECT_ROOT / "logs"

def setup_logging():
    """Cấu hình logging với các cấp độ khác nhau cho file và console."""
    LOGS_DIR.mkdir(exist_ok=True)
    log_file = LOGS_DIR / "updater.log"

    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # ---- Quan trọng: Đặt mức log của logger GỐC là DEBUG ----
    # Điều này cho phép nó thu nhận tất cả các thông điệp.
    # Việc lọc sẽ được thực hiện tại các handler.
    root_logger.setLevel(logging.DEBUG)

    # --- File Handler (Ghi lại mọi thứ từ mức DEBUG trở lên) ---
    file_handler = RotatingFileHandler(
        log_file, maxBytes=5*1024*1024, backupCount=2, encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # --- Console Handler (Chỉ hiển thị từ mức INFO trở lên) ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Thêm handlers vào logger gốc
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)