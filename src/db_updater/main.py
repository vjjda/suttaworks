# Path: /src/db_updater/main.py
import argparse
import sys
import logging
from pathlib import Path

# --- THAY ĐỔI 1: Import constants ---
# Thay vì tự định nghĩa, ta import các đường dẫn dùng chung từ constants.py
from src.config import constants

# Thêm PROJECT_ROOT vào sys.path để Python có thể tìm thấy các module trong `src`
# Điều này cần thiết để dòng `from src.config...` ở trên hoạt động
# Lưu ý: Cách làm này phổ biến nhưng có thể có cách khác tốt hơn (ví dụ: cài đặt dự án ở chế độ editable).
# Tạm thời ta vẫn giữ để đảm bảo script chạy được từ bất kỳ đâu.
sys.path.append(str(constants.PROJECT_ROOT))

from src.config.logging_config import setup_logging
from src.db_updater.config_parser import load_config
from src.db_updater.handlers import api_handler, gdrive_handler, git_handler, git_release_handler

setup_logging("db_updater.log")
log = logging.getLogger(__name__)

# --- THAY ĐỔI 2: Sử dụng hằng số đã import ---
# Xây dựng đường dẫn tới file config từ các hằng số
CONFIG_PATH = constants.CONFIG_PATH / "updater_config.yaml"
RAW_DATA_PATH = constants.RAW_DATA_PATH

def main():
    parser = argparse.ArgumentParser(description="Công cụ dòng lệnh để cập nhật dữ liệu thô cho dự án.")
    subparsers = parser.add_subparsers(dest='command', required=True, help='Các lệnh có sẵn')

    parser_update = subparsers.add_parser('update', help='Chạy một module cập nhật.')
    parser_update.add_argument('-m', '--module', required=True, help="Tên module cần cập nhật (ví dụ: 'suttaplex').")
    args = parser.parse_args()

    if args.command == 'update':
        module_name = args.module
        log.info(f"Yêu cầu cập nhật cho module: {module_name}")
        
        config = load_config(CONFIG_PATH)
        if not config: return
        if module_name not in config:
            log.error(f"Không tìm thấy module '{module_name}' trong file cấu hình.")
            log.info(f"Các module hiện có: {list(config.keys())}")
            return

        module_config = config[module_name]
        # Sử dụng RAW_DATA_PATH đã import
        destination_dir = RAW_DATA_PATH / module_name
        
        if not isinstance(module_config, dict):
            log.warning(f"Cấu trúc config cho module '{module_name}' không hợp lệ. Cần phải là một dictionary.")
            return

        module_type = list(module_config.keys())[0]
        handler_config = module_config[module_type]
        log.info(f"Bắt đầu cập nhật module '{module_name}' với handler '{module_type}'...")
        
        if module_type == "api":
            api_handler.process_api_data(handler_config, destination_dir)
        elif module_type == "google-drive":
            gdrive_handler.process_gdrive_data(handler_config, destination_dir)
        elif module_type == "git-submodule":
            # Cần truyền PROJECT_ROOT vào function này
            git_handler.process_git_submodules(handler_config, constants.PROJECT_ROOT, destination_dir)
        elif module_type == "git-release":
            git_release_handler.process_git_release_data(handler_config, destination_dir)
        else:
            log.warning(f"Chưa hỗ trợ loại handler '{module_type}'.")

        log.info("Hoàn tất!")

if __name__ == "__main__":
    main()