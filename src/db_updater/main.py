# Path: /src/db_updater/main.py
import argparse
from pathlib import Path
import sys
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from src.config.logging_config import setup_logging
from src.db_updater.config_parser import load_config
from src.db_updater.handlers import api_handler, gdrive_handler, git_handler

setup_logging()
log = logging.getLogger(__name__)

# --- THAY ĐỔI 1: Cập nhật tên file config ---
CONFIG_PATH = PROJECT_ROOT / "src/config/updater_config.yaml"
RAW_DATA_PATH = PROJECT_ROOT / "data/raw"

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
        destination_dir = RAW_DATA_PATH / module_name
        
        # --- THAY ĐỔI 2: Giản lược logic điều phối ---
        # Logic bây giờ nhất quán cho tất cả các module
        if not isinstance(module_config, dict):
            log.warning(f"Cấu trúc config cho module '{module_name}' không hợp lệ. Cần phải là một dictionary.")
            return

        # Lấy ra loại handler từ key đầu tiên
        module_type = list(module_config.keys())[0]
        handler_config = module_config[module_type]
        log.info(f"Bắt đầu cập nhật module '{module_name}' với handler '{module_type}'...")
        
        if module_type == "api":
            api_handler.process_api_data(handler_config, destination_dir)
        elif module_type == "google-drive":
            gdrive_handler.process_gdrive_data(handler_config, destination_dir)
        elif module_type == "sub-submodule": # <-- Tên handler mới cho git
            git_handler.process_git_submodules(handler_config, PROJECT_ROOT, destination_dir)
        else:
            log.warning(f"Chưa hỗ trợ loại handler '{module_type}'.")
        # ---------------------------------------------------

        log.info("Hoàn tất!")

if __name__ == "__main__":
    main()