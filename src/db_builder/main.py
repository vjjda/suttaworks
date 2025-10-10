# Path: src/db_builder/main.py
#!/usr/bin/env python3

import logging
from pathlib import Path

# Thêm src vào sys.path để có thể import từ các thư mục khác
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from config.logging_config import setup_logging
from db_builder.config_loader import load_config
from db_builder.database_manager import DatabaseManager # <-- IMPORT MỚI

logger = logging.getLogger(__name__)

def main():
    """Hàm chính điều phối quá trình xây dựng database."""
    setup_logging()
    logger.info("▶️ Bắt đầu chương trình xây dựng database...")
    
    try:
        # 1. Tải cấu hình
        config_file_path = PROJECT_ROOT / "config" / "builder_config.yaml"
        db_config = load_config(config_file_path)
        
        # 2. Chuẩn bị đường dẫn và khởi tạo DatabaseManager
        db_path = Path(db_config['path']) / db_config['name']
        logger.info(f"Database sẽ được tạo tại: {db_path}")

        # 3. Sử dụng 'with' để quản lý kết nối database
        with DatabaseManager(db_path) as db_manager:
            # Tạo bảng Hierarchy
            db_manager.create_hierarchy_table()

        # (Các bước xử lý JSON sẽ được thêm vào đây)

    except Exception as e:
        logger.critical(f"❌ Chương trình gặp lỗi nghiêm trọng và đã dừng lại: {e}", exc_info=True)
    else:
        logger.info("✅ Hoàn tất chương trình xây dựng database.")

if __name__ == "__main__":
    main()