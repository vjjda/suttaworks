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
from db_builder.database_manager import DatabaseManager
from db_builder.processors.hierarchy_processor import HierarchyProcessor # <-- IMPORT MỚI

logger = logging.getLogger(__name__)

def main():
    """Hàm chính điều phối quá trình xây dựng database."""
    setup_logging()
    logger.info("▶️ Bắt đầu chương trình xây dựng database...")
    
    try:
        # 1. Tải cấu hình
        config_file_path = PROJECT_ROOT / "config" / "builder_config.yaml"
        db_config = load_config(config_file_path)
        
        # 2. Khởi tạo DatabaseManager và tạo bảng
        db_path = Path(db_config['path']) / db_config['name']
        logger.info(f"Database sẽ được tạo tại: {db_path}")

        with DatabaseManager(db_path) as db_manager:
            db_manager.create_hierarchy_table()

            # 3. Khởi tạo và chạy HierarchyProcessor
            processor = HierarchyProcessor(db_config['tree'])
            nodes_data = processor.process_trees()

            # 4. Ghi dữ liệu đã xử lý vào database
            if nodes_data:
                db_manager.insert_hierarchy_nodes(nodes_data)

    except Exception as e:
        logger.critical(f"❌ Chương trình gặp lỗi nghiêm trọng và đã dừng lại.", exc_info=True)
    else:
        logger.info("✅ Hoàn tất chương trình xây dựng database.")

if __name__ == "__main__":
    main()