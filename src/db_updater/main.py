# Path: src/db_builder/main.py
#!/usr//env python3

import logging
from pathlib import Path
import sys

# --- THAY ĐỔI 1: Import hằng số từ file tập trung ---
# Thêm src vào sys.path để có thể import từ các thư mục khác
PROJECT_ROOT_FROM_MAIN = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT_FROM_MAIN))

from src.config.constants import PROJECT_ROOT, CONFIG_PATH
from src.config.logging_config import setup_logging
from src.db_builder.config_loader import load_config
from src.db_builder.database_manager import DatabaseManager
from src.db_builder.processors.hierarchy_processor import HierarchyProcessor

logger = logging.getLogger(__name__)

def main():
    """Hàm chính điều phối quá trình xây dựng database."""
    setup_logging()
    logger.info("▶️ Bắt đầu chương trình xây dựng database...")
    
    try:
        # 2. Tải cấu hình, sử dụng đường dẫn từ constants
        config_file_path = CONFIG_PATH / "builder_config.yaml"
        config = load_config(config_file_path)
        db_config = config['suttacentral-sqlite']
        
        # Đường dẫn database giờ cũng được tạo từ constants
        db_path = PROJECT_ROOT / db_config['path'] / db_config['name']
        logger.info(f"Database sẽ được tạo tại: {db_path}")

        with DatabaseManager(db_path) as db_manager:
            db_manager.create_hierarchy_table()
            processor = HierarchyProcessor(db_config['tree'])
            nodes_data = processor.process_trees()
            if nodes_data:
                db_manager.insert_hierarchy_nodes(nodes_data)

    except Exception as e:
        logger.critical(f"❌ Chương trình gặp lỗi nghiêm trọng và đã dừng lại.", exc_info=True)
    else:
        logger.info("✅ Hoàn tất chương trình xây dựng database.")

if __name__ == "__main__":
    main()