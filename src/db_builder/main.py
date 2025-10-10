# Path: src/db_builder/main.py
#!/usr/bin/env python3

import logging
from pathlib import Path

# --- Sửa lỗi import bằng cách chạy với -m ---
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
        config_file_path = CONFIG_PATH / "builder_config.yaml"
        
        # --- THAY ĐỔI Ở ĐÂY ---
        # Code cũ:
        # config = load_config(config_file_path)
        # db_config = config['suttacentral-sqlite']

        # Code mới:
        # Hàm load_config đã trả về đúng phần chúng ta cần
        db_config = load_config(config_file_path)
        # --- KẾT THÚC THAY ĐỔI ---
        
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