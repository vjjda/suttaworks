# Path: src/db_builder/main.py
#!/usr/bin/env python3

import logging
from pathlib import Path
import argparse # <-- IMPORT MỚI

from src.config.constants import PROJECT_ROOT, CONFIG_PATH
from src.config.logging_config import setup_logging
from src.db_builder.config_loader import load_config
from src.db_builder.database_manager import DatabaseManager
from src.db_builder.processors.hierarchy_processor import HierarchyProcessor
from src.db_builder.processors.suttaplex_processor import SuttaplexProcessor
from src.db_builder.processors.biblio_processor import BiblioProcessor # <-- IMPORT MỚI


logger = logging.getLogger(__name__)

def main():
    """Hàm chính điều phối quá trình xây dựng database."""
    # --- THAY ĐỔI: Thêm argparse để xử lý tùy chọn dòng lệnh ---
    parser = argparse.ArgumentParser(description="Công cụ xây dựng database SuttaCentral.")
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help="Xóa file database hiện có trước khi xây dựng lại."
    )
    args = parser.parse_args()
    # --------------------------------------------------------
    
    setup_logging()
    logger.info("▶️ Bắt đầu chương trình xây dựng database...")
    
    try:
        config_file_path = CONFIG_PATH / "builder_config.yaml"
        db_config = load_config(config_file_path)
        
        db_path = PROJECT_ROOT / db_config['path'] / db_config['name']

        # --- THAY ĐỔI: Logic xóa database nếu có cờ --overwrite ---
        if args.overwrite:
            if db_path.exists():
                logger.warning(f"⚠️ Tùy chọn --overwrite được bật. Đang xóa database cũ: {db_path}")
                db_path.unlink()
            else:
                logger.info("Tùy chọn --overwrite được bật, không tìm thấy database cũ để xóa.")
        # --------------------------------------------------------

        logger.info(f"Database sẽ được tạo tại: {db_path}")

        with DatabaseManager(db_path) as db_manager:
            db_manager.create_tables_from_schema()

            # --- Bước 1: Xử lý Bibliography ---
            logger.info("--- Bắt đầu xử lý Bibliography ---")
            b_processor = BiblioProcessor(db_config['bibliography'])
            biblio_data, biblio_map = b_processor.process()
            db_manager.insert_data("Bibliography", biblio_data)

            # --- Bước 2: Xử lý Suttaplex để lấy "danh sách vàng" ---
            logger.info("--- Bắt đầu xử lý Suttaplex & Sutta_References ---")
            s_processor = SuttaplexProcessor(db_config['suttaplex'], biblio_map)
            suttaplex_data, sutta_references_data, valid_uids = s_processor.process()

            # --- Ghi Suttaplex TRƯỚC ---
            db_manager.insert_data("Suttaplex", suttaplex_data)
            db_manager.insert_data("Sutta_References", sutta_references_data)
            
            # --- Sau đó mới xử lý và ghi Hierarchy ---
            logger.info("--- Bắt đầu xử lý Hierarchy ---")
            h_processor = HierarchyProcessor(db_config['tree'], valid_uids)
            nodes_data = h_processor.process_trees()
            db_manager.insert_data("Hierarchy", nodes_data)
            
    except Exception as e:
        logger.critical(f"❌ Chương trình gặp lỗi nghiêm trọng và đã dừng lại.", exc_info=True)
    else:
        logger.info("✅ Hoàn tất chương trình xây dựng database.")

if __name__ == "__main__":
    main()