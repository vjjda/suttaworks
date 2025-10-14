# Path: src/db_builder/main.py
#!/usr/bin/env python3

import logging
from pathlib import Path
import argparse

from src.config.constants import PROJECT_ROOT, CONFIG_PATH
from src.config.logging_config import setup_logging
from src.db_builder.config_loader import load_config
from src.db_builder.database_manager import DatabaseManager
from src.db_builder.processors.hierarchy_processor import HierarchyProcessor
from src.db_builder.processors.suttaplex_processor import SuttaplexProcessor
from src.db_builder.processors.biblio_processor import BiblioProcessor


logger = logging.getLogger(__name__)

def main():
    """Hàm chính điều phối quá trình xây dựng database."""
    parser = argparse.ArgumentParser(description="Công cụ xây dựng database SuttaCentral.")
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help="Xóa file database hiện có trước khi xây dựng lại."
    )
    args = parser.parse_args()
    
    # --- THAY ĐỔI Ở ĐÂY ---
    setup_logging("db_builder.log")
    logger.info("▶️ Bắt đầu chương trình xây dựng database...")
    
    try:
        config_file_path = CONFIG_PATH / "builder_config.yaml"
        db_config = load_config(config_file_path)
        
        db_path = PROJECT_ROOT / db_config['path'] / db_config['name']

        if args.overwrite:
            if db_path.exists():
                logger.warning(f"⚠️ Tùy chọn --overwrite được bật. Đang xóa database cũ: {db_path}")
                db_path.unlink()
            else:
                logger.info("Tùy chọn --overwrite được bật, không tìm thấy database cũ để xóa.")

        logger.info(f"Database sẽ được tạo tại: {db_path}")

        with DatabaseManager(db_path) as db_manager:
            db_manager.create_tables_from_schema()

            logger.info("--- Bắt đầu xử lý Bibliography ---")
            b_processor = BiblioProcessor(db_config['bibliography'])
            biblio_data, biblio_map = b_processor.process()
            db_manager.insert_data("Bibliography", biblio_data)

            logger.info("--- Bắt đầu xử lý Suttaplex & Sutta_References ---")
            s_processor = SuttaplexProcessor(db_config['suttaplex'], biblio_map)
            suttaplex_data, sutta_references_data, valid_uids, uid_to_type_map = s_processor.process()

            logger.info("--- Bắt đầu xử lý Hierarchy ---")
            h_processor = HierarchyProcessor(db_config['tree'], valid_uids, uid_to_type_map)
            nodes_data = h_processor.process_trees()
            
            db_manager.insert_data("Suttaplex", suttaplex_data)
            db_manager.insert_data("Hierarchy", nodes_data)
            db_manager.insert_data("Sutta_References", sutta_references_data)
            
    except Exception as e:
        logger.critical(f"❌ Chương trình gặp lỗi nghiêm trọng và đã dừng lại.", exc_info=True)
    else:
        logger.info("✅ Hoàn tất chương trình xây dựng database.")

if __name__ == "__main__":
    main()