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
from src.db_builder.processors.bilara_segment_processor import BilaraSegmentProcessor

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
            b_processor = BiblioProcessor(db_config['bibliography'][0])
            biblio_data, biblio_map = b_processor.process()
            
            logger.info("--- Bắt đầu xử lý Suttaplex và các dữ liệu liên quan ---")
            s_processor = SuttaplexProcessor(db_config['suttaplex'], biblio_map)
            (suttaplex_data, sutta_references_data, authors_data, languages_data, 
             translations_data, valid_uids, uid_to_type_map) = s_processor.process()
            
            logger.info("--- Bắt đầu xử lý Hierarchy ---")
            h_processor = HierarchyProcessor(db_config['tree'], valid_uids, uid_to_type_map)
            nodes_data = h_processor.process_trees()
            
            logger.info("--- Bắt đầu chèn dữ liệu vào database ---")
            db_manager.insert_data("Bibliography", biblio_data)
            db_manager.insert_data("Authors", authors_data)
            db_manager.insert_data("Languages", languages_data)
            db_manager.insert_data("Suttaplex", suttaplex_data)
            db_manager.insert_data("Hierarchy", nodes_data)
            db_manager.insert_data("Sutta_References", sutta_references_data)
            db_manager.insert_data("Translations", translations_data)
            
            # --- CẬP NHẬT LOGIC GỌI PROCESSOR ---
            logger.info("--- Bắt đầu xử lý dữ liệu Segment Bilara ---")
            all_segment_data = []
            if 'bilara-segment' in db_config:
                for config_item in db_config['bilara-segment']:
                    logger.info(f"Chạy BilaraSegmentProcessor cho config: {config_item['json']}")
                    # Không cần truyền db_manager nữa
                    segment_proc = BilaraSegmentProcessor(config_item)
                    segment_data = segment_proc.process()
                    all_segment_data.extend(segment_data)
                
                logger.info(f"Tổng hợp được {len(all_segment_data)} segment từ tất cả các nguồn Bilara.")
                db_manager.insert_data("Segments", all_segment_data)
            else:
                logger.warning("Không tìm thấy cấu hình 'bilara-segment' trong builder_config.yaml. Bỏ qua.")
            # --- KẾT THÚC CẬP NHẬT ---
                
    except Exception as e:
        logger.critical(f"❌ Chương trình gặp lỗi nghiêm trọng và đã dừng lại.", exc_info=True)
    else:
        logger.info("✅ Hoàn tất chương trình xây dựng database.")

if __name__ == "__main__":
    main()