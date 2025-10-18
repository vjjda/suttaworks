# Path: src/db_builder/main.py

#!/usr/r/bin/env python3

import logging
import argparse
from pathlib import Path

from src.config.constants import PROJECT_ROOT, CONFIG_PATH
from src.config.logging_config import setup_logging
from src.db_builder.config_loader import load_config
from src.db_builder.database_manager import DatabaseManager
from src.db_builder.processors.hierarchy_processor import HierarchyProcessor
from src.db_builder.processors.suttaplex_processor import SuttaplexProcessor
from src.db_builder.processors.biblio_processor import BiblioProcessor
from src.db_builder.processors.bilara_segment_processor import BilaraSegmentProcessor

# Khởi tạo logger cho module này
logger = logging.getLogger(__name__)

def main():
    """
    Hàm chính điều phối toàn bộ quá trình xây dựng database SuttaCentral,
    từ việc tạo bảng, xử lý dữ liệu từ các nguồn khác nhau, cho đến việc
    chèn dữ liệu đã xử lý vào database.
    """
    # --- 1. Thiết lập ban đầu ---
    parser = argparse.ArgumentParser(description="Công cụ xây dựng database SuttaCentral.")
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help="Xóa file database hiện có trước khi xây dựng lại."
    )
    args = parser.parse_args()
    
    setup_logging("db_builder.log")
    logger.info("▶️  Bắt đầu chương trình xây dựng database...")
    
    try:
        # --- 2. Tải cấu hình và chuẩn bị Database ---
        config_file_path = CONFIG_PATH / "builder_config.yaml"
        db_config = load_config(config_file_path)
        
        db_path = PROJECT_ROOT / db_config['path'] / db_config['name']

        if args.overwrite:
            if db_path.exists():
                logger.warning(f"⚠️  Tùy chọn --overwrite được bật. Đang xóa database cũ: {db_path}")
                db_path.unlink()
            else:
                logger.info("Tùy chọn --overwrite được bật, không tìm thấy database cũ để xóa.")

        logger.info(f"Database sẽ được tạo tại: {db_path}")

        with DatabaseManager(db_path) as db_manager:
            # --- 3. Tạo cấu trúc bảng (Schema) ---
            logger.info("--- Bắt đầu tạo cấu trúc bảng cho database ---")
            
            # Gọi hàm tạo các bảng tĩnh từ file schema chính
            main_schema_path = PROJECT_ROOT / "src/db_builder/suttacentral_schema.sql"
            db_manager.create_tables_from_schema(main_schema_path) # Giả định hàm này nhận vào path

            # Tạo các bảng Bilara động từ template
            bilara_config = db_config.get('bilara-segment', {})
            list_of_sources = bilara_config.get('json', [])
            table_names_to_create = [list(d.keys())[0] for d in list_of_sources if d]
            
            if table_names_to_create:
                template_path = PROJECT_ROOT / "src/db_builder/suttacentral_schema_bilara_template.sql"
                db_manager.create_tables_from_template(template_path, table_names_to_create)
                logger.info(f"Đã tạo {len(table_names_to_create)} bảng từ template: {', '.join(table_names_to_create)}")
            
            # --- 4. Xử lý và chèn dữ liệu chính ---
            logger.info("--- Bắt đầu xử lý Bibliography ---")
            b_processor = BiblioProcessor(db_config['bibliography'])
            biblio_data, biblio_map = b_processor.process()
            
            logger.info("--- Bắt đầu xử lý Suttaplex và các dữ liệu liên quan ---")
            s_processor = SuttaplexProcessor(db_config['suttaplex'], biblio_map)
            (suttaplex_data, sutta_references_data, authors_data, languages_data, 
             translations_data, valid_uids, uid_to_type_map) = s_processor.process()
            
            logger.info("--- Bắt đầu xử lý Hierarchy ---")
            h_processor = HierarchyProcessor(db_config['tree'], valid_uids, uid_to_type_map)
            nodes_data = h_processor.process_trees()
            
            logger.info("--- Bắt đầu chèn dữ liệu vào các bảng chính ---")
            db_manager.insert_data("Bibliography", biblio_data)
            db_manager.insert_data("Authors", authors_data)
            db_manager.insert_data("Languages", languages_data)
            db_manager.insert_data("Suttaplex", suttaplex_data)
            db_manager.insert_data("Hierarchy", nodes_data)
            db_manager.insert_data("Sutta_References", sutta_references_data)
            db_manager.insert_data("Translations", translations_data)
            
            # --- 5. Xử lý và chèn dữ liệu cho các bảng Bilara ---
            logger.info("--- Bắt đầu xử lý dữ liệu từ các nguồn Bilara ---")
            if not bilara_config:
                logger.warning("⚠️  Không tìm thấy mục 'bilara-segment' trong config. Bỏ qua.")
            else:
                folder = bilara_config.get('folder')
                author_remap = bilara_config.get('author-remap', {})

                for source_dict in list_of_sources:
                    for table_name, manifest_path in source_dict.items():
                        logger.info(f"▶️  Đang xử lý nguồn '{manifest_path}' cho bảng: '{table_name}'")
                        
                        processor_config = {
                            'folder': folder,
                            'json': manifest_path,
                            'author-remap': author_remap
                        }
                        
                        segment_proc = BilaraSegmentProcessor(processor_config)
                        segment_data = segment_proc.process(target_table=table_name)

                        logger.info(f"Tổng hợp được {len(segment_data)} segment. Đang chèn vào bảng '{table_name}'...")
                        db_manager.insert_data(table_name, segment_data)
                        logger.info(f"✅  Hoàn tất chèn dữ liệu cho bảng '{table_name}'.")
            
    except Exception as e:
        logger.critical("❌  Chương trình gặp lỗi nghiêm trọng và đã dừng lại.", exc_info=True)
    else:
        logger.info("✅  Hoàn tất chương trình xây dựng database thành công.")

if __name__ == "__main__":
    main()