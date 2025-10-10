# Path: src/db_builder/main.py
#!/usr/bin/env python3

import logging
from pathlib import Path

# --- Quan trọng: Import từ các module của chúng ta ---
# Thêm src vào sys.path để có thể import từ các thư mục khác
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from config.logging_config import setup_logging
from db_builder.config_loader import load_config

# Thiết lập logger cho file main
logger = logging.getLogger(__name__)

def main():
    """Hàm chính điều phối quá trình xây dựng database."""
    
    # 1. Thiết lập logging ngay từ đầu
    setup_logging()
    
    logger.info("▶️ Bắt đầu chương trình xây dựng database...")
    
    try:
        # 2. Xác định và tải cấu hình
        config_file_path = PROJECT_ROOT / "config" / "builder_config.yaml"
        db_config = load_config(config_file_path)
        
        logger.info(f"📁 Đường dẫn lưu database: {db_config['path']}")
        logger.info(f"🗂️ Tên file database: {db_config['name']}")
        
        # (Các bước tiếp theo sẽ được thêm vào đây)

    except Exception as e:
        logger.critical(f"❌ Chương trình gặp lỗi nghiêm trọng và đã dừng lại: {e}", exc_info=True)
    else:
        logger.info("✅ Hoàn tất chương trình xây dựng database.")

if __name__ == "__main__":
    main()