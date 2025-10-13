# Path: src/db_builder/database_manager.py
import sqlite3
import logging
from pathlib import Path
from typing import Any, Dict, List

from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Quản lý các thao tác với database SQLite một cách tổng quát."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = None
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """Thiết lập kết nối đến database."""
        try:
            logger.info(f"Đang kết nối đến database: {self.db_path}")
            self.conn = sqlite3.connect(self.db_path)
            logger.info("✅ Kết nối database thành công.")
        except sqlite3.Error as e:
            logger.error(f"Lỗi khi kết nối đến database: {e}")
            raise

    def close(self):
        """Đóng kết nối database nếu đang mở."""
        if self.conn:
            self.conn.close()
            logger.info("Đã đóng kết nối database.")

    def create_tables_from_schema(self):
        """Đọc file schema và thực thi để tạo tất cả các bảng."""
        # --- THAY ĐỔI TÊN FILE Ở ĐÂY ---
        schema_path = PROJECT_ROOT / "src" / "db_builder" / "suttacentral_schema.sql"
        
        logger.info(f"Đang đọc schema từ: {schema_path}")
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            self.conn.executescript(schema_sql)
            logger.info("✅ Đã tạo tất cả các bảng từ file schema thành công.")
        except sqlite3.Error as e:
            logger.error(f"Lỗi khi thực thi file schema: {e}")
            raise

    def insert_data(self, table_name: str, data: List[Dict[str, Any]]):
        """
        Chèn một danh sách dữ liệu vào một bảng bất kỳ.
        Hàm sẽ tự động xây dựng câu lệnh SQL dựa trên các key của dictionary.
        """
        if not data:
            logger.warning(f"Không có dữ liệu để chèn vào bảng '{table_name}'.")
            return

        logger.info(f"Chuẩn bị chèn {len(data)} hàng vào bảng '{table_name}'...")
        
        # Lấy các cột từ key của dictionary đầu tiên
        columns = data[0].keys()
        column_list = ", ".join(columns)
        placeholders = ", ".join(f":{col}" for col in columns)

        sql = f"INSERT OR REPLACE INTO \"{table_name}\" ({column_list}) VALUES ({placeholders});"
        
        try:
            cursor = self.conn.cursor()
            cursor.executemany(sql, data)
            self.conn.commit()
            logger.info(f"✅ Đã chèn thành công {cursor.rowcount} hàng vào '{table_name}'.")
        except sqlite3.Error as e:
            logger.error(f"Lỗi khi chèn hàng loạt vào '{table_name}': {e}")
            raise
    
    def connect(self):
        """Thiết lập kết nối đến database và bật kiểm tra khóa ngoại."""
        try:
            logger.info(f"Đang kết nối đến database: {self.db_path}")
            self.conn = sqlite3.connect(self.db_path)
            # --- THÊM DÒNG NÀY ĐỂ BẬT TÍNH NĂNG ---
            self.conn.execute("PRAGMA foreign_keys = ON;")
            logger.info("✅ Kết nối database thành công (đã bật foreign keys).")
        except sqlite3.Error as e:
            logger.error(f"Lỗi khi kết nối đến database: {e}")
            raise