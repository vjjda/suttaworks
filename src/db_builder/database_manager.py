# Path: src/db_builder/database_manager.py
import sqlite3
import logging
from pathlib import Path
from typing import Any, Dict, List

from src.config.constants import PROJECT_ROOT

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Quản lý các thao tác với database SQLite, được tối ưu cho việc xây dựng hàng loạt."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = None
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def __enter__(self):
        """Khi bắt đầu khối 'with', kết nối và tối ưu hóa cho ghi hàng loạt."""
        try:
            logger.info(f"Đang kết nối đến database: {self.db_path}")
            self.conn = sqlite3.connect(self.db_path)
            
            # --- TỐI ƯU HÓA HIỆU NĂNG ---
            # 1. Tạm tắt kiểm tra khóa ngoại khi đang xây dựng DB
            self.conn.execute("PRAGMA foreign_keys = OFF;")
            # 2. Chế độ ghi WAL (Write-Ahead Logging) thường nhanh hơn
            self.conn.execute("PRAGMA journal_mode = WAL;")
            # 3. Tắt đồng bộ hóa (chỉ an toàn khi không có tiến trình nào khác truy cập)
            self.conn.execute("PRAGMA synchronous = OFF;")
            
            logger.info("✅ Kết nối database thành công và tối ưu cho ghi hàng loạt.")
            
        except sqlite3.Error as e:
            logger.error(f"Lỗi khi kết nối hoặc cấu hình database: {e}")
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Khi kết thúc khối 'with', commit nếu thành công, rollback nếu có lỗi,
        và khôi phục cài đặt an toàn.
        """
        if self.conn:
            try:
                if exc_type is None:
                    # Không có lỗi, commit toàn bộ giao dịch
                    logger.info("Không có lỗi xảy ra, đang commit toàn bộ các thay đổi...")
                    self.conn.commit()
                    logger.info("✅ Commit thành công.")
                else:
                    # Có lỗi, hủy bỏ tất cả thay đổi
                    logger.warning(f"Đã xảy ra lỗi: {exc_val}. Đang rollback...")
                    self.conn.rollback()
                    logger.warning("✅ Rollback thành công.")
            finally:
                # --- KHÔI PHỤC CÀI ĐẶT AN TOÀN ---
                # Luôn bật lại foreign keys trước khi đóng
                logger.info("Khôi phục cài đặt an toàn cho database...")
                self.conn.execute("PRAGMA foreign_keys = ON;")
                self.conn.close()
                logger.info("Đã đóng kết nối database.")

    def create_tables_from_schema(self):
        """Đọc file schema và thực thi để tạo tất cả các bảng."""
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
        Chèn hàng loạt dữ liệu. Việc commit sẽ được quản lý bởi context manager.
        """
        if not data:
            logger.info(f"Không có dữ liệu để chèn vào bảng '{table_name}'.")
            return

        logger.info(f"Chuẩn bị chèn {len(data)} hàng vào bảng '{table_name}'...")
        
        columns = data[0].keys()
        column_list = ", ".join(columns)
        placeholders = ", ".join(f":{col}" for col in columns)

        sql = f"INSERT OR REPLACE INTO \"{table_name}\" ({column_list}) VALUES ({placeholders});"
        
        try:
            cursor = self.conn.cursor()
            cursor.executemany(sql, data)
            # --- XÓA COMMIT Ở ĐÂY ---
            # self.conn.commit() <-- Việc này sẽ do __exit__ xử lý một lần duy nhất
            logger.info(f"✅ Đã chuẩn bị {cursor.rowcount} hàng để chèn vào '{table_name}'.")
        except sqlite3.Error as e:
            logger.error(f"Lỗi khi chèn hàng loạt vào '{table_name}': {e}")
            raise
    
    # Xóa hàm connect cũ vì logic đã được tích hợp vào __enter__