# Path: src/db_builder/database_manager.py
#!/usr/bin/env python3

import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Quản lý các thao tác với database SQLite."""

    def __init__(self, db_path: Path):
        """
        Khởi tạo DatabaseManager.

        Args:
            db_path: Đường dẫn đầy đủ đến file database SQLite.
        """
        self.db_path = db_path
        self.conn = None
        self.db_path.parent.mkdir(parents=True, exist_ok=True) # Đảm bảo thư mục tồn tại

    def __enter__(self):
        """Hỗ trợ cho câu lệnh 'with', tự động kết nối."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Hỗ trợ cho câu lệnh 'with', tự động đóng kết nối."""
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

    def _execute_sql(self, sql: str, params=None):
        """Thực thi một câu lệnh SQL."""
        if not self.conn:
            raise sqlite3.Error("Chưa kết nối đến database.")
        try:
            cursor = self.conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            self.conn.commit()
            return cursor
        except sqlite3.Error as e:
            logger.error(f"Lỗi khi thực thi SQL: {sql}\n{e}")
            raise

    def create_table(self, create_table_sql: str):
        """Tạo một bảng từ một chuỗi SQL."""
        logger.info("Đang tiến hành tạo bảng...")
        self._execute_sql(create_table_sql)
        logger.info("Bảng đã được tạo hoặc đã tồn tại.")
        
    def create_hierarchy_table(self):
        """Tạo bảng Hierarchy chuyên dụng."""
        sql = """
        CREATE TABLE IF NOT EXISTS Hierarchy (
            uid TEXT PRIMARY KEY,
            parent_uid TEXT REFERENCES Hierarchy(uid),
            position INTEGER,
            title TEXT,
            node_type TEXT
        );
        """
        self.create_table(sql)