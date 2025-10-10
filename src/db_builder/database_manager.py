# Path: src/db_builder/database_manager.py
#!/usr/bin/env python3

import sqlite3
import logging
from pathlib import Path
from typing import Any, Dict, List # <--- THÊM DÒNG NÀY

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
            parent_uid TEXT,
            position INTEGER,
            type TEXT,
            pitaka_root TEXT,
            book_root TEXT,
            depth INTEGER, -- <--- THAY ĐỔI 1: THÊM CỘT MỚI
            prev_uid TEXT,
            next_uid TEXT
        );
        """
        self.create_table(sql)

    def insert_hierarchy_nodes(self, nodes: List[Dict[str, Any]]):
        """Chèn một danh sách các node vào bảng Hierarchy."""
        if not nodes:
            logger.warning("Không có node nào để chèn vào Hierarchy.")
            return

        logger.info(f"Chuẩn bị chèn/cập nhật {len(nodes)} hàng vào bảng Hierarchy...")
        
        # --- THAY ĐỔI 2: CẬP NHẬT CÂU LỆNH INSERT VÀ LOGIC CHUẨN BỊ DỮ LIỆU ---
        sql = """
        INSERT OR REPLACE INTO Hierarchy (uid, parent_uid, position, type, pitaka_root, book_root, depth, prev_uid, next_uid)
        VALUES (:uid, :parent_uid, :position, :type, :pitaka_root, :book_root, :depth, :prev_uid, :next_uid);
        """
        
        try:
            # Cập nhật thứ tự các cột để bao gồm 'depth'
            column_order = (
                'uid', 'parent_uid', 'position', 'type', 
                'pitaka_root', 'book_root', 'depth', 'prev_uid', 'next_uid'
            )
            
            # Tạo list of dicts với đầy đủ các key, đảm bảo thứ tự
            data_dicts = []
            for n in nodes:
                data_dicts.append({key: n.get(key) for key in column_order})

            cursor = self.conn.cursor()
            cursor.executemany(sql, data_dicts)
            self.conn.commit()
            logger.info(f"✅ Đã chèn/cập nhật thành công {cursor.rowcount} hàng.")
        except sqlite3.Error as e:
            logger.error(f"Lỗi khi chèn hàng loạt vào Hierarchy: {e}")
            raise