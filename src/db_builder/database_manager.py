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
    # Helper method để tránh lặp code
    def _execute_many(self, sql: str, data: List[Dict[str, Any]], table_name: str):
        try:
            cursor = self.conn.cursor()
            cursor.executemany(sql, data)
            self.conn.commit()
            logger.info(f"✅ Đã chèn thành công {cursor.rowcount} hàng vào {table_name}.")
        except sqlite3.Error as e:
            logger.error(f"Lỗi khi chèn hàng loạt vào {table_name}: {e}")
            raise

    def create_table(self, create_table_sql: str):
        """Tạo một bảng từ một chuỗi SQL."""
        logger.info("Đang tiến hành tạo bảng...")
        self._execute_sql(create_table_sql)
        logger.info("Bảng đã được tạo hoặc đã tồn tại.")
    
    def create_bibliography_table(self):
        """Tạo bảng Bibliography."""
        sql = """
        CREATE TABLE IF NOT EXISTS Bibliography (
            uid TEXT PRIMARY KEY,
            name TEXT,
            text TEXT
        );
        """
        self.create_table(sql)

    def insert_bibliography_data(self, biblio_data: List[Dict[str, Any]]):
        """Chèn dữ liệu vào bảng Bibliography."""
        if not biblio_data: return
        logger.info(f"Chuẩn bị chèn {len(biblio_data)} hàng vào bảng Bibliography...")
        sql = """
        INSERT OR REPLACE INTO Bibliography (uid, name, text)
        VALUES (:uid, :name, :text);
        """
        self._execute_many(sql, biblio_data, "Bibliography")
        
    def create_hierarchy_table(self):
        """Tạo bảng Hierarchy với pitaka_depth và book_depth."""
        sql = """
        CREATE TABLE IF NOT EXISTS Hierarchy (
            uid TEXT PRIMARY KEY,
            parent_uid TEXT,
            type TEXT,
            pitaka_root TEXT,
            book_root TEXT,
            pitaka_depth INTEGER,   -- <--- THAY ĐỔI
            book_depth INTEGER,     -- <--- THAY ĐỔI
            sibling_position INTEGER,
            depth_position INTEGER,
            global_position INTEGER,
            prev_uid TEXT,
            next_uid TEXT
        );
        """
        self.create_table(sql)

    def insert_hierarchy_nodes(self, nodes: List[Dict[str, Any]]):
        """Chèn một danh sách các node vào bảng Hierarchy."""
        if not nodes:
            return

        logger.info(f"Chuẩn bị chèn/cập nhật {len(nodes)} hàng vào bảng Hierarchy...")
        
        sql = """
        INSERT OR REPLACE INTO Hierarchy (
            uid, parent_uid, type, pitaka_root, book_root, 
            pitaka_depth, book_depth, sibling_position, depth_position, 
            global_position, prev_uid, next_uid
        )
        VALUES (
            :uid, :parent_uid, :type, :pitaka_root, :book_root, 
            :pitaka_depth, :book_depth, :sibling_position, :depth_position, 
            :global_position, :prev_uid, :next_uid
        );
        """
        
        try:
            # --- THAY ĐỔI: Cập nhật danh sách cột ---
            column_order = (
                'uid', 'parent_uid', 'type', 'pitaka_root', 'book_root', 
                'pitaka_depth', 'book_depth', 'sibling_position', 'depth_position', 
                'global_position', 'prev_uid', 'next_uid'
            )
            
            data_dicts = [{key: n.get(key) for key in column_order} for n in nodes]

            cursor = self.conn.cursor()
            cursor.executemany(sql, data_dicts)
            self.conn.commit()
            logger.info(f"✅ Đã chèn/cập nhật thành công {cursor.rowcount} hàng.")
        except sqlite3.Error as e:
            logger.error(f"Lỗi khi chèn hàng loạt vào Hierarchy: {e}")
            raise
    
    def create_suttaplex_table(self):
        """Tạo bảng Suttaplex (đã bỏ cột difficulty)."""
        sql = """
        CREATE TABLE IF NOT EXISTS Suttaplex (
            uid TEXT PRIMARY KEY,
            root_lang TEXT,
            acronym TEXT,
            translated_title TEXT,
            original_title TEXT,
            blurb TEXT,
            FOREIGN KEY (uid) REFERENCES Hierarchy (uid)
        );
        """
        self.create_table(sql)


    def insert_suttaplex_data(self, suttaplex_data: List[Dict[str, Any]]):
        """Chèn dữ liệu vào bảng Suttaplex."""
        if not suttaplex_data: return
        logger.info(f"Chuẩn bị chèn {len(suttaplex_data)} hàng vào bảng Suttaplex...")
        sql = """
        INSERT OR REPLACE INTO Suttaplex (uid, root_lang, acronym, translated_title, original_title, blurb)
        VALUES (:uid, :root_lang, :acronym, :translated_title, :original_title, :blurb);
        """
        self._execute_many(sql, suttaplex_data, "Suttaplex")
        
    def create_references_table(self):
        """Tạo bảng References."""
        # --- THAY ĐỔI: Chuyển biblio_uid xuống cuối ---
        sql = """
        CREATE TABLE IF NOT EXISTS "References" (
            uid TEXT PRIMARY KEY,
            volpages TEXT,
            alt_volpages TEXT,
            verseNo TEXT,
            biblio_uid TEXT,
            FOREIGN KEY (uid) REFERENCES Hierarchy (uid)
        );
        """
        self.create_table(sql)

    def insert_references_data(self, references_data: List[Dict[str, Any]]):
        """Chèn dữ liệu vào bảng References."""
        if not references_data: return
        logger.info(f"Chuẩn bị chèn {len(references_data)} hàng vào bảng References...")
        # --- THAY ĐỔI: Chuyển biblio_uid xuống cuối ---
        sql = """
        INSERT OR REPLACE INTO "References" (uid, volpages, alt_volpages, verseNo, biblio_uid)
        VALUES (:uid, :volpages, :alt_volpages, :verseNo, :biblio_uid);
        """
        self._execute_many(sql, references_data, "References")