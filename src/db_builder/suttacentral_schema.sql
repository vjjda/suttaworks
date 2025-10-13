-- Path: src/db_builder/suttacentral_schema.sql

-- Bảng Suttaplex: Lưu siêu dữ liệu chính, giờ là bảng "trung tâm"
CREATE TABLE IF NOT EXISTS "Suttaplex" (
    "uid" TEXT PRIMARY KEY,
    "root_lang" TEXT,
    "acronym" TEXT,
    "translated_title" TEXT,
    "original_title" TEXT,
    "blurb" TEXT
    -- FOREIGN KEY ĐÃ ĐƯỢC XÓA BỎ
);

-- Bảng Hierarchy: Lưu trữ cấu trúc, tham chiếu đến Suttaplex
CREATE TABLE IF NOT EXISTS "Hierarchy" (
    "uid" TEXT PRIMARY KEY,
    "parent_uid" TEXT,
    "type" TEXT,
    "pitaka_root" TEXT,
    "book_root" TEXT,
    "pitaka_depth" INTEGER,
    "book_depth" INTEGER,
    "sibling_position" INTEGER,
    "depth_position" INTEGER,
    "global_position" INTEGER,
    "prev_uid" TEXT,
    "next_uid" TEXT,
    -- --- THÊM FOREIGN KEY VÀO ĐÂY ---
    FOREIGN KEY ("uid") REFERENCES "Suttaplex" ("uid")
);

-- (Các bảng khác không thay đổi)
CREATE TABLE IF NOT EXISTS "Sutta_References" (
    "uid" TEXT PRIMARY KEY,
    "volpages" TEXT,
    "alt_volpages" TEXT,
    "verseNo" TEXT,
    "biblio_uid" TEXT,
    FOREIGN KEY ("uid") REFERENCES "Suttaplex" ("uid") -- <-- Sửa tham chiếu đến Suttaplex
);

CREATE TABLE IF NOT EXISTS "Bibliography" (
    "biblio_uid" TEXT PRIMARY KEY,
    "citation_key" TEXT,
    "full_citation" TEXT
);