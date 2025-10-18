-- Path: src/db_builder/suttacentral_schema.sql

-- Bảng tra cứu mới
CREATE TABLE IF NOT EXISTS "Authors" (
    "author_uid" TEXT PRIMARY KEY,
    "author_name" TEXT,
    "author_short" TEXT
);

CREATE TABLE IF NOT EXISTS "Languages" (
    "lang_code" TEXT PRIMARY KEY,
    "lang_name" TEXT
);

-- Cập nhật bảng Suttaplex để loại bỏ các trường không cần thiết và thêm khóa ngoại
CREATE TABLE IF NOT EXISTS "Suttaplex" (
    "uid" TEXT PRIMARY KEY,
    "root_lang" TEXT,
    "acronym" TEXT,
    "translated_title" TEXT,
    "original_title" TEXT,
    "blurb" TEXT,
    "priority_author_uid" TEXT,
    FOREIGN KEY ("root_lang") REFERENCES "Languages" ("lang_code"),
    FOREIGN KEY ("priority_author_uid") REFERENCES "Authors" ("author_uid")
);

-- Bảng mới cho các bản dịch
CREATE TABLE IF NOT EXISTS "Translations" (
    "translation_uid" TEXT PRIMARY KEY,
    "sc_uid" TEXT, -- << ĐỔI TÊN TỪ sutta_uid
    "author_uid" TEXT,
    "lang" TEXT,
    "title" TEXT,
    "publication_date" TEXT,
    "segmented" INTEGER,
    "has_comment" INTEGER,
    "is_root" INTEGER,
    "file_path" TEXT,
    FOREIGN KEY ("sc_uid") REFERENCES "Suttaplex" ("uid"),
    FOREIGN KEY ("author_uid") REFERENCES "Authors" ("author_uid"),
    FOREIGN KEY ("lang") REFERENCES "Languages" ("lang_code")
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
    FOREIGN KEY ("uid") REFERENCES "Suttaplex" ("uid")
);

-- Các bảng khác không thay đổi
CREATE TABLE IF NOT EXISTS "Sutta_References" (
    "uid" TEXT PRIMARY KEY,
    "volpages" TEXT,
    "alt_volpages" TEXT,
    "verseNo" TEXT,
    "biblio_uid" TEXT,
    FOREIGN KEY ("uid") REFERENCES "Suttaplex" ("uid"),
    FOREIGN KEY ("biblio_uid") REFERENCES "Bibliography" ("biblio_uid")
);

CREATE TABLE IF NOT EXISTS "Bibliography" (
    "biblio_uid" TEXT PRIMARY KEY,
    "citation_key" TEXT,
    "full_citation" TEXT
);

-- Bảng chứa nội dung chi tiết của từng segment từ dữ liệu Bilara
CREATE TABLE IF NOT EXISTS "Segments_bilara" (
    "sc_uid" TEXT NOT NULL,
    "segment" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "lang" TEXT NOT NULL,
    "author_alias" TEXT,
    "content" TEXT NOT NULL,
    PRIMARY KEY ("sc_uid", "segment", "type", "lang", "author_alias")
);
CREATE INDEX IF NOT EXISTS "idx_segments_bilara_on_sc_uid" ON "Segments_bilara" ("sc_uid");
CREATE INDEX IF NOT EXISTS "idx_segments_bilara_compound" ON "Segments_bilara" ("sc_uid", "type", "lang", "author_alias");

-- Bảng chứa nội dung chi tiết của từng segment từ dữ liệu Site
CREATE TABLE IF NOT EXISTS "Segments_site" (
    "sc_uid" TEXT NOT NULL,
    "segment" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "lang" TEXT NOT NULL,
    "author_alias" TEXT,
    "content" TEXT NOT NULL,
    PRIMARY KEY ("sc_uid", "segment", "type", "lang", "author_alias")
);
CREATE INDEX IF NOT EXISTS "idx_segments_site_on_sc_uid" ON "Segments_site" ("sc_uid");
CREATE INDEX IF NOT EXISTS "idx_segments_site_compound" ON "Segments_site" ("sc_uid", "type", "lang", "author_alias");