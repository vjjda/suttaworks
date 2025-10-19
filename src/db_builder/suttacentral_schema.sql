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

-- 1. Bảng Bilara_names (đã bỏ is_root)
CREATE TABLE IF NOT EXISTS "Bilara_names" (
    "sc_uid" TEXT NOT NULL,
    "lang" TEXT NOT NULL,
    "content" TEXT NOT NULL,
    PRIMARY KEY ("sc_uid", "lang")
);

-- 2. Bảng Bilara_blurbs (đã bỏ is_root)
CREATE TABLE IF NOT EXISTS "Bilara_blurbs" (
    "sc_uid" TEXT NOT NULL,
    "lang" TEXT NOT NULL,
    "content" TEXT NOT NULL,
    PRIMARY KEY ("sc_uid", "lang")
);

-- 3. Bảng Bilara_sites
CREATE TABLE IF NOT EXISTS "Bilara_sites" (
    "sc_uid" TEXT NOT NULL,
    "segment" TEXT NOT NULL,
    "lang" TEXT NOT NULL,
    "content" TEXT NOT NULL,
    PRIMARY KEY ("sc_uid", "segment", "lang")
);

-- 4. Bảng Bilara_segments (tạm thời giữ nguyên)
CREATE TABLE IF NOT EXISTS "Bilara_segments" (
    "sc_uid" TEXT NOT NULL,
    "segment" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "lang" TEXT NOT NULL,
    "author_alias" TEXT,
    "content" TEXT NOT NULL,
    PRIMARY KEY ("sc_uid", "segment", "type", "lang", "author_alias")
);

CREATE INDEX IF NOT EXISTS idx_bilara_main_query
ON Bilara_segments (type, lang, sc_uid, segment, author_alias);

CREATE INDEX IF NOT EXISTS idx_bilara_segments_sc_uid 
ON Bilara_segments (sc_uid, segment);

-- 1. VIEW cho type = 'html'
DROP VIEW IF EXISTS V_HtmlSegments;
CREATE VIEW V_HtmlSegments AS
SELECT rowid, sc_uid, segment, lang, author_alias, content
FROM Bilara_segments
WHERE type = 'html'
ORDER BY rowid;

-- 2. VIEW cho type = 'reference'
DROP VIEW IF EXISTS V_ReferenceSegments;
CREATE VIEW V_ReferenceSegments AS
SELECT rowid, sc_uid, segment, lang, author_alias, content
FROM Bilara_segments
WHERE type = 'reference'
ORDER BY rowid;

-- 3. VIEW cho type = 'root'
DROP VIEW IF EXISTS V_RootSegments;
CREATE VIEW V_RootSegments AS
SELECT rowid, sc_uid, segment, lang, author_alias, content
FROM Bilara_segments
WHERE type = 'root'
ORDER BY rowid;

-- 4. VIEW cho type = 'translation'
DROP VIEW IF EXISTS V_TranslationSegments;
CREATE VIEW V_TranslationSegments AS
SELECT rowid, sc_uid, segment, lang, author_alias, content
FROM Bilara_segments
WHERE type = 'translation'
ORDER BY rowid;


-- 5. VIEW cho type = 'variant'
DROP VIEW IF EXISTS V_VariantSegments;
CREATE VIEW V_VariantSegments AS
SELECT rowid, sc_uid, segment, lang, author_alias, content
FROM Bilara_segments
WHERE type = 'variant'
ORDER BY rowid;


-- 6. VIEW cho type = 'comment'
DROP VIEW IF EXISTS V_CommentSegments;
CREATE VIEW V_CommentSegments AS
SELECT rowid, sc_uid, segment, lang, author_alias, content
FROM Bilara_segments
WHERE type = 'comment'
ORDER BY rowid;


DROP VIEW IF EXISTS V_SitesEN;
CREATE VIEW V_SitesEN AS
SELECT rowid, sc_uid, segment, content
FROM Bilara_sites
WHERE lang = 'en'
ORDER BY rowid;


DROP VIEW IF EXISTS V_SitesOthers;
CREATE VIEW V_SitesOthers AS
SELECT rowid, sc_uid, segment, lang, content
FROM Bilara_sites
WHERE lang != 'en'
ORDER BY rowid;
