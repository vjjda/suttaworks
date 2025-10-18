-- Path: src/db_builder/suttacentral_schema_bilara_template.sql

CREATE TABLE IF NOT EXISTS "{table_name}" (
    "sc_uid" TEXT NOT NULL,
    "segment" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "lang" TEXT NOT NULL,
    "author_alias" TEXT,
    "content" TEXT NOT NULL,
    PRIMARY KEY ("sc_uid", "segment", "type", "lang", "author_alias")
);

CREATE INDEX IF NOT EXISTS "idx_{table_name}_compound" 
ON "{table_name}" ("sc_uid", "type", "lang", "author_alias");