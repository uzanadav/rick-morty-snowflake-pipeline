

USE DATABASE RICK_MORTY_DB;

USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT json_format
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    STRIP_OUTER_ARRAY = FALSE;

CREATE OR REPLACE STAGE raw_data_stage
    FILE_FORMAT = json_format
    COMMENT = 'Internal stage for loading raw JSON files';

DROP TABLE IF EXISTS RAW.characters;
CREATE TABLE characters (
    id INTEGER NOT NULL,
    raw_data VARIANT NOT NULL,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR(255),
    CONSTRAINT characters_pk PRIMARY KEY (id)
);

DROP TABLE IF EXISTS RAW.episodes;
CREATE TABLE episodes (
    id INTEGER NOT NULL,
    raw_data VARIANT NOT NULL,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR(255),
    CONSTRAINT episodes_pk PRIMARY KEY (id)
);

