USE DATABASE RICK_MORTY_DB;

USE SCHEMA DBO;

DROP TABLE IF EXISTS DBO.dim_characters CASCADE;

CREATE TABLE DBO.dim_characters (
    id INTEGER NOT NULL,
    name VARCHAR(500) NOT NULL,
    status VARCHAR(50),
    species VARCHAR(100),
    type VARCHAR(100),
    gender VARCHAR(50),
    origin_name VARCHAR(500),
    origin_url VARCHAR(500),
    location_name VARCHAR(500),
    location_url VARCHAR(500),
    image VARCHAR(500),
    url VARCHAR(500),
    created TIMESTAMP_NTZ,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT dim_characters_pk PRIMARY KEY (id)
);

DROP TABLE IF EXISTS DBO.dim_episodes CASCADE;

CREATE TABLE DBO.dim_episodes (
    id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    episode VARCHAR(50) NOT NULL,
    air_date VARCHAR(50),
    url VARCHAR(500),
    created TIMESTAMP_NTZ,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT dim_episodes_pk PRIMARY KEY (id)
);

DROP TABLE IF EXISTS DBO.bridge_character_episodes;

CREATE TABLE DBO.bridge_character_episodes (
    character_id INTEGER NOT NULL,
    episode_id INTEGER NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT bridge_character_episodes_pk PRIMARY KEY (character_id, episode_id),
    CONSTRAINT fk_character 
        FOREIGN KEY (character_id) 
        REFERENCES dim_characters(id),
    CONSTRAINT fk_episode 
        FOREIGN KEY (episode_id) 
        REFERENCES dim_episodes(id)
);