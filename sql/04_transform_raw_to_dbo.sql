-- ============================================================================
-- RAW to DBO Transformation
-- ============================================================================
-- Transform raw JSON data into normalized, flattened dimensional model
-- Uses MERGE for idempotency - can be run multiple times safely

USE DATABASE RICK_MORTY_DB;

-- ============================================================================
-- 1. Transform Characters (RAW → DBO)
-- ============================================================================
-- Flatten nested objects: origin.name → origin_name, location.name → location_name

MERGE INTO DBO.dim_characters AS target
USING (
    SELECT 
        raw_data:id::INTEGER AS id,
        raw_data:name::VARCHAR AS name,
        raw_data:status::VARCHAR AS status,
        raw_data:species::VARCHAR AS species,
        raw_data:type::VARCHAR AS type,
        raw_data:gender::VARCHAR AS gender,
        raw_data:origin:name::VARCHAR AS origin_name,
        raw_data:origin:url::VARCHAR AS origin_url,
        raw_data:location:name::VARCHAR AS location_name,
        raw_data:location:url::VARCHAR AS location_url,
        raw_data:image::VARCHAR AS image,
        raw_data:url::VARCHAR AS url,
        raw_data:created::TIMESTAMP_NTZ AS created,
        CURRENT_TIMESTAMP() AS ingested_at
    FROM RAW.characters
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
    target.name = source.name,
    target.status = source.status,
    target.species = source.species,
    target.type = source.type,
    target.gender = source.gender,
    target.origin_name = source.origin_name,
    target.origin_url = source.origin_url,
    target.location_name = source.location_name,
    target.location_url = source.location_url,
    target.image = source.image,
    target.url = source.url,
    target.created = source.created,
    target.ingested_at = source.ingested_at
WHEN NOT MATCHED THEN INSERT (
    id, name, status, species, type, gender,
    origin_name, origin_url, location_name, location_url,
    image, url, created, ingested_at
) VALUES (
    source.id, source.name, source.status, source.species, source.type, source.gender,
    source.origin_name, source.origin_url, source.location_name, source.location_url,
    source.image, source.url, source.created, source.ingested_at
);

-- ============================================================================
-- 2. Transform Episodes (RAW → DBO)
-- ============================================================================

MERGE INTO DBO.dim_episodes AS target
USING (
    SELECT 
        raw_data:id::INTEGER AS id,
        raw_data:name::VARCHAR AS name,
        raw_data:episode::VARCHAR AS episode,
        raw_data:air_date::VARCHAR AS air_date,
        raw_data:url::VARCHAR AS url,
        raw_data:created::TIMESTAMP_NTZ AS created,
        CURRENT_TIMESTAMP() AS ingested_at
    FROM RAW.episodes
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
    target.name = source.name,
    target.episode = source.episode,
    target.air_date = source.air_date,
    target.url = source.url,
    target.created = source.created,
    target.ingested_at = source.ingested_at
WHEN NOT MATCHED THEN INSERT (
    id, name, episode, air_date, url, created, ingested_at
) VALUES (
    source.id, source.name, source.episode, source.air_date, 
    source.url, source.created, source.ingested_at
);

-- ============================================================================
-- 3. Transform Bridge Table (Explode Arrays)
-- ============================================================================
-- Explode character.episode[] array into one row per (character_id, episode_id)
-- Extract episode ID from URL: "https://rickandmortyapi.com/api/episode/1" → 1

MERGE INTO DBO.bridge_character_episodes AS target
USING (
    SELECT DISTINCT
        raw_data:id::INTEGER AS character_id,
        REGEXP_SUBSTR(episode_url.value::VARCHAR, '[0-9]+$')::INTEGER AS episode_id,
        CURRENT_TIMESTAMP() AS created_at
    FROM RAW.characters,
    LATERAL FLATTEN(input => raw_data:episode) AS episode_url
    WHERE episode_url.value IS NOT NULL
) AS source
ON target.character_id = source.character_id 
   AND target.episode_id = source.episode_id
WHEN NOT MATCHED THEN INSERT (
    character_id, episode_id, created_at
) VALUES (
    source.character_id, source.episode_id, source.created_at
);
