-- ============================================================================
-- Data Quality Checks for Rick and Morty Pipeline
-- ============================================================================
-- Validates primary keys, uniqueness, referential integrity, and NOT NULL constraints

USE DATABASE RICK_MORTY_DB;
USE SCHEMA DBO;

-- ============================================================================
-- 1. PRIMARY KEY UNIQUENESS - Characters
-- ============================================================================
-- Check: No duplicate character IDs using QUALIFY (Snowflake-optimized)
-- Expected: 0 duplicates

SELECT 
    'PK_CHARACTERS_UNIQUE' AS check_name,
    COUNT(*) AS duplicate_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (
    SELECT id
    FROM DBO.dim_characters
    QUALIFY COUNT(*) OVER (PARTITION BY id) > 1
);

-- ============================================================================
-- 2. PRIMARY KEY UNIQUENESS - Episodes
-- ============================================================================
-- Check: No duplicate episode IDs using QUALIFY (Snowflake-optimized)
-- Expected: 0 duplicates

SELECT 
    'PK_EPISODES_UNIQUE' AS check_name,
    COUNT(*) AS duplicate_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (
    SELECT id
    FROM DBO.dim_episodes
    QUALIFY COUNT(*) OVER (PARTITION BY id) > 1
);

-- ============================================================================
-- 3. PRIMARY KEY UNIQUENESS - Bridge Table
-- ============================================================================
-- Check: No duplicate (character_id, episode_id) pairs using QUALIFY
-- Expected: 0 duplicates

SELECT 
    'PK_BRIDGE_UNIQUE' AS check_name,
    COUNT(*) AS duplicate_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (
    SELECT character_id, episode_id
    FROM DBO.bridge_character_episodes
    QUALIFY COUNT(*) OVER (PARTITION BY character_id, episode_id) > 1
);

-- ============================================================================
-- 4. NOT NULL CHECKS - Characters
-- ============================================================================
-- Check: Required fields are not null
-- Expected: 0 null values

SELECT 
    'NOT_NULL_CHARACTERS_ID' AS check_name,
    COUNT(*) AS null_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM DBO.dim_characters
WHERE id IS NULL;

SELECT 
    'NOT_NULL_CHARACTERS_NAME' AS check_name,
    COUNT(*) AS null_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM DBO.dim_characters
WHERE name IS NULL;

-- ============================================================================
-- 5. NOT NULL CHECKS - Episodes
-- ============================================================================
-- Check: Required fields are not null
-- Expected: 0 null values

SELECT 
    'NOT_NULL_EPISODES_ID' AS check_name,
    COUNT(*) AS null_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM DBO.dim_episodes
WHERE id IS NULL;

SELECT 
    'NOT_NULL_EPISODES_NAME' AS check_name,
    COUNT(*) AS null_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM DBO.dim_episodes
WHERE name IS NULL;

SELECT 
    'NOT_NULL_EPISODES_EPISODE' AS check_name,
    COUNT(*) AS null_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM DBO.dim_episodes
WHERE episode IS NULL;

-- ============================================================================
-- 6. REFERENTIAL INTEGRITY - Bridge to Characters
-- ============================================================================
-- Check: All character_ids in bridge table exist in dim_characters
-- Expected: 0 orphaned records

SELECT 
    'FK_BRIDGE_TO_CHARACTERS' AS check_name,
    COUNT(*) AS orphaned_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM DBO.bridge_character_episodes b
WHERE NOT EXISTS (
    SELECT 1 
    FROM DBO.dim_characters c 
    WHERE c.id = b.character_id
);

-- ============================================================================
-- 7. REFERENTIAL INTEGRITY - Bridge to Episodes
-- ============================================================================
-- Check: All episode_ids in bridge table exist in dim_episodes
-- Expected: 0 orphaned records

SELECT 
    'FK_BRIDGE_TO_EPISODES' AS check_name,
    COUNT(*) AS orphaned_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM DBO.bridge_character_episodes b
WHERE NOT EXISTS (
    SELECT 1 
    FROM DBO.dim_episodes e 
    WHERE e.id = b.episode_id
);

-- ============================================================================
-- 8. DATA COMPLETENESS - RAW vs DBO
-- ============================================================================
-- Check: Row counts match between RAW and DBO layers
-- Expected: Exact match

SELECT 
    'ROW_COUNT_CHARACTERS' AS check_name,
    (SELECT COUNT(*) FROM RAW.characters) AS raw_count,
    (SELECT COUNT(*) FROM DBO.dim_characters) AS dbo_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM RAW.characters) = (SELECT COUNT(*) FROM DBO.dim_characters) THEN 'PASS'
        ELSE 'FAIL'
    END AS status;

SELECT 
    'ROW_COUNT_EPISODES' AS check_name,
    (SELECT COUNT(*) FROM RAW.episodes) AS raw_count,
    (SELECT COUNT(*) FROM DBO.dim_episodes) AS dbo_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM RAW.episodes) = (SELECT COUNT(*) FROM DBO.dim_episodes) THEN 'PASS'
        ELSE 'FAIL'
    END AS status;

-- ============================================================================
-- 9. BRIDGE TABLE INTEGRITY
-- ============================================================================
-- Check: Bridge table has reasonable relationships
-- Expected: Each character appears in at least 1 episode

SELECT 
    'BRIDGE_MIN_EPISODES_PER_CHARACTER' AS check_name,
    COUNT(*) AS characters_with_no_episodes,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'WARNING'
    END AS status
FROM DBO.dim_characters c
WHERE NOT EXISTS (
    SELECT 1 
    FROM DBO.bridge_character_episodes b 
    WHERE b.character_id = c.id
);
