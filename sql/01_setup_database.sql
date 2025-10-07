-- ============================================================================
-- 1. CREATE WAREHOUSE (if not exists)
-- ============================================================================
-- Warehouse is required to run queries in Snowflake
-- We use X-Small for this small dataset (cost-effective)

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = FALSE
    COMMENT = 'Warehouse for Rick and Morty data pipeline';

USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS RICK_MORTY_DB
    COMMENT = 'Rick and Morty API data - Characters and Episodes';

USE DATABASE RICK_MORTY_DB;

CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw zone - stores unprocessed JSON data from Rick and Morty API';

CREATE SCHEMA IF NOT EXISTS DBO
    COMMENT = 'DBO zone - normalized and flattened dimensional model';

