# Rick and Morty Data Pipeline

A data pipeline that ingests Rick and Morty API data into Snowflake with full data quality validation.

## Quick Start

```bash
# 1. Setup
./scripts/setup.sh

# 2. Configure (edit with your Snowflake credentials)
cp .env.example .env
vim .env

# 3. Run
python main.py --step all
```

That's it! The pipeline will:
- Ingest 826 characters + 51 episodes from the API
- Load into Snowflake RAW tables (JSON)
- Transform to normalized DBO tables (flattened)
- Run 13 quality checks (100% pass rate)

Runtime: ~45 seconds

---

## Architecture

```
API → Ingestion → RAW (JSON) → Transform → DBO (Normalized) → Quality Checks
```

**3-Layer Design:**
- **RAW**: Immutable JSON storage (`VARIANT` type)
- **DBO**: Flattened dimensional model (normalized tables)

**Key Transformations:**
- Nested objects → columns: `origin.name` → `origin_name`
- Arrays → rows: `episode[]` → bridge table (1,267 records)

---

## Schema

### RAW Layer
```sql
-- Individual entities stored as JSON
RAW.characters (id, raw_data, ingested_at, source_file)
RAW.episodes   (id, raw_data, ingested_at, source_file)
```

### DBO Layer
```sql
-- Flattened dimensions
DBO.dim_characters (
    id, name, status, species, gender,
    origin_name, origin_url,      -- Nested object flattened
    location_name, location_url   -- Nested object flattened
)

DBO.dim_episodes (
    id, name, episode, air_date
)

-- Many-to-many bridge
DBO.bridge_character_episodes (
    character_id, episode_id
)
```

**Flattening Example:**
```json
// API Response
{
  "id": 1,
  "name": "Rick Sanchez",
  "origin": {
    "name": "Earth (C-137)",
    "url": "https://..."
  },
  "episode": [
    "https://.../episode/1",
    "https://.../episode/2"
  ]
}

// Transforms to:
// dim_characters: 1 row with origin_name, origin_url columns
// bridge_character_episodes: 2 rows (character_id=1, episode_id=1/2)
```

---

## Pipeline Steps

Run individually or all together:

```bash
python main.py --step all              # Full pipeline

# Or individual steps:
python main.py --step setup-snowflake  # Create DB/schemas/tables
python main.py --step ingest           # Fetch from API
python main.py --step load-raw         # Load to RAW tables
python main.py --step setup-dbo        # Create DBO tables
python main.py --step transform        # RAW → DBO (MERGE)
python main.py --step quality          # Run 13 validation checks
```

---

## Data Quality Checks

13 automated checks using Snowflake's `QUALIFY` for performance:

**Primary Key Uniqueness:**
- No duplicate character IDs
- No duplicate episode IDs
- No duplicate bridge pairs

**NOT NULL Constraints:**
- All required fields populated

**Referential Integrity:**
- All foreign keys valid
- No orphaned records

**Data Completeness:**
- RAW row counts match DBO
- All characters appear in episodes

**Result:** 100% pass rate (13/13)

---

## Idempotency

All transformations use `MERGE` statements (upsert):
- Safe to re-run multiple times
- Updates existing records
- Inserts new records
- No duplicates created

**File Management:**
- Latest JSON kept on disk (old files auto-deleted)
- All data persisted in Snowflake

---

## Configuration

Edit `.env` with your credentials:

```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RICK_MORTY_DB
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

---

## Project Structure

```
├── main.py                         # Pipeline orchestration
├── requirements.txt                # Dependencies
│
├── scripts/
│   ├── setup.sh                    # Environment setup
│   └── run.sh                      # Quick run
│
├── src/
│   ├── config.py                   # Configuration
│   ├── utils.py                    # Helpers (logging, timestamps, summaries)
│   ├── ingestion.py                # API fetching (pagination, retries)
│   ├── snowflake_dal.py            # Database connection manager
│   ├── raw_loader.py               # RAW layer loader (PUT/COPY INTO)
│   └── quality_checks.py           # Validation runner
│
└── sql/
    ├── 01_setup_database.sql       # DB and schemas
    ├── 02_raw_tables.sql           # RAW tables (VARIANT)
    ├── 03_dbo_tables.sql           # DBO tables (normalized)
    ├── 04_transform_raw_to_dbo.sql # MERGE statements
    └── 05_data_quality_checks.sql  # Validation queries (QUALIFY)
```

---

## Technical Highlights

**API Ingestion:**
- Automatic pagination
- Exponential backoff retry (`tenacity` library)
- Rate limiting

**Snowflake Optimization:**
- `VARIANT` for efficient JSON storage
- `MERGE` for idempotent upserts
- `LATERAL FLATTEN` for array explosion
- `QUALIFY` for duplicate detection
- Internal stages for bulk loading

**Data Quality:**
- Snowflake-optimized `QUALIFY` checks
- Foreign key enforcement
- Shows actual duplicate IDs when failures occur

---

## Results

**Data Volumes:**
- 826 Characters
- 51 Episodes
- 1,267 Character-Episode relationships

**Quality:**
- 13/13 checks passed (100%)
- 0 duplicates
- 0 NULL violations
- 0 orphaned foreign keys

**Performance:**
- Full pipeline: ~45 seconds
- Ingestion: ~8 seconds
- Quality checks: ~10 seconds

---

## Troubleshooting

**Test Snowflake connection:**
```bash
python -c "from src.snowflake_dal import SnowflakeDAL; dal = SnowflakeDAL(); dal.test_connection(); dal.close()"
```

**Check table counts:**
```bash
python -c "from src.snowflake_dal import SnowflakeDAL; dal = SnowflakeDAL(); print(dal.execute_query('SELECT COUNT(*) FROM RAW.characters', fetch=True)); dal.close()"
```

**Reset everything:**
```sql
-- In Snowflake:
DROP DATABASE IF EXISTS RICK_MORTY_DB CASCADE;

-- Then re-run:
python main.py --step all
```

---

## Requirements Met

✅ All pages ingested (826 characters, 51 episodes)  
✅ Tables fully flattened (nested objects → columns, arrays → rows)  
✅ Idempotent and repeatable (MERGE statements)  
✅ Bridge table correct (1,267 many-to-many relationships)  
✅ Primary keys, NOT NULL, foreign keys enforced  
✅ 13 quality checks (100% pass rate)  
✅ Single command execution  

---

## License

MIT - Data Engineering Assessment Project