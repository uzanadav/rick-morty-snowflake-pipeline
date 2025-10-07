"""
Raw data loader - Load JSON files into Snowflake RAW tables.
"""

import logging
from pathlib import Path
from typing import Dict, List
from glob import glob

from .snowflake_dal import SnowflakeDAL
from .config import RAW_DATA_PATH, RAW_SCHEMA
from .utils import setup_logging, print_summary


logger = setup_logging()


def setup_raw_tables(dal: SnowflakeDAL) -> bool:
    """
    Execute DDL to create raw tables, stage, and file format.
    
    Args:
        dal: SnowflakeDAL instance
    
    Returns:
        True if successful
    """
    logger.info("=" * 60)
    logger.info("Setting up RAW tables...")
    logger.info("=" * 60)
    
    try:
        dal.execute_file("sql/02_raw_tables.sql")
        logger.info("✓ RAW tables, stage, and file format created successfully")
        return True
    except Exception as e:
        logger.error(f"✗ Failed to create RAW tables: {e}")
        raise


def load_raw_entity(
    dal: SnowflakeDAL,
    entity_name: str,
    raw_data_path: Path,
    target_table: str,
    stage_name: str
) -> int:
    """
    Load a single entity's JSON file into Snowflake using PUT + COPY INTO.
    
    Args:
        dal: SnowflakeDAL instance
        entity_name: Name of the entity (characters/episodes)
        raw_data_path: Path to the entity's data directory
        target_table: Target table name
        stage_name: Snowflake stage name
    
    Returns:
        Number of rows loaded
    """
    # Find the latest JSON file
    pattern = f"{entity_name}_*.json"
    files = sorted(
        glob(str(raw_data_path / pattern)),
        key=lambda x: Path(x).stat().st_mtime,
        reverse=True
    )
    
    if not files:
        raise FileNotFoundError(f"No {entity_name} files found in {raw_data_path}")
    
    latest_file = files[0]
    logger.info(f"Loading {Path(latest_file).name}")
    
    # Upload file to stage
    dal.upload_file_to_stage(latest_file, stage_name)
    
    # Flatten JSON array and load into table
    rows_loaded = dal.copy_into_from_stage(
        table_name=target_table,
        stage_name=stage_name,
        file_pattern=Path(latest_file).name,
        flatten_json_array=True
    )
    
    logger.info(f"✓ Loaded {rows_loaded} rows into {target_table}")
    return rows_loaded


def load_raw_data(dal: SnowflakeDAL) -> Dict[str, int]:
    """
    Load JSON files into RAW tables.
    
    Args:
        dal: SnowflakeDAL instance
    
    Returns:
        Dictionary with row counts for each table
    """
    logger.info("=" * 60)
    logger.info("Loading data into RAW tables...")
    logger.info("=" * 60)
    logger.info("")
    
    # Load characters
    logger.info("Loading characters...")
    characters_count = load_raw_entity(
        dal,
        entity_name="characters",
        raw_data_path=RAW_DATA_PATH / "characters",
        target_table=f"{RAW_SCHEMA}.characters",
        stage_name="@raw_data_stage"
    )
    logger.info("")
    
    # Load episodes
    logger.info("Loading episodes...")
    episodes_count = load_raw_entity(
        dal,
        entity_name="episodes",
        raw_data_path=RAW_DATA_PATH / "episodes",
        target_table=f"{RAW_SCHEMA}.episodes",
        stage_name="@raw_data_stage"
    )
    logger.info("")
    
    return {
        'characters': characters_count,
        'episodes': episodes_count
    }


def verify_raw_data(dal: SnowflakeDAL) -> Dict[str, int]:
    """
    Verify data was loaded correctly.
    
    Args:
        dal: SnowflakeDAL instance
    
    Returns:
        Dictionary with verification results
    """
    logger.info("=" * 60)
    logger.info("Verifying RAW data...")
    logger.info("=" * 60)
    
    try:
        # Check row counts
        char_count = dal.get_row_count("characters", RAW_SCHEMA)
        ep_count = dal.get_row_count("episodes", RAW_SCHEMA)
        
        logger.info(f"  Characters in characters: {char_count}")
        logger.info(f"  Episodes in episodes: {ep_count}")
        logger.info("")
        
        # Sample data
        logger.info("Sample character data:")
        char_sample = dal.execute_query(
            f"""
            SELECT 
                id,
                raw_data:name::string as name,
                raw_data:species::string as species,
                raw_data:status::string as status
            FROM {RAW_SCHEMA}.characters 
            LIMIT 5
            """,
            fetch=True
        )
        
        for row in char_sample:
            logger.info(f"  ID: {row[0]}, Name: {row[1]}, Species: {row[2]}, Status: {row[3]}")
        
        logger.info("")
        logger.info("Sample episode data:")
        ep_sample = dal.execute_query(
            f"""
            SELECT 
                id,
                raw_data:name::string as name,
                raw_data:episode::string as episode_code,
                raw_data:air_date::string as air_date
            FROM {RAW_SCHEMA}.episodes 
            LIMIT 5
            """,
            fetch=True
        )
        
        for row in ep_sample:
            logger.info(f"  ID: {row[0]}, Name: {row[1]}, Episode: {row[2]}, Air Date: {row[3]}")
        
        logger.info("")
        logger.info("✓ Verification complete")
        
        return {
            "characters_count": char_count,
            "episodes_count": ep_count
        }
    
    except Exception as e:
        logger.error(f"✗ Verification failed: {e}")
        raise


def run_raw_data_pipeline(dal: SnowflakeDAL):
    """
    Complete pipeline: Setup tables and load raw data.
    
    Args:
        dal: SnowflakeDAL instance
    
    Returns:
        Dictionary with pipeline results
    """
    logger.info("\n🚀 Starting RAW Data Pipeline")
    logger.info("=" * 60)
    
    # Step 1: Setup tables
    setup_raw_tables(dal)
    
    # Step 2: Load data
    load_results = load_raw_data(dal)
    
    # Step 3: Verify
    verify_results = verify_raw_data(dal)
    
    # Summary
    print_summary("RAW Data Pipeline Summary", {
        "Characters Loaded": load_results['characters'],
        "Episodes Loaded": load_results['episodes'],
        "Characters in Table": verify_results['characters_count'],
        "Episodes in Table": verify_results['episodes_count'],
        "Status": "✓ SUCCESS"
    })
    
    return {
        "loaded": load_results,
        "verified": verify_results
    }


if __name__ == "__main__":
    """
    Run raw data pipeline as standalone script.
    """
    import sys
    
    dal = None
    try:
        dal = SnowflakeDAL()
        result = run_raw_data_pipeline(dal)
        logger.info("✓ RAW data pipeline completed successfully!")
        sys.exit(0)
    except Exception as e:
        logger.error(f"✗ RAW data pipeline failed: {e}")
        sys.exit(1)
    finally:
        if dal:
            dal.close()
