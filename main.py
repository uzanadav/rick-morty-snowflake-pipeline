import sys
import argparse
import logging
from pathlib import Path
import re

from src.config import LOG_LEVEL
from src.utils import setup_logging, print_summary
from src.ingestion import run_ingestion
from src.snowflake_dal import SnowflakeDAL
from src.raw_loader import run_raw_data_pipeline


logger = setup_logging(LOG_LEVEL)


def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Rick and Morty Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
                Examples:
                python main.py                            # Run full pipeline
                python main.py --step setup-snowflake     # Setup Snowflake database
                python main.py --step ingest              # Run only ingestion
                python main.py --step load-raw            # Load JSON into RAW tables
                python main.py --step setup-dbo           # Create DBO tables
                python main.py --step transform           # Run only transformation
                python main.py --step quality             # Run only quality checks
        """
    )
    
    parser.add_argument(
        '--step',
        choices=['setup-snowflake', 'ingest', 'load-raw', 'setup-dbo', 'transform', 'quality', 'all'],
        default='all',
        help='Pipeline step to execute (default: all)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    return parser.parse_args()


def run_snowflake_setup_step(dal: SnowflakeDAL):
    """
    Execute Snowflake database and schema setup.
    
    Args:
        dal: SnowflakeDAL instance
    
    Returns:
        True if successful
    """
    logger.info("STEP: Snowflake Database Setup")
    logger.info("-" * 60)
    
    try:
        # Test connection first
        logger.info("Testing Snowflake connection...")
        if not dal.test_connection():
            raise Exception("Snowflake connection test failed")
        
        # Execute setup SQL
        logger.info("\nExecuting database setup SQL...")
        dal.execute_file("sql/01_setup_database.sql")
        
        # Verify setup
        logger.info("\nVerifying setup...")
        result = dal.execute_query(
            "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()", 
            fetch=True
        )
        if result:
            db, schema, warehouse = result[0]
            logger.info(f"  ✓ Current Database: {db}")
            logger.info(f"  ✓ Current Schema: {schema}")
            logger.info(f"  ✓ Current Warehouse: {warehouse}")
        
        logger.info("\n✓ Snowflake setup completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"✗ Snowflake setup failed: {e}")
        raise


def run_ingestion_step():
    """
    Execute the ingestion step.
    
    Returns:
        Ingested data dictionary
    """
    logger.info("STEP: Data Ingestion")
    logger.info("-" * 60)
    
    try:
        data = run_ingestion()
        logger.info("✓ Ingestion step completed successfully")
        return data
    except Exception as e:
        logger.error(f"✗ Ingestion step failed: {e}")
        raise


def run_load_raw_step(dal: SnowflakeDAL):
    """
    Execute the load raw data step.
    
    Args:
        dal: SnowflakeDAL instance
    
    Returns:
        Load results dictionary
    """
    logger.info("STEP: Load RAW Data into snowflake")
    logger.info("-" * 60)
    
    try:
        results = run_raw_data_pipeline(dal)
        logger.info("✓ Load RAW step completed successfully")
        return results
    except Exception as e:
        logger.error(f"✗ Load RAW step failed: {e}")
        raise


def run_setup_dbo_step(dal: SnowflakeDAL):
    """
    Execute DBO (modeled) layer table setup.
    
    Args:
        dal: SnowflakeDAL instance
    
    Returns:
        True if successful
    """
    logger.info("STEP: Setup DBO Tables")
    logger.info("-" * 60)
    
    try:
        # Execute DBO tables DDL
        logger.info("Creating DBO dimension and bridge tables...")
        dal.execute_file("sql/03_dbo_tables.sql")
        
        # Verify tables were created
        logger.info("\nVerifying DBO tables...")
        tables = dal.execute_query("SHOW TABLES IN SCHEMA DBO", fetch=True)
        logger.info(f"  ✓ Created {len(tables)} tables:")
        for table in tables:
            logger.info(f"    - {table[1]}")
        
        logger.info("\n✓ DBO tables setup completed successfully")
        return True
    except Exception as e:
        logger.error(f"✗ DBO setup failed: {e}")
        raise


def run_transformation_step(dal: SnowflakeDAL):
    """
    Execute transformation from RAW to DBO.
    
    Args:
        dal: SnowflakeDAL instance
    
    Returns:
        Dictionary with transformation results
    """
    logger.info("STEP: Transform RAW → DBO")
    logger.info("-" * 60)
    
    try:
        # Execute transformation SQL (MERGE statements)
        logger.info("Executing transformation SQL...")
        dal.execute_file("sql/04_transform_raw_to_dbo.sql")
        
        # Verify results
        logger.info("\nVerifying transformation results...")
        char_count = dal.get_row_count("dim_characters", "DBO")
        ep_count = dal.get_row_count("dim_episodes", "DBO")
        bridge_count = dal.get_row_count("bridge_character_episodes", "DBO")
        
        logger.info(f"  ✓ Characters: {char_count}")
        logger.info(f"  ✓ Episodes: {ep_count}")
        logger.info(f"  ✓ Bridge records: {bridge_count}")
        logger.info("\n✓ Transformation completed successfully")
        
        return {
            "characters": char_count,
            "episodes": ep_count,
            "bridge_records": bridge_count
        }
    except Exception as e:
        logger.error(f"✗ Transformation failed: {e}")
        raise


def run_quality_checks_step(dal: SnowflakeDAL):
    """
    Execute data quality checks.
    
    Args:
        dal: SnowflakeDAL instance
    
    Returns:
        Quality check results dictionary
    """
    logger.info("STEP: Data Quality Checks")
    logger.info("-" * 60)
    
    try:
        from src.quality_checks import run_quality_checks
        
        results = run_quality_checks(dal)
        
        # Check if any tests failed
        if results['failed']:
            logger.error(f"\n✗ Quality checks failed: {len(results['failed'])} check(s) failed")
            raise Exception(f"Data quality validation failed: {len(results['failed'])} checks failed")
        
        logger.info("\n✓ Quality checks completed successfully")
        return results
    
    except Exception as e:
        logger.error(f"✗ Quality checks step failed: {e}")
        raise


def main():
    """
    Main pipeline orchestration.
    """
    args = parse_arguments()
    
    # Update log level if specified
    if args.log_level:
        logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("  RICK AND MORTY DATA PIPELINE")
    logger.info("=" * 60)
    logger.info(f"  Step: {args.step}")
    logger.info(f"  Log Level: {args.log_level}")
    logger.info("=" * 60)
    logger.info("")
    
    # Create DAL once for the entire pipeline
    dal = None
    
    try:
        
        # Initialize DAL for steps that need Snowflake connection
        needs_dal = args.step in ['setup-snowflake', 'load-raw', 'setup-dbo', 'transform', 'quality', 'all']
        if needs_dal:
            logger.info("Initializing Snowflake connection...")
            dal = SnowflakeDAL()
            logger.info("")
        
        results = {}
        
        # Execute requested steps
        if args.step in ['setup-snowflake', 'all']:
            results['snowflake_setup'] = run_snowflake_setup_step(dal)
        
        if args.step in ['ingest', 'all']:
            results['ingestion'] = run_ingestion_step()
        
        if args.step in ['load-raw', 'all']:
            results['load_raw'] = run_load_raw_step(dal)
        
        if args.step in ['setup-dbo', 'all']:
            results['setup_dbo'] = run_setup_dbo_step(dal)
        
        if args.step in ['transform', 'all']:
            results['transformation'] = run_transformation_step(dal)
        
        if args.step in ['quality', 'all']:
            results['quality'] = run_quality_checks_step(dal)
        
        # Final summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("  PIPELINE EXECUTION COMPLETE")
        logger.info("=" * 60)
        
        if 'ingestion' in results and results['ingestion']:
            data = results['ingestion']
            logger.info(f"  ✓ Characters ingested: {len(data.get('characters', []))}")
            logger.info(f"  ✓ Episodes ingested: {len(data.get('episodes', []))}")
        
        logger.info("  ✓ Pipeline completed successfully!")
        logger.info("=" * 60)
        logger.info("")
        
        return 0
    
    except KeyboardInterrupt:
        logger.warning("\n⚠ Pipeline interrupted by user")
        return 130
    
    except Exception as e:
        logger.error(f"\n✗ Pipeline failed with error: {e}", exc_info=True)
        return 1
    
    finally:
        # Always close DAL connection if it was created
        if dal is not None:
            try:
                dal.close()
                logger.info("✓ Snowflake connection closed")
            except Exception as e:
                logger.warning(f"Warning: Failed to close Snowflake connection: {e}")


if __name__ == "__main__":
    sys.exit(main())
