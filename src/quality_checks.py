"""
Data Quality Checks Module
Executes validation queries and reports results.
"""

from typing import Dict, List, Tuple
from src.snowflake_dal import SnowflakeDAL
from src.utils import setup_logging, get_timestamp
from src.config import LOG_LEVEL

logger = setup_logging(LOG_LEVEL)


def run_quality_checks(dal: SnowflakeDAL) -> Dict[str, any]:
    """
    Execute all data quality checks.
    
    Args:
        dal: SnowflakeDAL instance
    
    Returns:
        Dictionary with check results
    """
    logger.info("=" * 60)
    logger.info("Running Data Quality Checks...")
    logger.info("=" * 60)
    logger.info("")
    
    # Execute all checks from SQL file
    checks = execute_all_checks(dal)
    
    # Analyze results
    results = analyze_results(checks)
    
    # Print summary
    print_quality_summary(results)
    
    return results


def execute_all_checks(dal: SnowflakeDAL) -> List[Tuple]:
    """
    Execute all quality check queries from SQL file.
    
    Args:
        dal: SnowflakeDAL instance
    
    Returns:
        List of check results (flattened from all queries)
    """
    logger.info("Executing quality check queries...")
    
    # Use DAL method to execute all SELECT queries from file
    results_by_query = dal.execute_queries_from_file("sql/05_data_quality_checks.sql")
    
    # Flatten results from all queries into single list
    all_results = []
    for result_set in results_by_query:
        all_results.extend(result_set)
    
    logger.info("")
    return all_results


def analyze_results(checks: List[Tuple]) -> Dict[str, any]:
    """
    Analyze check results and categorize by status.
    
    Args:
        checks: List of check result tuples
    
    Returns:
        Dictionary with analysis
    """
    passed = []
    failed = []
    warnings = []
    
    for check in checks:
        check_name = check[0]
        status = check[-1]  # Last column is always status
        
        if status == 'PASS':
            passed.append(check)
        elif status == 'FAIL':
            failed.append(check)
        elif status == 'WARNING':
            warnings.append(check)
    
    return {
        'total': len(checks),
        'passed': passed,
        'failed': failed,
        'warnings': warnings,
        'success_rate': (len(passed) / len(checks) * 100) if checks else 0
    }


def print_quality_summary(results: Dict[str, any]) -> None:
    """
    Print formatted quality check summary.
    
    Args:
        results: Analysis results dictionary
    """
    logger.info("=" * 60)
    logger.info("DATA QUALITY SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total Checks: {results['total']}")
    logger.info(f"Passed: {len(results['passed'])} ✓")
    logger.info(f"Failed: {len(results['failed'])} ✗")
    logger.info(f"Warnings: {len(results['warnings'])} ⚠")
    logger.info(f"Success Rate: {results['success_rate']:.1f}%")
    logger.info("")
    
    # Show passed checks
    if results['passed']:
        logger.info("✓ PASSED CHECKS:")
        for check in results['passed']:
            check_name = check[0]
            logger.info(f"  ✓ {check_name}")
        logger.info("")
    
    # Show warnings
    if results['warnings']:
        logger.info("⚠ WARNINGS:")
        for check in results['warnings']:
            check_name = check[0]
            details = check[1:-1]  # Middle columns have details
            logger.warning(f"  ⚠ {check_name}: {details}")
        logger.info("")
    
    # Show failed checks
    if results['failed']:
        logger.info("✗ FAILED CHECKS:")
        for check in results['failed']:
            check_name = check[0]
            details = check[1:-1]  # Middle columns have details
            logger.error(f"  ✗ {check_name}: {details}")
        logger.info("")
    
    # Overall status
    if results['failed']:
        logger.error("=" * 60)
        logger.error("⚠ DATA QUALITY: FAILED")
        logger.error("=" * 60)
    elif results['warnings']:
        logger.warning("=" * 60)
        logger.warning("⚠ DATA QUALITY: PASSED WITH WARNINGS")
        logger.warning("=" * 60)
    else:
        logger.info("=" * 60)
        logger.info("✓ DATA QUALITY: ALL CHECKS PASSED")
        logger.info("=" * 60)
    
    logger.info("")


if __name__ == "__main__":
    """
    Run quality checks as standalone script.
    """
    import sys
    
    dal = None
    try:
        dal = SnowflakeDAL()
        results = run_quality_checks(dal)
        
        # Exit with error code if any checks failed
        if results['failed']:
            sys.exit(1)
        else:
            sys.exit(0)
    
    except Exception as e:
        logger.error(f"✗ Quality checks failed with error: {e}")
        sys.exit(1)
    
    finally:
        if dal:
            dal.close()
