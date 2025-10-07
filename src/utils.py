"""
Utility functions for the Rick and Morty data pipeline.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Configure logging for the pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Configured logger instance
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def save_json_to_file(data: Any, file_path: Path) -> None:
    """
    Save data as JSON to a file.
    
    Args:
        data: Data to save (must be JSON serializable)
        file_path: Path to the output file
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def get_timestamp() -> str:
    """
    Get current timestamp as ISO format string.
    
    Returns:
        ISO formatted timestamp
    """
    return datetime.utcnow().isoformat()


def print_summary(title: str, stats: Dict[str, Any]) -> None:
    """
    Print a formatted summary of pipeline statistics.
    
    Args:
        title: Title for the summary
        stats: Dictionary of statistics to display
    """
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)
    
    for key, value in stats.items():
        print(f"  {key:.<40} {value}")
    
    print("=" * 60 + "\n")


