"""
Configuration management for the Rick and Morty data pipeline.
Loads environment variables and provides configuration constants.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
RAW_DATA_PATH = Path(os.getenv("RAW_DATA_PATH", "./data/raw"))

# API Configuration
RICK_MORTY_API_BASE_URL = os.getenv("RICK_MORTY_API_BASE_URL", "https://rickandmortyapi.com/api")
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "30"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "5"))
API_RETRY_BACKOFF = int(os.getenv("API_RETRY_BACKOFF", "2"))

# API Endpoints
CHARACTERS_ENDPOINT = f"{RICK_MORTY_API_BASE_URL}/character"
EPISODES_ENDPOINT = f"{RICK_MORTY_API_BASE_URL}/episode"

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "RICK_MORTY_DB"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
}

# Snowflake Schemas
RAW_SCHEMA = "RAW"
DBO_SCHEMA = "DBO"

# Data Pipeline Settings
BATCH_SIZE = 20  # API returns 20 records per page
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


def validate_config():
    """
    Validate that required configuration is present.
    
    Returns:
        tuple: (is_valid, missing_keys)
    """
    required_keys = ["account", "user", "password", "warehouse"]
    missing = [key for key in required_keys if not SNOWFLAKE_CONFIG.get(key)]
    
    return len(missing) == 0, missing


def ensure_data_directories(): # todo check if need this 
    """
    Create necessary data directories if they don't exist.
    
    Note: This is optional - directories are auto-created when saving files.
    Kept for explicit initialization if needed.
    """
    RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
    (RAW_DATA_PATH / "characters").mkdir(exist_ok=True)
    (RAW_DATA_PATH / "episodes").mkdir(exist_ok=True)


if __name__ == "__main__":
    # Test configuration
    print("Configuration Test")
    print("=" * 50)
    print(f"API Base URL: {RICK_MORTY_API_BASE_URL}")
    print(f"Characters Endpoint: {CHARACTERS_ENDPOINT}")
    print(f"Episodes Endpoint: {EPISODES_ENDPOINT}")
    print(f"API Timeout: {API_TIMEOUT}s")
    print(f"Max Retries: {API_MAX_RETRIES}")
    print(f"Raw Data Path: {RAW_DATA_PATH}")
    print("\nSnowflake Configuration:")
    is_valid, missing = validate_config()
    if is_valid:
        print("✓ All required Snowflake configs present")
    else:
        print(f"✗ Missing Snowflake configs: {', '.join(missing)}")
