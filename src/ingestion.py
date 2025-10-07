"""
Data ingestion module for Rick and Morty API.
Handles API requests with pagination, retry logic, and exponential backoff.
"""

import json
import logging
import time
from typing import Dict, List, Optional, Any
from pathlib import Path
from glob import glob

import requests
from tenacity import (retry, stop_after_attempt, wait_exponential,retry_if_exception_type,before_sleep_log)

from .config import (CHARACTERS_ENDPOINT,EPISODES_ENDPOINT,API_TIMEOUT,API_MAX_RETRIES,API_RETRY_BACKOFF,RAW_DATA_PATH)
from .utils import setup_logging, save_json_to_file, get_timestamp, print_summary


logger = setup_logging()


class APIIngestionError(Exception): #todo: not sure we need it
    """Custom exception for API ingestion errors."""
    pass


def cleanup_old_files(directory: Path, pattern: str, keep_latest: int = 1):
    """
    Remove old files from directory, keeping only the most recent ones.
    
    Args:
        directory: Directory to clean
        pattern: File pattern to match (e.g., 'characters_*.json')
        keep_latest: Number of latest files to keep
    """
    files = sorted(
        glob(str(directory / pattern)),
        key=lambda x: Path(x).stat().st_mtime,
        reverse=True
    )
    
    # Delete all but the latest N files
    files_to_delete = files[keep_latest:]
    
    for file_path in files_to_delete:
        try:
            Path(file_path).unlink()
            logger.info(f"  Deleted old file: {Path(file_path).name}")
        except Exception as e:
            logger.warning(f"  Failed to delete {file_path}: {e}")


class RickMortyAPIClient:
    """
    Client for interacting with the Rick and Morty API.
    Implements pagination, retry logic with exponential backoff.
    """
    
    def __init__(self, timeout: int = API_TIMEOUT, max_retries: int = API_MAX_RETRIES):
        """
        Initialize the API client.
        
        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'RickMorty-DataPipeline/1.0',
            'Accept': 'application/json'
        })
    
    @retry(
        stop=stop_after_attempt(API_MAX_RETRIES),
        wait=wait_exponential(multiplier=API_RETRY_BACKOFF, min=1, max=30),
        retry=retry_if_exception_type((
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError
        )),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def _fetch_page(self, url: str) -> Dict[str, Any]:
        """
        Fetch a single page from the API with retry logic.
        
        Args:
            url: API endpoint URL
        
        Returns:
            JSON response as dictionary
        
        Raises:
            APIIngestionError: If request fails after all retries
        """
        try:
            logger.debug(f"Fetching URL: {url}")
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            return response.json()
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code >= 500:
                # Retry on 5xx errors
                logger.warning(f"Server error (5xx): {e}. Retrying...")
                raise
            else:
                # Don't retry on 4xx errors (client errors)
                logger.error(f"Client error (4xx): {e}")
                raise APIIngestionError(f"HTTP error: {e}")
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise APIIngestionError(f"Failed to fetch data from {url}: {e}")
    
    def fetch_all_pages(self, endpoint: str) -> List[Dict[str, Any]]:
        """
        Fetch all pages from a paginated API endpoint.
        
        Args:
            endpoint: API endpoint URL (e.g., character or episode)
        
        Returns:
            List of all records across all pages
        """
        all_results = []
        current_url = endpoint
        page_number = 1
        
        logger.info(f"Starting pagination from: {endpoint}")
        
        while current_url:
            try:
                logger.info(f"Fetching page {page_number}...")
                response = self._fetch_page(current_url)
                
                # Extract results from current page
                results = response.get('results', [])
                all_results.extend(results)
                
                # Get next page URL from pagination info
                info = response.get('info', {})
                current_url = info.get('next')
                
                logger.info(
                    f"Page {page_number}: Retrieved {len(results)} records. "
                    f"Total so far: {len(all_results)}"
                )
                
                if current_url:
                    page_number += 1
                    # Small delay to be respectful to the API
                    time.sleep(0.1)
                else:
                    logger.info(f"✓ Completed pagination. Total records: {len(all_results)}")
            
            except Exception as e:
                logger.error(f"Error during pagination on page {page_number}: {e}")
                raise APIIngestionError(f"Pagination failed on page {page_number}: {e}")
        
        return all_results
    
    def ingest_entity( self, endpoint: str, entity_name: str,  save_to_file: bool = True  ) -> List[Dict[str, Any]]:
        """
        Generic method to ingest data from any API endpoint.
        
        Args:
            endpoint: API endpoint URL
            entity_name: Name of the entity (e.g., 'characters', 'episodes')
            save_to_file: Whether to save raw JSON to file
        
        Returns:
            List of all records from the endpoint
        """
        logger.info("=" * 60)
        logger.info(f"Starting {entity_name} ingestion...")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        try:
            records = self.fetch_all_pages(endpoint)
            
            elapsed_time = time.time() - start_time
            
            # Save to file if requested
            if save_to_file:
                # Clean up old files before saving new one
                entity_dir = RAW_DATA_PATH / entity_name
                cleanup_old_files(entity_dir, f"{entity_name}_*.json", keep_latest=0)
                
                timestamp = get_timestamp().replace(':', '-')
                file_path = entity_dir / f"{entity_name}_{timestamp}.json"
                
                data_to_save = {
                    "ingested_at": get_timestamp(),
                    "source": endpoint,
                    "total_records": len(records),
                    "data": records
                }
                
                save_json_to_file(data_to_save, file_path)
                logger.info(f"✓ Saved raw {entity_name} data to: {file_path}")
                logger.info(f"  (Old files cleaned up, keeping only latest)")
            
            # Print summary
            print_summary(f"{entity_name.title()} Ingestion Summary", {
                f"Total {entity_name.title()}": len(records),
                "Elapsed Time": f"{elapsed_time:.2f}s",
                "Records/Second": f"{len(records)/elapsed_time:.2f}",
                "Status": "✓ SUCCESS"
            })
            
            return records
        
        except Exception as e:
            logger.error(f"{entity_name.title()} ingestion failed: {e}")
            raise APIIngestionError(f"Failed to ingest {entity_name}: {e}")
    
    def close(self):
        """Close the HTTP session."""
        self.session.close()


def run_ingestion() -> Dict[str, List[Dict[str, Any]]]:
    """
    Run complete ingestion for both characters and episodes.
    
    Returns:
        Dictionary with 'characters' and 'episodes' keys containing the data
    """
    logger.info("\n🚀 Starting Rick and Morty API Ingestion Pipeline")
    logger.info("=" * 60)
    
    client = RickMortyAPIClient()
    
    try:
        # Ingest characters
        characters = client.ingest_entity(CHARACTERS_ENDPOINT, "characters", save_to_file=True)
        
        # Ingest episodes
        episodes = client.ingest_entity(EPISODES_ENDPOINT, "episodes", save_to_file=True)
        
        # Overall summary
        print_summary("Overall Ingestion Summary", {
            "Total Characters": len(characters),
            "Total Episodes": len(episodes),
            "Total API Records": len(characters) + len(episodes),
            "Storage Location": str(RAW_DATA_PATH),
            "Status": "✓ PIPELINE COMPLETED SUCCESSFULLY"
        })
        
        return {
            "characters": characters,
            "episodes": episodes
        }
    
    finally:
        client.close()


if __name__ == "__main__":
    """
    Run ingestion as standalone script for testing.
    Note: Directories will be auto-created when saving files.
    """
    # Run ingestion
    try:
        result = run_ingestion()
        logger.info(f"✓ Ingestion completed successfully!")
        logger.info(f"  - Characters: {len(result['characters'])}")
        logger.info(f"  - Episodes: {len(result['episodes'])}")
    except Exception as e:
        logger.error(f"✗ Ingestion failed: {e}")
        raise
