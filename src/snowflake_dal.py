"""
Snowflake Data Access Layer (DAL).
Handles database connections, DDL execution, and data operations.
"""

import logging
from typing import Optional, List, Dict, Any

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import Error as SnowflakeError

from .config import SNOWFLAKE_CONFIG
from .utils import setup_logging


logger = setup_logging()


class SnowflakeDAL:
    """
    Data Access Layer for Snowflake operations.
    Handles connections and SQL execution.
    """
    
    def __init__(self, config: Optional[Dict[str, str]] = None):
        """
        Initialize Snowflake DAL.
        
        Args:
            config: Snowflake connection configuration.
                    If None, uses SNOWFLAKE_CONFIG from environment.
        """
        self.config = config or SNOWFLAKE_CONFIG
        self._connection: Optional[SnowflakeConnection] = None
        self._validate_config()
    
    def _validate_config(self):
        """
        Validate that required configuration is present.
        
        Raises:
            ValueError: If required configuration is missing
        """
        required_keys = ["account", "user", "password"]
        missing = [key for key in required_keys if not self.config.get(key)]
        
        if missing:
            raise ValueError(
                f"Missing required Snowflake configuration: {', '.join(missing)}\n"
                f"Please check your .env file."
            )
    
    def connect(self) -> SnowflakeConnection:
        """
        Establish connection to Snowflake.
        
        Returns:
            Active Snowflake connection
        
        Raises:
            SnowflakeError: If connection fails
        """
        if self._connection is not None and not self._connection.is_closed():
            logger.debug("Using existing Snowflake connection")
            return self._connection
        
        try:
            logger.info(f"Connecting to Snowflake account: {self.config['account']}")
            
            self._connection = snowflake.connector.connect(
                account=self.config["account"],
                user=self.config["user"],
                password=self.config["password"],
                role=self.config.get("role"),
                warehouse=self.config.get("warehouse"),
                database=self.config.get("database"),
                schema=self.config.get("schema"),
                session_parameters={
                    'QUERY_TAG': 'rick_morty_pipeline',
                },
                # SSL configuration to handle certificate issues
                ocsp_fail_open=True,  # Allow connection even if certificate validation fails
                insecure_mode=True,   # Completely bypass SSL certificate validation
            )
            
            logger.info("✓ Successfully connected to Snowflake")
            
            # Log connection details
            cursor = self._connection.cursor()
            cursor.execute("SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
            account, user, role, warehouse = cursor.fetchone()
            logger.info(f"  Account: {account}")
            logger.info(f"  User: {user}")
            logger.info(f"  Role: {role}")
            logger.info(f"  Warehouse: {warehouse}")
            cursor.close()
            
            return self._connection
        
        except SnowflakeError as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def close(self):
        """Close Snowflake connection."""
        if self._connection and not self._connection.is_closed():
            self._connection.close()
            logger.info("✓ Snowflake connection closed")
            self._connection = None
    
    def execute_query( self, query: str, params: Optional[tuple] = None, fetch: bool = False ) -> Optional[List]:
        """
        Execute a single SQL query.
        
        Args:
            query: SQL query to execute
            params: Query parameters for safe parameterization
            fetch: If True, returns results
        
        Returns:
            Query results if fetch=True, None otherwise
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            logger.debug(f"Executing query: {query[:200]}...")
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if fetch:
                results = cursor.fetchall()
                return results
            
            return None
        
        except SnowflakeError as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            raise
        
        finally:
            cursor.close()
    
    def execute_script(self, sql_script: str) -> None:
        """
        Execute a multi-statement SQL script.
        Splits by semicolon and executes each statement.
        
        Args:
            sql_script: SQL script with multiple statements
        """
        # Split by semicolon
        raw_statements = sql_script.split(';')
        
        # Clean each statement: remove comment lines and empty lines
        statements = []
        for stmt in raw_statements:
            # Remove comment lines (lines starting with --)
            cleaned_lines = [
                line 
                for line in stmt.split('\n') 
                if line.strip() and not line.strip().startswith('--')
            ]
            cleaned_stmt = '\n'.join(cleaned_lines).strip()
            
            # Only add non-empty statements
            if cleaned_stmt:
                statements.append(cleaned_stmt)
        
        logger.info(f"Executing script with {len(statements)} statements...")
        
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            for i, statement in enumerate(statements, 1):
                try:
                    logger.debug(f"Statement {i}/{len(statements)}: {statement[:100]}...")
                    cursor.execute(statement)
                    logger.info(f"✓ Statement {i}/{len(statements)} executed successfully")
                
                except SnowflakeError as e:
                    logger.error(f"✗ Statement {i} failed: {e}")
                    logger.error(f"Statement: {statement}")
                    raise
        
        finally:
            cursor.close()
        
        logger.info(f"✓ Script executed successfully ({len(statements)} statements)")
    
    def execute_file(self, sql_file_path: str) -> None:
        """
        Execute SQL statements from a file.
        
        Args:
            sql_file_path: Path to SQL file
        """
        logger.info(f"Executing SQL file: {sql_file_path}")
        
        with open(sql_file_path, 'r') as f:
            sql_script = f.read()
        
        self.execute_script(sql_script)
    
    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        Check if a table exists.
        
        Args:
            table_name: Name of the table
            schema: Schema name (uses default if not provided)
        
        Returns:
            True if table exists, False otherwise
        """
        schema = schema or self.config.get("schema", "PUBLIC")
        
        query = """
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = %s 
            AND TABLE_NAME = %s
        """
        
        result = self.execute_query(query, (schema.upper(), table_name.upper()), fetch=True)
        return result[0][0] > 0 if result else False
    
    def get_row_count(self, table_name: str, schema: Optional[str] = None) -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Name of the table
            schema: Schema name (uses default if not provided)
        
        Returns:
            Number of rows in the table
        """
        schema = schema or self.config.get("schema", "PUBLIC")
        full_table_name = f"{schema}.{table_name}"
        
        query = f"SELECT COUNT(*) FROM {full_table_name}"
        result = self.execute_query(query, fetch=True)
        
        return result[0][0] if result else 0
    
    def upload_file_to_stage(self, local_file_path: str, stage_name: str) -> None:
        """
        Upload a local file to Snowflake internal stage using PUT command.
        
        Args:
            local_file_path: Path to local file
            stage_name: Name of the stage (with @ prefix)
        
        Raises:
            Exception if upload fails
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            put_command = f"PUT file://{local_file_path} {stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            
            cursor.execute(put_command)
            result = cursor.fetchone()
            
            if result and result[6] == 'UPLOADED':
                logger.info(f"✓ Uploaded to stage")
            else:
                raise Exception(f"Upload failed with status: {result}")
        
        finally:
            cursor.close()
    
    def copy_into_from_stage(
        self, 
        table_name: str, 
        stage_name: str,
        file_pattern: Optional[str] = None,
        transformations: Optional[str] = None,
        flatten_json_array: bool = False
    ) -> int:
        """
        Copy data from stage into table using COPY INTO command.
        
        Args:
            table_name: Target table name (schema.table)
            stage_name: Stage name (with @ prefix)
            file_pattern: Optional file pattern to match (e.g., 'characters_.*')
            transformations: Optional column transformations in SELECT
            flatten_json_array: If True, use LATERAL FLATTEN to explode JSON data array
        
        Returns:
            Number of rows loaded
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Build COPY INTO command
            copy_cmd = f"COPY INTO {table_name} "
            
            if transformations:
                copy_cmd += f"({transformations}) "
            
            copy_cmd += f"FROM {stage_name}"
            
            if file_pattern:
                copy_cmd += f"/{file_pattern}"
            
            copy_cmd += " FILE_FORMAT = (TYPE = 'JSON') MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
            
            # If flatten_json_array is True, use two-step process
            if flatten_json_array:
                # Step 1: Create temp table and load entire JSON file
                temp_table = f"{table_name}_temp"
                stage_path = f"{stage_name}/{file_pattern}" if file_pattern else stage_name
                
                logger.info(f"Creating temporary table {temp_table}")
                cursor.execute(f"CREATE TEMPORARY TABLE {temp_table} (raw_json VARIANT)")
                
                logger.info(f"Loading JSON file into temporary table")
                copy_temp_cmd = f"""
                COPY INTO {temp_table}
                FROM {stage_path}
                FILE_FORMAT = (TYPE = 'JSON')
                """
                cursor.execute(copy_temp_cmd)
                
                # Step 2: Flatten and insert into target table
                logger.info(f"Flattening JSON array and inserting into {table_name}")
                insert_cmd = f"""
                INSERT INTO {table_name} (id, raw_data, source_file)
                SELECT 
                    value:id::INTEGER as id,
                    value as raw_data,
                    '{file_pattern}' as source_file
                FROM {temp_table},
                LATERAL FLATTEN(input => raw_json:data)
                """
                cursor.execute(insert_cmd)
                result = cursor.fetchone()
                
                # Clean up temp table
                cursor.execute(f"DROP TABLE {temp_table}")
                
                # Return row count from INSERT
                if result:
                    rows_loaded = result[0] if isinstance(result[0], int) else 0
                    logger.info(f"✓ Loaded {rows_loaded} rows into {table_name}")
                    return rows_loaded
                return 0
            
            logger.info(f"Copying data into {table_name}")
            logger.debug(f"COPY command: {copy_cmd}")
            
            cursor.execute(copy_cmd)
            result = cursor.fetchone()
            
            # COPY INTO returns (file, status, rows_parsed, rows_loaded, ...)
            if result:
                rows_loaded = result[3] if len(result) > 3 else 0
                logger.info(f"✓ Loaded {rows_loaded} rows into {table_name}")
                return rows_loaded
            
            return 0
        
        except Exception as e:
            logger.error(f"Failed to copy data: {e}")
            raise
        
        finally:
            cursor.close()
    
    def load_json_to_raw_table(
        self,
        json_file_path: str,
        table_name: str,
        stage_name: str = "@raw_data_stage"
    ) -> int:
        """
        Complete workflow: Upload JSON file and load into raw table.
        Handles JSON files with array of records wrapped in metadata.
        
        Args:
            json_file_path: Path to JSON file
            table_name: Target table name
            stage_name: Stage name (default: @raw_data_stage)
        
        Returns:
            Number of rows loaded
        """
        import json
        from pathlib import Path
        
        logger.info(f"Loading {json_file_path} into {table_name}")
        
        # Read and parse JSON file to extract individual records
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        records = data.get('data', [])
        source_file = Path(json_file_path).name
        
        if not records:
            logger.warning(f"No records found in {json_file_path}")
            return 0
        
        logger.info(f"Found {len(records)} records to load")
        
        # Insert records one by one (simpler for this use case)
        conn = self.connect()
        cursor = conn.cursor()
        rows_loaded = 0
        
        try:
            for record in records:
                record_id = record.get('id')
                
                # Insert with PARSE_JSON to convert string to VARIANT
                insert_sql = f"""
                INSERT INTO {table_name} (id, raw_data, source_file)
                VALUES (%s, PARSE_JSON(%s), %s)
                """
                
                cursor.execute(
                    insert_sql,
                    (record_id, json.dumps(record), source_file)
                )
                rows_loaded += 1
            
            logger.info(f"✓ Successfully loaded {rows_loaded} rows into {table_name}")
            return rows_loaded
        
        except Exception as e:
            logger.error(f"Failed during load: {e}")
            raise
        
        finally:
            cursor.close()
    
    def test_connection(self) -> bool:
        """
        Test Snowflake connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.connect()
            result = self.execute_query("SELECT 1", fetch=True)
            
            if result and result[0][0] == 1:
                logger.info("✓ Connection test successful")
                return True
            
            return False
        
        except Exception as e:
            logger.error(f"✗ Connection test failed: {e}")
            return False
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


if __name__ == "__main__":
    """
    Test Snowflake connection when run as standalone script.
    """
    import sys
    
    logger.info("=" * 60)
    logger.info("Testing Snowflake Connection")
    logger.info("=" * 60)
    
    try:
        dal = SnowflakeDAL()
        success = dal.test_connection()
        dal.close()
        
        if success:
            logger.info("\n" + "=" * 60)
            logger.info("  ✓ Connection test PASSED")
            logger.info("=" * 60)
            sys.exit(0)
        else:
            logger.error("\n" + "=" * 60)
            logger.error("  ✗ Connection test FAILED")
            logger.error("=" * 60)
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"\n✗ Connection test failed with error: {e}")
        sys.exit(1)
