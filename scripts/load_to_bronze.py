#!/usr/bin/env python3
"""
Bronze Layer Ingestion
====================================================================
Author: Lorenz Alay-ay 
Created Date: 06/OCT/2025

This script loads raw data from various formats into the Bronze layer

USAGE EXAMPLES:
--------------
Load all tables:
    python load_to_bronze.py

Load specific tables:
    python load_to_bronze.py --tables customers,products

Dry run (validate without writing):
    python load_to_bronze.py --dry-run

Force reload (ignore processed files):
    python load_to_bronze.py --force

Custom paths:
    python load_to_bronze.py --raw data_raw --lake lake
"""

# ============================================================================
# IMPORTS
# ============================================================================

# Standard library imports - these come with Python
import argparse
import csv
import hashlib
import json
import pathlib
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple

# Third-party imports - these need to be installed via pip
import duckdb
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.json as pa_json
import pyarrow.parquet as pq
import pyarrow.compute as pc

# Try to import Delta Lake support
DELTA_AVAILABLE = False
try:
    from deltalake import write_deltalake, DeltaTable
    DELTA_AVAILABLE = True
except Exception:
    pass

# Import schemas from our schemas module
try:
    from schemas.schemas import (
        customers_schema, products_schema, stores_schema, suppliers_schema,
        orders_header_schema, orders_lines_schema, events_schema, sensors_schema,
        exchange_rates_schema, shipments_schema, returns_day1_schema,
    )
except ImportError as exc:
    print(f"ERROR: Cannot import schemas: {exc}", file=sys.stderr)
    sys.exit(1)

# ============================================================================
# imports for bronze ingestion
# ===========================================================================

from scripts.config import (
    SCHEMA_MAP,
    PRIMARY_KEYS,
    SOURCE_PATTERNS,
    PARTITION_COLUMNS,
    MIN_FILE_SIZE_MB,
    TARGET_FILE_SIZE_MB,
    MAX_FILE_SIZE_MB,
)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def parse_args():
    """
    Parse command-line arguments for the data loading script.
    This allows users to customize how the script runs without changing code.
    Returns:
        argparse.Namespace: Object containing all parsed arguments
    """
    # Create the argument parser with a description
    parser = argparse.ArgumentParser(
        description="Load raw data to Bronze layer"
    )

    # Define each command-line argument
    parser.add_argument(
        "--raw-dir", 
        type=str, 
        default="data_raw",
        help="Directory containing raw source files (default: data_raw)"
    )
    
    parser.add_argument(
        "--lake-dir", 
        type=str, 
        default="lake",
        help="Data lake output directory (default: lake)"
    )
    
    parser.add_argument(
        "--db-path", 
        type=str, 
        default="duckdb/warehouse.duckdb",
        help="DuckDB database path for manifest tracking (default: duckdb/warehouse.duckdb)"
    )
    
    parser.add_argument(
        "--tables", 
        type=str, 
        default="",
        help="Comma-separated list of tables to load (default: all). Example: customers,orders"
    )
    
    parser.add_argument(
        "--force", 
        action="store_true",
        help="Force reload even if files already processed (ignores manifest)"
    )
    
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Validate data without writing (test mode - no changes made)"
    )
    
    return parser.parse_args()


def ensure_dir(path: pathlib.Path):
    """
    Create a directory if it doesn't exist.
    Args:
        path: pathlib.Path object representing the directory to create
    """

    path.mkdir(parents=True, exist_ok=True)


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable format.
    Args:
        seconds: Duration in seconds (can be decimal)     
    Returns:
        Formatted string like "2m 5.5s" or "12.3s"
    """

    if seconds < 60:
        return f"{seconds:.1f}s"
    
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = seconds % 60
        return f"{mins}m {secs:.1f}s"
    
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"


def compute_row_hash(row: Dict[str, Any]) -> str:
    """
    Compute an MD5 hash of a row for deduplication.
    Args:
        row: Dictionary representing a single row of data    
    Returns:
        MD5 hash string (32 characters of letters/numbers)
    """

    content = json.dumps(row, sort_keys=True, default=str)

    return hashlib.md5(content.encode()).hexdigest()


def add_audit_columns(table: pa.Table, source_file: str, 
                      ingestion_ts: datetime) -> pa.Table:
    """
    Add metadata columns to track data lineage for each row.
    Args:
        table: PyArrow Table to enrich with audit columns
        source_file: Name/path of the source file
        ingestion_ts: Timestamp of when the row was ingested 
    Returns:
        PyArrow Table with 3 additional audit columns appended
    """

    num_rows = len(table)

    ingestion_col = pa.array(
        [ingestion_ts] * num_rows,
        type=pa.timestamp("us")
    )
    
    filename_col = pa.array(
        [source_file] * num_rows,
        type=pa.string()
    )

    hashes = []
    
    for i in range(num_rows):
        row_dict = {
            col: table[col][i].as_py() 
            for col in table.column_names
        }
        
        hashes.append(compute_row_hash(row_dict))
    
    hash_col = pa.array(hashes, type=pa.string())

    table = table.append_column("ingestion_ts", ingestion_col)
    table = table.append_column("src_filename", filename_col)
    table = table.append_column("src_row_hash", hash_col)

    return table

def derive_partition_from_timestamp(table: pa.Table, timestamp_col: str, 
                                    partition_cols: List[str]) -> pa.Table:
    """
    Derive partition columns from timestamp just before writing.
    Handles string timestamps with timezone offsets.
    
    Args:
        table: Table with timestamp column
        timestamp_col: Name of timestamp column (e.g., "sensor_ts", "event_ts")
        partition_cols: Desired partition columns (e.g., ["store_id", "month"], ["event_date"])
    
    Returns:
        Table with partition columns added (temporarily, just for write)
    """
    result_table = table
    
    for partition_col in partition_cols:
        if partition_col in table.column_names:
            # Column already exists (e.g., store_id from CSV)
            continue
        
        # Get timestamp column
        timestamps = table[timestamp_col]
        
        # ====================================================================
        # Convert string timestamps to timestamp type
        # ====================================================================
        if pa.types.is_string(timestamps.type) or pa.types.is_large_string(timestamps.type):
            print(f" Converting {timestamp_col} from string to timestamp...")
            
            # METHOD 1: Strip timezone and parse
            # For strings like '2024-01-01T03:34:25+00:00'
            # Strip the timezone part (+00:00, -05:00, etc.) and parse as naive timestamp
            try:
                # Remove timezone offset using string manipulation
                timestamps_stripped = pc.replace_substring_regex(
                    timestamps, 
                    pattern=r'[+-]\d{2}:\d{2}$',  # Matches +00:00, -05:00, etc.
                    replacement=''
                )
                
                # Now parse without timezone
                timestamps = pc.strptime(timestamps_stripped, format='%Y-%m-%dT%H:%M:%S', unit='us')
                print(f" Parsed timestamps (stripped timezone)")
                
            except Exception as e1:
                print(f" Method 1 failed: {str(e1)[:100]}")
                
                # METHOD 2: Manual parsing with Python
                try:
                    print(f" Trying Python datetime parsing...")
                    from dateutil import parser as date_parser
                    
                    parsed_timestamps = []
                    for ts_str in timestamps.to_pylist():
                        if ts_str is None:
                            parsed_timestamps.append(None)
                        else:
                            # Parse with dateutil (handles any format)
                            dt = date_parser.parse(ts_str)
                            # Convert to naive (ignore timezone)
                            dt_naive = dt.replace(tzinfo=None)
                            parsed_timestamps.append(dt_naive)
                    
                    timestamps = pa.array(parsed_timestamps, type=pa.timestamp('us'))
                    print(f" Parsed with Python datetime")
                    
                except Exception as e2:
                    print(f" Method 2 failed: {str(e2)}")
                    raise ValueError(
                        f"Cannot parse timestamp column '{timestamp_col}'.\n"
                        f"First value: {table[timestamp_col][0]}\n"
                        f"Column type: {table[timestamp_col].type}"
                    )
        
        elif pa.types.is_timestamp(timestamps.type):
            # Already a timestamp
            pass
        
        else:
            raise ValueError(
                f"Column '{timestamp_col}' must be string or timestamp, "
                f"got {timestamps.type}"
            )
        
        # ====================================================================
        # Extract partition values
        # ====================================================================
        
        if partition_col == "month":
            # Derive month from timestamp (YYYY-MM)
            years = pc.year(timestamps)
            months = pc.month(timestamps)
            
            month_values = []
            for year, month in zip(years.to_pylist(), months.to_pylist()):
                if year is not None and month is not None:
                    month_str = f"{year:04d}-{month:02d}"
                else:
                    month_str = None
                month_values.append(month_str)
            
            month_col = pa.array(month_values, type=pa.string())
            result_table = result_table.append_column("month", month_col)
            print(f" Added 'month' column")
        
        elif partition_col in ["date", "event_date", "order_date", "sensor_date"]:
            # Derive date from timestamp (YYYY-MM-DD)
            years = pc.year(timestamps)
            months = pc.month(timestamps)
            days = pc.day(timestamps)
            
            date_values = []
            for year, month, day in zip(years.to_pylist(), months.to_pylist(), days.to_pylist()):
                if year is not None and month is not None and day is not None:
                    date_str = f"{year:04d}-{month:02d}-{day:02d}"
                else:
                    date_str = None
                date_values.append(date_str)
            
            date_col = pa.array(date_values, type=pa.string())
            result_table = result_table.append_column(partition_col, date_col)
            print(f" Added '{partition_col}' column")
    
    return result_table

# ============================================================================
# MANIFEST TRACKING (Idempotency)
# ============================================================================

class ManifestTracker:
    """
    Tracks which files have been processed to avoid reloading.
    """

    def __init__(self, db_path: str):
        """
        Initialize the manifest tracker with a DuckDB connection. 
        Args:
            db_path: Path to the DuckDB database file that will store the manifest
        """

        self.db_path = pathlib.Path(db_path)
        
        ensure_dir(self.db_path.parent)
        
        self.conn = duckdb.connect(str(self.db_path))
        
        self._create_manifest_table()

    def _create_manifest_table(self):
        """
        Create the bronze_manifest table if it doesn't already exist.
        This table stores a record of every file we've processed.
        This ensures we can't accidentally record the same file twice.
        """
        # Execute SQL to create table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze_manifest (
                table_name VARCHAR,        -- Which table this file belongs to
                source_file VARCHAR,       -- Full path to the source file
                file_size BIGINT,          -- File size in bytes
                file_hash VARCHAR,         -- MD5 hash of file contents
                processed_at TIMESTAMP,    -- When we loaded this file
                row_count BIGINT,          -- How many rows were loaded
                PRIMARY KEY (table_name, source_file)  -- Unique constraint
            )
        """)

    def is_processed(self, table_name: str, source_file: str, 
                     file_size: int, file_hash: str) -> bool:
        """
        Check if a file has already been processed.        
        Args:
            table_name: Logical table name (e.g., "customers")
            source_file: Path to the file (e.g., "data_raw/customers.csv")
            file_size: Current size of the file in bytes
            file_hash: Current MD5 hash of the file content            
        Returns:
            True if file already processed and unchanged, False otherwise
        """
        # Query the manifest table to see if this exact file exists
        result = self.conn.execute("""
            SELECT COUNT(*) 
            FROM bronze_manifest
            WHERE table_name = ?     -- Match table name
              AND source_file = ?    -- Match file path
              AND file_size = ?      -- Match file size
              AND file_hash = ?      -- Match file hash
        """, [table_name, source_file, file_size, file_hash]).fetchone()
        
        return result[0] > 0

    def mark_processed(self, table_name: str, source_file: str,
                       file_size: int, file_hash: str, row_count: int):
        """
        Record that a file has been successfully processed.        
        Args:
            table_name: Logical table name
            source_file: Path to the file
            file_size: File size in bytes
            file_hash: MD5 hash of the file content
            row_count: Number of rows successfully loaded
        """

        self.conn.execute("""
            INSERT OR REPLACE INTO bronze_manifest
            (table_name, source_file, file_size, file_hash, processed_at, row_count)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            table_name,                    # Which table
            source_file,                   # Which file
            file_size,                     # File size
            file_hash,                     # File hash
            datetime.now(timezone.utc),    # Current timestamp (UTC)
            row_count                      # How many rows loaded
        ])

    def close(self):
        """
        Close the DuckDB database connection.
        
        Always close database connections when done to:
        - Release file locks
        - Ensure all writes are flushed to disk
        - Free up system resources
        
        This should be called in a finally block or at the end of processing.
        """
        self.conn.close()


# ============================================================================
# DATA QUALITY & REJECTS HANDLING
# ============================================================================

class RejectsHandler:
    """
    Handles rows that fail validation.
    """

    def __init__(self, rejects_dir: pathlib.Path):
        """
        Initialize the rejects handler.
        Args:
            rejects_dir: Folder where rejected rows will be saved
                        (e.g., "lake/_rejects/")
        """

        self.rejects_dir = rejects_dir
        
        ensure_dir(self.rejects_dir)
        
        self.rejects: Dict[str, List[Dict]] = defaultdict(list)

    def add_reject(self, table_name: str, row_data: Dict, 
                   reason: str, source_file: str):
        """
        Record a rejected row with the reason why it failed.        
        Args:
            table_name: Name of the table where the row came from
            row_data: The actual row data that failed validation (as dict)
            reason: Why the row was rejected (human-readable error message)
            source_file: The file where the row came from (for tracing)
        """

        self.rejects[table_name].append({
            "row_data": row_data,           # The bad row itself
            "reason": reason,                # Why it failed
            "source_file": source_file,      # Where it came from
            "rejected_at": datetime.now(timezone.utc).isoformat()  # When we rejected it
        })

    def write_rejects(self):
        """
        Write all rejected rows to JSON files.
        Each table gets its own reject file:
        - lake/_rejects/customers_rejects.json
        - lake/_rejects/orders_rejects.json
        - etc.
        """

        if not self.rejects:
            return

        for table_name, reject_list in self.rejects.items():

            reject_file = self.rejects_dir / f"{table_name}_rejects.json"
            
            with reject_file.open("w", encoding="utf-8") as f:
                json.dump(reject_list, f, indent=2, default=str)
            
            print(f"  Wrote {len(reject_list):,} rejects to {reject_file}")

    def get_reject_count(self, table_name: str) -> int:
        """
        Return the number of rejects for a given table.        
        Args:
            table_name: Name of the table            
        Returns:
            Number of rejected rows (0 if none)
        """

        return len(self.rejects.get(table_name, []))


# ============================================================================
# SCHEMA EVOLUTION HANDLER
# ============================================================================

class SchemaEvolutionHandler:
    """
    Handles schema changes between expected and actual data.
    """
    
    def __init__(self):
        """
        Initialize the schema evolution handler.
        
        Tracks all schema changes detected during processing.
        """

        self.schema_changes: Dict[str, Dict[str, List[str]]] = defaultdict(
            lambda: {"new_columns": [], "missing_columns": []}
        )
    
    def handle_schema_mismatch(self, table: pa.Table, expected_schema: pa.Schema,
                               table_name: str) -> pa.Table:
        """
        Reconcile differences between incoming data and expected schema.        
        Args:
            table: PyArrow Table with incoming data
            expected_schema: PyArrow Schema defining expected structure
            table_name: Name of the table (for logging)            
        Returns:
            PyArrow Table with schema reconciled (missing columns added)
        """

        table_columns = set(table.column_names)
        expected_columns = set(expected_schema.names)
        
        # Find NEW columns: In data but not in expected schema
        new_columns = table_columns - expected_columns
        
        if new_columns:
            print(f"  Schema evolution detected in {table_name}")
            print(f"  New columns: {', '.join(new_columns)}")
            
            # Track this change
            self.schema_changes[table_name]["new_columns"].extend(new_columns)
        
        # Find MISSING columns: in expected schema but not in data
        missing_columns = expected_columns - table_columns
        
        if missing_columns:
            # Log the missing columns
            print(f"  Missing columns in {table_name}")
            print(f"  Adding with NULL values: {', '.join(missing_columns)}")
            
            # Track this change
            self.schema_changes[table_name]["missing_columns"].extend(missing_columns)
            
            # Add NULL columns for each missing field
            for col_name in missing_columns:

                field = expected_schema.field(col_name)
                
                null_array = pa.array(
                    [None] * len(table),
                    type=field.type        # Use the expected data type
                )
                
                # Append this column to the table
                table = table.append_column(col_name, null_array)
        
        return table
    
    def get_summary(self) -> str:
        """
        Generate a summary report of all schema changes detected.        
        Returns:
            Human-readable summary of all schema changes
        """

        if not self.schema_changes:
            return "No schema changes detected"
        
        lines = ["Schema Evolution Summary:", "=" * 50]
        
        for table_name, changes in self.schema_changes.items():
            lines.append(f"\n{table_name}:")
            
            # Report new columns
            if changes["new_columns"]:
                lines.append(f"  New columns added: {', '.join(changes['new_columns'])}")
            
            # Report missing columns
            if changes["missing_columns"]:
                lines.append(f"  Missing columns (added as NULL): {', '.join(changes['missing_columns'])}")
        
        # Join all lines with newlines
        return "\n".join(lines)


# ============================================================================
# SOFT DELETE DETECTOR
# ============================================================================

class SoftDeleteDetector:
    """
    Detects deleted records by comparing with previous data version.
    """
    
    def detect_deletes(self, new_table: pa.Table, table_name: str,
                      lake_dir: pathlib.Path, primary_keys: List[str]) -> Optional[pa.Table]:
        """
        Detect deleted records by comparing with previous version.        
        Args:
            new_table: PyArrow Table with new/updated data
            table_name: Name of the table (for finding Delta path)
            lake_dir: Base directory for data lake
            primary_keys: List of column names forming primary key            
        Returns:
            PyArrow Table containing tombstone records for deleted rows,
            or None if no deletes detected or table doesn't exist yet
        """

        bronze_delta = lake_dir / "bronze" / "delta" / table_name
        
        # If Delta table doesn't exist yet, this is the first load
        if not bronze_delta.exists():
            return None
        
        try:
            # Load the existing Delta table (previous version)
            dt = DeltaTable(str(bronze_delta))
            existing_table = dt.to_pyarrow_table()
            
            # Extract primary key values from NEW data
            # Convert to set of tuples for efficient comparison
            new_pks = set(
                # For each row, create a tuple of primary key values
                tuple(row[pk] for pk in primary_keys)
                for row in new_table.select(primary_keys).to_pylist()
            )
            
            # Extract primary key values from EXISTING data
            existing_pks = set(
                tuple(row[pk] for pk in primary_keys)
                for row in existing_table.select(primary_keys).to_pylist()
            )
            
            # Find DELETED records: in existing but not in new
            # Set subtraction: existing - new = deleted
            deleted_pks = existing_pks - new_pks
            
            # If no deletes, return None
            if not deleted_pks:
                return None
            
            # Log how many deletes we found
            print(f"  Detected {len(deleted_pks):,} deleted records in {table_name}")
            
            # Create tombstone records for deleted rows
            deleted_rows = []
            
            # Convert existing table to list of dictionaries for easier processing
            for row in existing_table.to_pylist():
                # Get this row's primary key as a tuple
                pk_tuple = tuple(row[pk] for pk in primary_keys)
                
                # If this primary key was deleted
                if pk_tuple in deleted_pks:
                    # Add tombstone markers to the row
                    row['is_deleted'] = True                    # Mark as deleted
                    row['deleted_at'] = datetime.now(timezone.utc)  # When deleted
                    deleted_rows.append(row)
            
            # Convert list of deleted rows to PyArrow Table
            if deleted_rows:
                return pa.Table.from_pylist(deleted_rows)
            else:
                return None
                
        except Exception as e:
            # If anything goes wrong (e.g., corrupted Delta table), log and continue
            print(f"  Error detecting deletes for {table_name}: {e}")
            return None


# ============================================================================
# FILE SIZE OPTIMIZER 
# ============================================================================

class FileSizeOptimizer:
    """
    Optimizes file sizes for query performance.
    - Too small (< 100MB): Too many files = slow metadata operations
    - Too large (> 250MB): Slow to read, high memory usage, poor parallelism
    - Just right (100-250MB): Fast queries, good parallelism, efficient storage
    
    TARGET: 100-250MB per file
    This is industry best practice for Parquet files in data lakes.
    """
    
    def __init__(self, min_size_mb: int = MIN_FILE_SIZE_MB,
                 target_size_mb: int = TARGET_FILE_SIZE_MB,
                 max_size_mb: int = MAX_FILE_SIZE_MB):
        """
        Initialize the file size optimizer with size thresholds.
        
        Args:
            min_size_mb: Minimum desirable file size (default: 100MB)
            target_size_mb: Target file size when splitting (default: 150MB)
            max_size_mb: Maximum file size before splitting (default: 250MB)
        """
        self.min_size_mb = min_size_mb      # Below this = too small
        self.target_size_mb = target_size_mb  # Aim for this size
        self.max_size_mb = max_size_mb      # Above this = too large

    def optimize_table(self, table: pa.Table) -> List[pa.Table]:
        """
        Optimize table by splitting if too large or warning if too small.        
        Args:
            table: PyArrow Table to optimize    
        Returns:
            List of PyArrow Tables (multiple if split, single if optimal)
        """
        # Calculate current size in megabytes
        current_size_mb = table.nbytes / (1024 * 1024)
        
        # Case 1: Too small (< 100MB)
        if current_size_mb < self.min_size_mb:

            print(f"  Small file ({current_size_mb:.1f}MB) - "
                  f"consider batching with other data")
            # Return as-is (single table in a list)
            return [table]
        
        # Case 2: Too large (> 250MB) - need to split
        elif current_size_mb > self.max_size_mb:
            # Calculate how many chunks we need
            num_chunks = int(current_size_mb / self.target_size_mb) + 1
            
            # Calculate rows per chunk
            total_rows = len(table)
            rows_per_chunk = total_rows // num_chunks
            
            # Split table into chunks
            chunks = []
            for i in range(num_chunks):
                # Calculate start and end row for this chunk
                start = i * rows_per_chunk
                
                # Last chunk gets all remaining rows (handles rounding)
                if i == num_chunks - 1:
                    end = total_rows
                else:
                    end = start + rows_per_chunk
                
                # Extract this slice of rows
                chunk = table.slice(start, end - start)
                chunks.append(chunk)
            
            # Log
            print(f"  Split {current_size_mb:.1f}MB into {num_chunks} "
                  f"files (~{current_size_mb/num_chunks:.1f}MB each)")
            
            return chunks
        
        # Case 3: Optimal size (100-250MB) - perfect!
        else:
            # Return as-is (single table in a list)
            return [table]

# ============================================================================
# TABLE LOADERS - One loader per source format
# ============================================================================

def load_csv_files(raw_dir: pathlib.Path, pattern: str, schema: pa.Schema,
                   table_name: str, manifest: ManifestTracker,
                   rejects: RejectsHandler, schema_handler: SchemaEvolutionHandler,
                   force: bool, dry_run: bool) -> Optional[pa.Table]:
    """
    Load CSV files and validate against schema with evolution support.   
    Args:
        raw_dir: Base directory containing raw files
        pattern: Glob pattern to match files (e.g., "*.csv" or "**/*.csv")
        schema: Expected PyArrow schema for validation
        table_name: Logical table name for logging
        manifest: Tracker to avoid reprocessing files
        rejects: Handler for invalid rows
        schema_handler: Handles schema evolution 
        force: If True, reload files even if already processed
        dry_run: If True, validate only (don't write)        
    Returns:
        Combined PyArrow Table if successful, None otherwise
    """
    # Find all matching files using glob pattern
    files = list(raw_dir.glob(pattern))
    
    if not files:
        print(f"  No files found matching {pattern}")
        return None
    
    # Initialize accumulators for all files
    all_tables = []  # Will collect all loaded tables here
    total_rows = 0   # Count total rows across all files
    files_processed = 0  # Count how many files we actually processed
    
    # Process each file individually
    for file_path in files:
        # Calculate file metadata for change detection
        file_size = file_path.stat().st_size
        
        # If content changes, hash changes
        file_hash = hashlib.md5(file_path.read_bytes()).hexdigest()
        
        # Check if this file was already processed (idempotency check)
        if not force and manifest.is_processed(table_name, str(file_path), 
                                               file_size, file_hash):
            print(f"  Skipping {file_path.name} (already processed)")
            continue
        
        try:
            # Read CSV with PyArrow
            table = pa_csv.read_csv(
                file_path,
                parse_options=pa_csv.ParseOptions(delimiter=","),
                convert_options=pa_csv.ConvertOptions(
                    column_types={field.name: pa.string() for field in schema}
                )
            )
           
            # Replace empty strings with None (NULL)
            cleaned_columns = []
            for col_name in table.column_names:
                col = table[col_name]
                # Replace empty strings with None
                cleaned = pc.if_else(
                    pc.equal(col, ""),  # If value is empty string
                    None,               # Replace with None (NULL)
                    col                 # Otherwise keep the value
                )
                cleaned_columns.append(cleaned)
            
            # Rebuild table with cleaned columns
            table = pa.table({
                name: col for name, col in zip(table.column_names, cleaned_columns)
            })
            
            # Handle schema evolution
            table = schema_handler.handle_schema_mismatch(table, schema, table_name)
            
            # Validate and cast to expected schema
            try:
                table = table.cast(schema)
                
            except Exception as e:
                # Schema validation failed for some rows
                print(f"  Schema validation failed for {file_path.name}: {e}")
                
                valid_rows = []  # Accumulate good rows here
                
                for i in range(len(table)):
                    try:
                        # Extract single row
                        row_table = table.slice(i, 1)
                        
                        # Try to cast this single row
                        row_table.cast(schema)
                        
                        # Convert to dictionary and save
                        row_dict = {
                            col: table[col][i].as_py() 
                            for col in table.column_names
                        }
                        valid_rows.append(row_dict)
                        
                    except Exception:
                        # This row is bad - add to rejects
                        row_dict = {
                            col: table[col][i].as_py() 
                            for col in table.column_names
                        }
                        rejects.add_reject(
                            table_name, 
                            row_dict, 
                            f"Schema validation: {e}", 
                            str(file_path)
                        )
                
                # If no valid rows, skip this file
                if not valid_rows:
                    continue
                
                # Convert valid rows back to PyArrow table
                table = pa.Table.from_pylist(valid_rows)
                # Try casting again (should work now with only valid rows)
                table = table.cast(schema)
            
            # Dry run mode - just validate, don't process further
            if dry_run:
                print(f" [DRY RUN] Validated {file_path.name}: {len(table):,} rows")
                continue  # Skip to next file
            
            # Add audit columns
            table = add_audit_columns(
                table, 
                str(file_path),  # Source file path
                datetime.now(timezone.utc)  # Current timestamp
            )
            
            # Update statistics and manifest
            all_tables.append(table)
            row_count = len(table)
            total_rows += row_count
            files_processed += 1
            
            # Mark this file as processed in manifest
            # Future runs will skip it (unless file changes or --force used)
            manifest.mark_processed(
                table_name, 
                str(file_path), 
                file_size, 
                file_hash, 
                row_count
            )
            
            # Log what we loaded
            print(f"  Loaded {file_path.name}: {row_count:,} rows")
            
        except Exception as e:
            # If anything goes wrong with this file, log and continue
            print(f"  Error loading {file_path.name}: {e}")
            continue
    
    # Combine all loaded tables
    if not all_tables:
        return None
    
    # Concatenate all tables into one big table
    combined = pa.concat_tables(all_tables)
    
    # Print summary
    print(f"  Total: {total_rows:,} rows from {files_processed} files")
    
    return combined


def load_jsonl_files(raw_dir: pathlib.Path, pattern: str, schema: pa.Schema,
                     table_name: str, manifest: ManifestTracker,
                     rejects: RejectsHandler, schema_handler: SchemaEvolutionHandler,
                     force: bool, dry_run: bool) -> Optional[pa.Table]:
    """
    Load JSONL (JSON Lines) files into a PyArrow Table.
    Args:
        raw_dir: Folder containing JSONL files
        pattern: Glob pattern to match filenames
        schema: Expected table schema
        table_name: Logical name of the table
        manifest: Tracks processed files
        rejects: Handler for invalid rows
        schema_handler: Handles schema evolution
        force: Reload even if processed
        dry_run: Validate only, don't write
        
    Returns:
        Combined PyArrow Table, or None if no valid data
    """
    # Find all files matching pattern
    files = list(raw_dir.glob(pattern))
    if not files:
        print(f"  No files found matching {pattern}")
        return None

    # Initialize accumulators
    all_tables = []
    total_rows = 0
    files_processed = 0

    # Process each file
    for file_path in files:
        # Calculate file metadata
        file_size = file_path.stat().st_size
        file_hash = hashlib.md5(file_path.read_bytes()).hexdigest()

        # Check if already processed (idempotency)
        if not force and manifest.is_processed(table_name, str(file_path),
                                               file_size, file_hash):
            print(f"  Skipping {file_path.name} (already processed)")
            continue

        try:
            # Read JSONL file
            with file_path.open("r", encoding="utf-8") as f:
                lines = f.readlines()

            # Accumulate valid records
            valid_records = []

            # Process each line
            for line_num, line in enumerate(lines, 1):  # Start counting at 1
                try:
                    # Parse JSON from this line
                    record = json.loads(line.strip())

                    # Extract envelope & payload
                    if "envelope" in record and "payload" in record:
                        # Flatten structure:
                        flat = {**record["envelope"]}  # Copy envelope fields
                        flat["payload_json"] = json.dumps(record["payload"])  # Stringify payload
                        valid_records.append(flat)
                    else:
                        # Missing required fields - reject this line
                        rejects.add_reject(
                            table_name, 
                            {"line": line},  # Store the raw line
                            "Missing envelope or payload",
                            f"{file_path}:line{line_num}"  # Source reference
                        )

                except json.JSONDecodeError as e:
                    # This line has malformed JSON
                    rejects.add_reject(
                        table_name, 
                        {"line": line},  # Store the raw line
                        f"Invalid JSON: {e}",
                        f"{file_path}:line{line_num}"
                    )

            # If no valid records, skip this file
            if not valid_records:
                continue

            # Dry run mode - just validate, don't continue
            if dry_run:
                print(f"  [DRY RUN] Validated {file_path.name}: {len(valid_records):,} rows")
                continue

            # Convert to PyArrow Table
            table = pa.Table.from_pylist(valid_records)
            
            # Handle schema evolution
            # table = schema_handler.handle_schema_mismatch(table, schema, table_name)

            # Add audit columns
            table = add_audit_columns(
                table, 
                str(file_path),
                datetime.now(timezone.utc)
            )

            # Update statistics
            all_tables.append(table)
            row_count = len(table)
            total_rows += row_count
            files_processed += 1

            # Mark as processed
            manifest.mark_processed(
                table_name, 
                str(file_path), 
                file_size, 
                file_hash, 
                row_count
            )

            # Log progress
            reject_count = rejects.get_reject_count(table_name)
            print(f"  Loaded {file_path.name}: {row_count:,} rows "
                  f"({reject_count} rejects)")

        except Exception as e:
            # Something went wrong with this file
            print(f"  Error loading {file_path.name}: {e}")
            continue

    # Combine all tables
    if not all_tables:
        return None

    combined = pa.concat_tables(all_tables)
    print(f"  Total: {total_rows:,} rows from {files_processed} files")

    return combined


def load_excel_file(raw_dir: pathlib.Path, filename: str, schema: pa.Schema,
                   table_name: str, manifest: ManifestTracker,
                   schema_handler: SchemaEvolutionHandler,
                   force: bool, dry_run: bool) -> Optional[pa.Table]:
    """
    Load an Excel file (.xlsx) into a PyArrow Table using DuckDB.   
    Args:
        raw_dir: Directory containing the Excel file
        filename: Name of the Excel file (e.g., "exchange_rates.xlsx")
        schema: Expected PyArrow schema
        table_name: Logical table name
        manifest: Tracker for processed files
        schema_handler: Handles schema evolution
        force: Reload even if processed
        dry_run: Validate only, don't write        
    Returns:
        PyArrow Table if successful, None otherwise
    """

    file_path = raw_dir / filename
    
    if not file_path.exists():
        print(f"  File not found: {filename}")
        return None
    
    file_size = file_path.stat().st_size
    file_hash = hashlib.md5(file_path.read_bytes()).hexdigest()
    
    # Check if already processed
    if not force and manifest.is_processed(table_name, str(file_path),
                                           file_size, file_hash):
        print(f"  Skipping {filename} (already processed)")
        return None
    
    # Create in-memory DuckDB connection
    conn = duckdb.connect(":memory:")
    
    try:
        # Try to read Excel file using DuckDB
        try:
            # read_excel is a DuckDB function that parses Excel files
            # Returns first sheet as a table
            result = conn.execute(
                f"SELECT * FROM read_excel('{file_path}')"
            ).fetch_arrow_table()  # Get result as Arrow table
            
        except:
            # If read_excel doesn't work, try spatial extension
            # (Alternative Excel reader in DuckDB)
            conn.execute("INSTALL spatial; LOAD spatial;")
            result = conn.execute(
                f"SELECT * FROM st_read('{file_path}')"
            ).fetch_arrow_table()
        
        # Dry run mode - just validate
        if dry_run:
            print(f"  [DRY RUN] Validated {filename}: {len(result):,} rows")
            return None
        
        # Handle schema evolution
        result = schema_handler.handle_schema_mismatch(result, schema, table_name)
        
        # Validate and cast to expected schema
        result = result.cast(schema)
        
        # Add audit columns
        result = add_audit_columns(
            result, 
            str(file_path), 
            datetime.now(timezone.utc)
        )
        
        # Update manifest
        row_count = len(result)
        manifest.mark_processed(
            table_name, 
            str(file_path), 
            file_size, 
            file_hash, 
            row_count
        )
        
        print(f"  Loaded {filename}: {row_count:,} rows")
        return result
        
    except Exception as e:
        # Something went wrong
        print(f"  Error loading {filename}: {e}")
        import traceback
        traceback.print_exc()  # Print full error details
        return None
        
    finally:
        # Always close the connection
        conn.close()


def load_parquet_files(raw_dir: pathlib.Path, pattern: str, schema: pa.Schema,
                      table_name: str, manifest: ManifestTracker,
                      schema_handler: SchemaEvolutionHandler,
                      force: bool, dry_run: bool) -> Optional[pa.Table]:
    """
    Load Parquet files into a PyArrow table.   
    Args:
        raw_dir: Base directory containing raw data
        pattern: Glob pattern to find Parquet files
        schema: Expected PyArrow schema
        table_name: Logical table name
        manifest: Tracker for processed files
        schema_handler: Handles schema evolution
        force: Reload even if processed
        dry_run: Validate only, don't write        
    Returns:
        Combined PyArrow Table if successful, None otherwise
    """
    # Find all matching Parquet files
    files = list(raw_dir.glob(pattern))
    if not files:
        print(f"  No files found matching {pattern}")
        return None

    # Initialize accumulators
    all_tables = []
    total_rows = 0
    files_processed = 0

    # Process each file
    for file_path in files:

        file_size = file_path.stat().st_size
        file_hash = hashlib.md5(file_path.read_bytes()).hexdigest()

        if not force and manifest.is_processed(
            table_name, str(file_path), file_size, file_hash
        ):
            print(f"  Skipping {file_path.name} (already processed)")
            continue

        try:
            # Load Parquet file
            table = pq.read_table(file_path)

            table = schema_handler.handle_schema_mismatch(table, schema, table_name)
            
            table = table.cast(schema)

            # Dry run mode
            if dry_run:
                print(f"  [DRY RUN] Validated {file_path.name}: {len(table):,} rows")
                continue

            # Add audit columns
            table = add_audit_columns(
                table,
                str(file_path),
                datetime.now(timezone.utc)
            )

            # Update statistics
            all_tables.append(table)
            row_count = len(table)
            total_rows += row_count
            files_processed += 1

            manifest.mark_processed(
                table_name,
                str(file_path),
                file_size,
                file_hash,
                row_count
            )

            print(f"  Loaded {file_path.name}: {row_count:,} rows")

        except Exception as e:
            print(f"  Error loading {file_path.name}: {e}")
            continue

    # Combine all tables
    if not all_tables:
        return None

    combined = pa.concat_tables(all_tables)
    print(f"  Total: {total_rows:,} rows from {files_processed} files")

    return combined


def load_delta_table(raw_dir: pathlib.Path, pattern: str, schema: pa.Schema,
                    table_name: str, schema_handler: SchemaEvolutionHandler,
                    dry_run: bool) -> Optional[pa.Table]:
    """
    Load a Delta Lake table into a PyArrow table.    
    Args:
        raw_dir: Base directory containing raw data
        pattern: Glob pattern to locate Delta table directory
        schema: Expected PyArrow schema
        table_name: Logical table name
        schema_handler: Handles schema evolution
        dry_run: Validate only, don't write        
    Returns:
        PyArrow Table if successful, None otherwise
    """
    # Check if Delta Lake library is available
    if not DELTA_AVAILABLE:
        print(f"  Delta Lake not available, skipping {table_name}")
        return None

    # Find Delta table directory
    delta_dirs = list(raw_dir.glob(pattern))
    if not delta_dirs:
        print(f"  No Delta table found matching {pattern}")
        return None

    # Use first matching directory
    delta_path = delta_dirs[0]

    # Load Delta table
    try:
        # Open the Delta table
        dt = DeltaTable(str(delta_path))
        
        # Convert to PyArrow table
        table = dt.to_pyarrow_table()

        # Dry run mode
        if dry_run:
            print(f"  [DRY RUN] Validated Delta table: {len(table):,} rows")
            return None

        # Handle schema evolution
        table = schema_handler.handle_schema_mismatch(table, schema, table_name)

        # Add audit columns
        table = add_audit_columns(
            table,
            str(delta_path),
            datetime.now(timezone.utc)
        )

        print(f"  Loaded Delta table: {len(table):,} rows")
        return table

    except Exception as e:
        print(f"  Error loading Delta table: {e}")
        return None


# ============================================================================
# BRONZE WRITER
# ============================================================================

def write_bronze_table(table: pa.Table, table_name: str, 
                      lake_dir: pathlib.Path,
                      partition_cols: Optional[List[str]] = None,
                      primary_keys: Optional[List[str]] = None,
                      soft_delete_detector: Optional[SoftDeleteDetector] = None,
                      file_optimizer: Optional[FileSizeOptimizer] = None):
    """
    Write a PyArrow table to the Bronze layer.    
    Args:
        table: PyArrow Table containing the data
        table_name: Name of the table (used for file paths)
        lake_dir: Base directory for data lake storage
        partition_cols: Columns to partition by (e.g., ["order_dt"])
        primary_keys: Primary key columns for UPSERT (e.g., ["customer_id"])
        soft_delete_detector: Detects deleted records (optional)
        file_optimizer: Optimizes file sizes (optional)
    """
    # Define output directories
    bronze_parquet = lake_dir / "bronze" / "parquet" / table_name
    bronze_delta = lake_dir / "bronze" / "delta" / table_name

    # ========================================================================
    # SOFT DELETE DETECTION
    # ========================================================================
    # Check for deleted records by comparing with previous version
    # Skip for append-only tables (sensors, events)
    skip_soft_deletes = table_name in ["sensors", "events"]
    
    if soft_delete_detector and primary_keys and DELTA_AVAILABLE and not skip_soft_deletes:
        # Detect deletes
        tombstones = soft_delete_detector.detect_deletes(
            table, table_name, lake_dir, primary_keys
        )
        
        # Add is_deleted and deleted_at columns to new data
        # All new/updated records are NOT deleted
        table = table.append_column(
            'is_deleted',
            pa.array([False] * len(table), type=pa.bool_())
        )
        table = table.append_column(
            'deleted_at',
            pa.array([None] * len(table), type=pa.timestamp('us'))
        )
        
        # Merge tombstones with new data
        if tombstones:
            print(f"  Including {len(tombstones):,} tombstone records")
            table = pa.concat_tables([table, tombstones])

    # ========================================================================
    # FILE SIZE OPTIMIZATION
    # ========================================================================
    # Split large tables or warn about small ones
    if file_optimizer:
        table_chunks = file_optimizer.optimize_table(table)
    else:
        table_chunks = [table]

    # ========================================================================
    # WRITE PARQUET FORMAT
    # ========================================================================
    print(f"  Writing Parquet format...")
    ensure_dir(bronze_parquet)
    
    if partition_cols:
        # PARTITIONED WRITE
        # Combine all chunks into one table for partitioned write
        combined_table = pa.concat_tables(table_chunks) if len(table_chunks) > 1 else table_chunks[0]
        
        # For sensors, check if month was derived (not in original schema)
        # should_remove_month = (table_name == "sensors" and "month" in partition_cols 
                               # and "month" in combined_table.column_names)

        # Write with partitioning
        pq.write_to_dataset(
            combined_table,
            root_path=str(bronze_parquet),
            partition_cols=partition_cols,   # Create subdirectories by these columns
            compression="snappy",            # Fast compression
            existing_data_behavior="overwrite_or_ignore"  # Overwrite existing partitions
        )
        
        # Remove month column from table after write
        # if should_remove_month:
            # combined_table = combined_table.drop(["month"])
        
        print(f"  Wrote Parquet (partitioned by {', '.join(partition_cols)}): "
              f"{bronze_parquet} ({len(combined_table):,} rows)")
    
    else:
        # NON-PARTITIONED WRITE
        # Write each optimized chunk as separate file
        for idx, chunk in enumerate(table_chunks):

            file_name = f"{table_name}_part{idx:04d}.parquet"
            file_path = bronze_parquet / file_name
            
            # Write this chunk
            pq.write_table(
                chunk,
                str(file_path),
                compression="snappy"
            )
        
        total_rows = sum(len(chunk) for chunk in table_chunks)
        print(f"  Wrote {len(table_chunks)} Parquet file(s): "
              f"{bronze_parquet} ({total_rows:,} rows)")

    # ========================================================================
    # WRITE DELTA FORMAT
    # ========================================================================
    if DELTA_AVAILABLE:
        print(f"  Writing Delta format...")
        
        try:
            # Combine chunks for Delta write
            combined_table = pa.concat_tables(table_chunks) if len(table_chunks) > 1 else table_chunks[0]
            
            # Check if Delta table already exists
            delta_exists = bronze_delta.exists()
            
            if not delta_exists:
                # ============================================================
                # INITIAL LOAD - Create new Delta table
                # ============================================================
                ensure_dir(bronze_delta)
                
                # Write with mode='append' (best practice for initial loads)
                write_deltalake(
                    str(bronze_delta),
                    combined_table,
                    mode="append",                    # Use append, not overwrite
                    partition_by=partition_cols,      # Partition if specified
                    schema_mode="merge",              # Allow schema evolution
                )
                
                print(f"  Created Delta table: {bronze_delta} ({len(combined_table):,} rows)")
            
            elif primary_keys:
                # ============================================================
                # UPSERT - Merge new data with existing
                # ============================================================
                print(f"  Performing UPSERT on Delta table...")
                
                # Open existing Delta table
                dt = DeltaTable(str(bronze_delta))
                
                # Build merge predicate
                merge_predicate = " AND ".join([
                    f"target.{pk} = source.{pk}" for pk in primary_keys
                ])
                
                # Perform MERGE operation
                # This is like SQL: MERGE INTO target USING source ON <predicate>
                (
                    dt.merge(
                        source=combined_table,          # New data
                        predicate=merge_predicate,      # How to match rows
                        source_alias="source",          # Alias for new data
                        target_alias="target"           # Alias for existing data
                    )
                    .when_matched_update_all()          # Update if exists
                    .when_not_matched_insert_all()      # Insert if new
                    .execute()                          # Execute the merge
                )
                
                print(f"  Merged {len(combined_table):,} rows into Delta table (UPSERT)")
            
            else:
                # ============================================================
                # APPEND - No primary keys, just append (no UPSERT)
                # ============================================================
                write_deltalake(
                    str(bronze_delta),
                    combined_table,
                    mode="append",                    # Append to existing
                    schema_mode="merge",              # Allow schema evolution
                )
                
                print(f"  Appended to Delta table: {bronze_delta} ({len(combined_table):,} rows)")
            
            # ============================================================
            # OPTIMIZE DELTA TABLE
            # ============================================================
            # Compact small files for better query performance
            try:
                dt = DeltaTable(str(bronze_delta))
                
                # Compact small files into larger files
                dt.optimize.compact()
                
                # Z-order for better pruning
                if partition_cols:
                    # Z-order by first partition column
                    dt.optimize.z_order(partition_cols[:1])
                
                print(f"  Optimized Delta table (compacted small files)")
            except Exception as e:
                # Optimization is nice-to-have, don't fail if it doesn't work
                print(f"   Delta optimization skipped: {e}")
        
        except Exception as e:
            print(f"  Delta write failed: {e}")
            import traceback
            traceback.print_exc()
    
    else:
        print(f"   Delta Lake not available (Parquet only)")


# ============================================================================
# MAIN INGESTION ORCHESTRATOR
# ============================================================================

class BronzeIngestion:
    """
    Main orchestrator for loading all tables to the Bronze layer.
    """

    def __init__(self, raw_dir: str, lake_dir: str, db_path: str,
                 tables: Optional[List[str]] = None, force: bool = False,
                 dry_run: bool = False):
        """
        Initialize ingestion orchestrator with paths and options.
        This sets up all the helper classes and prepares for data loading.        
        Args:
            raw_dir: Where raw source files are located (e.g., "data_raw")
            lake_dir: Where to write bronze data (e.g., "lake")
            db_path: DuckDB database for manifest tracking (e.g., "duckdb/warehouse.duckdb")
            tables: List of specific tables to load (None = all tables)
            force: Force reload even if files already processed (ignores manifest)
            dry_run: Validate only, don't write anything (test mode)
        """
        # Convert string paths to pathlib.Path objects
        self.raw_dir = pathlib.Path(raw_dir)
        self.lake_dir = pathlib.Path(lake_dir)
        
        # Determine which tables to process
        # If tables specified, use those; otherwise use all tables from SCHEMA_MAP
        if tables:
            self.tables = set(tables)
        else:
            self.tables = set(SCHEMA_MAP.keys())
        
        # Store configuration flags
        self.force = force        # Reload files even if processed
        self.dry_run = dry_run    # Test mode (no writes)

        # ====================================================================
        # ENSURE REQUIRED DIRECTORIES EXIST
        # ====================================================================
        # Create directory structure if it doesn't exist
        
        # Parquet output directory
        ensure_dir(self.lake_dir / "bronze" / "parquet")
        
        # Delta output directory
        ensure_dir(self.lake_dir / "bronze" / "delta")
        
        # Rejects directory (for bad data)
        ensure_dir(self.lake_dir / "_rejects")

        # ====================================================================
        # INITIALIZE HELPER CLASSES
        # ====================================================================
        
        # Manifest Tracker - Prevents duplicate processing
        self.manifest = ManifestTracker(db_path)
        
        # Rejects Handler - Tracks invalid rows
        self.rejects = RejectsHandler(self.lake_dir / "_rejects")
        
        # Schema Evolution Handler - Handles schema changes 
        self.schema_handler = SchemaEvolutionHandler()
        
        # Soft Delete Detector - Finds deleted records 
        self.soft_delete_detector = SoftDeleteDetector()
        
        # File Size Optimizer - Optimizes file sizes 
        self.file_optimizer = FileSizeOptimizer()

        # ====================================================================
        # INITIALIZE STATISTICS TRACKING
        # ====================================================================
        # Track performance metrics for each table
        self.stats = defaultdict(lambda: {
            "rows": 0,        # How many rows processed
            "time": 0.0,      # How long it took (seconds)
            "status": "pending"  # Status: pending/success/failed
        })

    def ingest_table(self, table_name: str):
        """
        Ingest one table from raw to bronze.
        It handles one table at a time, from loading to writing.
        Args:
            table_name: Name of the table to ingest (e.g., "customers", "orders")
        """
        # ====================================================================
        # STEP 1: VALIDATE TABLE NAME
        # ====================================================================
        if table_name not in SCHEMA_MAP:
            print(f"  Unknown table: {table_name}")
            self.stats[table_name]["status"] = "failed"
            return

        # ====================================================================
        # STEP 2: START TIMING
        # ====================================================================
        # Record start time for performance measurement
        start_time = time.time()

        # Print banner for this table
        print(f"\n{'='*70}")
        print(f"INGESTING: {table_name}")
        print(f"{'='*70}")

        # ====================================================================
        # STEP 3: GET CONFIGURATION FOR THIS TABLE
        # ====================================================================
        # Get expected schema from configuration
        schema = SCHEMA_MAP[table_name]
        
        # Get file pattern (where to find source files)
        pattern = SOURCE_PATTERNS[table_name]
        
        # Get partition configuration (how to partition this table)
        partition_cols = PARTITION_COLUMNS.get(table_name)
        
        # Get primary keys (for UPSERT operations)
        primary_keys = PRIMARY_KEYS.get(table_name)

        # Initialize table variable (will hold loaded data)
        table = None

        # ====================================================================
        # STEP 4: DETECT FORMAT AND LOAD DATA
        # ====================================================================
        # Based on file pattern, determine format and call appropriate loader
        
        # FORMAT 1: CSV FILES
        # Patterns: *.csv or **/*.csv (single file or partitioned)
        if pattern.endswith(".csv") or "/**/*.csv" in pattern:
            print(f"  Format: CSV")
            table = load_csv_files(
                self.raw_dir, 
                pattern, 
                schema, 
                table_name,
                self.manifest,        # For idempotency
                self.rejects,         # For invalid rows
                self.schema_handler,  # For schema evolution 
                self.force, 
                self.dry_run
            )

        # FORMAT 2: JSONL FILES (JSON Lines)
        # Pattern: **/*.jsonl
        elif "/**/*.jsonl" in pattern:
            print(f"  Format: JSONL (JSON Lines)")
            table = load_jsonl_files(
                self.raw_dir, 
                pattern, 
                schema, 
                table_name,
                self.manifest, 
                self.rejects, 
                self.schema_handler,
                self.force, 
                self.dry_run
            )

        # FORMAT 3: EXCEL FILES
        # Pattern: *.xlsx
        elif pattern.endswith(".xlsx"):
            print(f"  Format: Excel (.xlsx)")
            table = load_excel_file(
                self.raw_dir, 
                pattern, 
                schema, 
                table_name,
                self.manifest, 
                self.schema_handler,
                self.force, 
                self.dry_run
            )

        # FORMAT 4: PARQUET FILES
        # Pattern: *.parquet or **/*.parquet
        elif ".parquet" in pattern:
            print(f"  Format: Parquet")
            table = load_parquet_files(
                self.raw_dir, 
                pattern, 
                schema, 
                table_name,
                self.manifest, 
                self.schema_handler, 
                self.force, 
                self.dry_run
            )

        # FORMAT 5: DELTA LAKE (special case for returns table)
        # Pattern: returns/**/*
        elif "returns" in table_name:
            print(f"  Format: Delta Lake (with Parquet fallback)")
            
            # Try Delta first
            table = load_delta_table(
                self.raw_dir, 
                pattern, 
                schema, 
                table_name, 
                self.schema_handler,
                self.dry_run
            )
            
            # If Delta fails or doesn't exist, try Parquet
            if table is None:
                print(f"  Falling back to Parquet format")
                table = load_parquet_files(
                    self.raw_dir, 
                    "returns/**/*.parquet",
                    schema, 
                    table_name, 
                    self.manifest, 
                    self.schema_handler,  # For schema evolution 
                    self.force, 
                    self.dry_run
                )

        # ====================================================================
        # STEP 5: WRITE TO BRONZE LAYER (if data loaded successfully)
        # ====================================================================
        if table is not None and len(table) > 0:           
            if not self.dry_run:
                # Not a dry run, actually write the data
                print(f"\n  Writing to Bronze layer...")
                
                # Derive month partition for sensors
                if table_name == "sensors" and partition_cols and "month" in partition_cols:
                    print(f"  Deriving month partition from sensor_ts...")
                    table = derive_partition_from_timestamp(
                        table,
                        timestamp_col="sensor_ts",
                        partition_cols=partition_cols
                    )

                # Derive month partition for events
                if table_name == "events" and partition_cols and "event_date" in partition_cols:
                    print(f"  Deriving event_date partition from event_ts...")
                    table = derive_partition_from_timestamp(
                        table,
                        "event_ts",
                        partition_cols
                    )

                write_bronze_table(
                    table, 
                    table_name, 
                    self.lake_dir,
                    partition_cols=partition_cols,          # Partitioning 
                    primary_keys=primary_keys,              # For UPSERT 
                    soft_delete_detector=self.soft_delete_detector,  # Soft deletes 
                    file_optimizer=self.file_optimizer      # File optimization 
                )
            
            # ================================================================
            # STEP 6: CALCULATE TIMING AND UPDATE STATS
            # ================================================================
            elapsed = time.time() - start_time
            self.stats[table_name]["rows"] = len(table)
            self.stats[table_name]["time"] = elapsed
            self.stats[table_name]["status"] = "success"

            # Print completion message
            mode_str = "[DRY RUN] " if self.dry_run else ""
            print(f"\n{mode_str}Completed {table_name} in {format_duration(elapsed)}")
            
        else:
            # No data loaded (either no files found or all files already processed)
            elapsed = time.time() - start_time
            self.stats[table_name]["status"] = "no_data"
            print(f"\n No data loaded for {table_name} ({format_duration(elapsed)})")

    def run_all(self):
        """
        Ingest all specified tables with comprehensive logging and summary.
        It orchestrates the entire ingestion process for all tables.
        """
        # ====================================================================
        # STEP 1: DISPLAY CONFIGURATION BANNER
        # ====================================================================
        print("\n" + "="*70)
        print("BRONZE LAYER INGESTION")
        print("="*70)
        print(f"Raw directory:     {self.raw_dir}")
        print(f"Lake directory:    {self.lake_dir}")
        print(f"Tables to load:    {', '.join(sorted(self.tables))}")
        print(f"Force reload:      {self.force}")
        print(f"Dry run:           {self.dry_run}")
        print(f"Delta Lake:        {'Available' if DELTA_AVAILABLE else 'Not available'}")
        print("="*70)

        # Record overall start time
        overall_start = time.time()

        # ====================================================================
        # STEP 2: PROCESS EACH TABLE
        # ====================================================================
        # Loop through tables in alphabetical order (for consistent output)
        for table_name in sorted(self.tables):
            try:
                # Ingest this table
                self.ingest_table(table_name)
                
            except Exception as e:
                # Log the error but continue with other tables
                print(f"\nError ingesting {table_name}: {e}")
                
                # Print full stack trace for debugging
                import traceback
                traceback.print_exc()
                
                # Mark table as failed
                self.stats[table_name]["status"] = "failed"

        # ====================================================================
        # STEP 3: WRITE REJECT LOGS
        # ====================================================================
        print(f"\n{'='*70}")
        print("WRITING REJECT LOGS")
        print(f"{'='*70}")
        
        # Write all accumulated rejects to JSON files
        self.rejects.write_rejects()

        # ====================================================================
        # STEP 4: CLOSE MANIFEST DATABASE
        # ====================================================================
        # Always close database connection to flush writes and release locks
        self.manifest.close()

        # ====================================================================
        # STEP 5: CALCULATE OVERALL TIMING
        # ====================================================================
        overall_elapsed = time.time() - overall_start

        # ====================================================================
        # STEP 6: GENERATE AND DISPLAY SUMMARY REPORT
        # ====================================================================
        print(f"\n{'='*70}")
        print("INGESTION COMPLETE")
        print(f"{'='*70}")
        
        # Count successful tables
        tables_loaded = sum(
            1 for stats in self.stats.values() 
            if stats["status"] == "success"
        )
        
        # Count failed tables
        tables_failed = sum(
            1 for stats in self.stats.values() 
            if stats["status"] == "failed"
        )
        
        # Sum total rows processed
        total_rows = sum(
            stats["rows"] for stats in self.stats.values()
        )
        
        # Display summary metrics
        print(f"Mode:              {'DRY RUN (validation only)' if self.dry_run else 'Full ingestion'}")
        print(f"Tables processed:  {tables_loaded}/{len(self.tables)} successful")
        if tables_failed > 0:
            print(f"Tables failed:     {tables_failed}")
        print(f"Total rows:        {total_rows:,}")
        print(f"Total time:        {format_duration(overall_elapsed)}")
        
        # Calculate throughput (rows per second)
        if total_rows > 0 and overall_elapsed > 0:
            throughput = total_rows / overall_elapsed
            print(f"Throughput:        {throughput:,.0f} rows/sec")
        
        # ====================================================================
        # STEP 7: PER-TABLE BREAKDOWN
        # ====================================================================
        if self.stats:
            print(f"\nPer-table details:")
            print(f"{'Table':<20} {'Rows':>12} {'Time':>12} {'Status':<10}")
            print(f"{'-'*20} {'-'*12} {'-'*12} {'-'*10}")
            
            for table_name in sorted(self.stats.keys()):
                stats = self.stats[table_name]
                
                status_str = stats["status"]
                if status_str == "success":
                    status_display = "Success"
                elif status_str == "failed":
                    status_display = "Failed"
                elif status_str == "no_data":
                    status_display = " No data"
                else:
                    status_display = status_str
                
                # Only show tables that were attempted
                if stats["rows"] > 0 or stats["status"] != "pending":
                    print(f"{table_name:<20} {stats['rows']:>12,} "
                          f"{format_duration(stats['time']):>12} {status_display:<10}")
        
        # ====================================================================
        # STEP 8: SCHEMA EVOLUTION SUMMARY 
        # ====================================================================
        print(f"\n{'='*70}")
        print(self.schema_handler.get_summary())
        
        # ====================================================================
        # STEP 9: DATA QUALITY SUMMARY
        # ====================================================================
        total_rejects = sum(
            self.rejects.get_reject_count(table) 
            for table in self.tables
        )
        
        if total_rejects > 0:
            print(f"\n{'='*70}")
            print("DATA QUALITY SUMMARY")
            print(f"{'='*70}")
            print(f"Total rejected rows: {total_rejects:,}")
            print(f"Reject files written to: {self.lake_dir / '_rejects'}")
            print("\nReview reject files to investigate data quality issues.")
        
        # Final separator
        print("="*70 + "\n")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """
    Main function - parse arguments and run ingestion.
    """
    # ========================================================================
    # STEP 1: PARSE COMMAND-LINE ARGUMENTS
    # ========================================================================
    args = parse_args()
    
    # ========================================================================
    # STEP 2: PARSE TABLE FILTER
    # ========================================================================
    # Convert comma-separated string to list
    # Example: "customers,orders,products" → ["customers", "orders", "products"]
    tables_list = None
    if args.tables:
        # Split by comma and clean up each table name
        tables_list = [
            t.strip().lower()  # Remove whitespace and convert to lowercase
            for t in args.tables.split(",")
        ]
        
        # Validate that all specified tables exist in configuration
        invalid_tables = [
            t for t in tables_list 
            if t not in SCHEMA_MAP
        ]
        
        if invalid_tables:
            print(f"Error: Unknown tables: {', '.join(invalid_tables)}")
            print(f"Available tables: {', '.join(sorted(SCHEMA_MAP.keys()))}")
            sys.exit(1)  # Exit with error code
    
    # ========================================================================
    # STEP 3: CREATE AND RUN INGESTION
    # ========================================================================
    try:
        # Create the ingestion orchestrator
        ingestion = BronzeIngestion(
            raw_dir=args.raw_dir,      # Where to find raw files
            lake_dir=args.lake_dir,    # Where to write bronze data
            db_path=args.db_path,      # DuckDB manifest database
            tables=tables_list,        # Which tables to load (None = all)
            force=args.force,          # Force reload?
            dry_run=args.dry_run       # Dry run mode?
        )
        
        # Run the ingestion process
        ingestion.run_all()
        
        # Exit with success code
        sys.exit(0)
        
    except KeyboardInterrupt:
        # User pressed Ctrl+C to stop
        print("\n\n Ingestion interrupted by user")
        sys.exit(130)  # Standard exit code for Ctrl+C
        
    except Exception as e:
        # Something went wrong
        print(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)  # Exit with error code


# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================
if __name__ == "__main__":

    main()