#!/usr/bin/env python3
"""
Bronze Layer Ingestion - Option A (Custom Python)
====================================================================
Author: Lorenz Alay-ay
Created Date: 06/OCT/2025

This script loads raw data from various formats into the Bronze layer
of the data lake with:
- Schema validation using PyArrow
- Dual output: Parquet + Delta Lake
- Idempotency tracking (won't reload same files)
- Data quality checks with rejects handling
- Audit columns (ingestion timestamp, source file, row hash)
- Performance timing for each table

Bronze layer = Raw data, validated but not transformed.

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
    python load_to_bronze.py --raw-dir data_raw --lake-dir lake
"""

# ============================================================================
# IMPORTS
# ============================================================================
import argparse
import csv
import hashlib
import json
import pathlib
import sys
import time  # NEW: For timing measurements
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import duckdb
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.json as pa_json
import pyarrow.parquet as pq

# Try to import Delta Lake support
DELTA_AVAILABLE = False
try:
    from deltalake import write_deltalake, DeltaTable
    DELTA_AVAILABLE = True
except Exception:
    pass

# Import schemas
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
# CONFIGURATION
# ============================================================================

# Map table names to their schemas
SCHEMA_MAP = {
    "customers": customers_schema,
    "products": products_schema,
    "stores": stores_schema,
    "suppliers": suppliers_schema,
    "orders": orders_header_schema,
    "order_lines": orders_lines_schema,
    "events": events_schema,
    "sensors": sensors_schema,
    "exchange_rates": exchange_rates_schema,
    "shipments": shipments_schema,
    "returns": returns_day1_schema,
}

# Source file patterns for each table
SOURCE_PATTERNS = {
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "suppliers": "suppliers.csv",
    "orders": "orders/**/*.csv",            # Partitioned by date
    "order_lines": "order_lines/**/*.csv",  # Partitioned by date
    "events": "events/**/*.jsonl",          # JSONL format, partitioned
    "sensors": "sensors/**/*.csv",          # Partitioned by store/month
    "exchange_rates": "exchange_rates.xlsx",
    "shipments": "shipments_*.parquet",     # Already in Parquet
    "returns": "returns/**/*",              # Delta or Parquet
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def parse_args():
    """
    Parse command-line arguments for the data loading script.

    This allows users to specify directories, database path, 
    tables to load, and whether to force reload.

    Returns:
        argparse.Namespace: Object with parsed arguments
    """
    parser = argparse.ArgumentParser(description="Load raw data to Bronze layer")

    parser.add_argument("--raw-dir", type=str, default="data_raw",
                        help="Raw data directory (default: data_raw)")
    
    parser.add_argument("--lake-dir", type=str, default="lake",
                        help="Data lake directory (default: lake)")
    
    parser.add_argument("--db-path", type=str, default="duckdb/warehouse.duckdb",
                        help="DuckDB database path (default: duckdb/warehouse.duckdb)")
    
    parser.add_argument("--tables", type=str, default="",
                        help="Comma-separated list of tables to load (default: all)")
    
    parser.add_argument("--force", action="store_true",
                        help="Force reload even if files already processed")
    
    # NEW: Dry run mode
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate without writing (check schemas only)")
    
    return parser.parse_args()


def ensure_dir(path: pathlib.Path):
    """
    Create a directory if it doesn't exist.
    
    Args:
        path: pathlib.Path object representing the directory
    """
    path.mkdir(parents=True, exist_ok=True)


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable format.
    
    Example: 125.5 seconds → "2m 5.5s"
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string (e.g., "2m 5.5s" or "12.3s")
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

    This generates a unique fingerprint for a row, so duplicates can be detected
    even if the row IDs are different.

    Args:
        row: Dictionary representing a single row of data

    Returns:
        MD5 hash string of the row content
    """
    content = json.dumps(row, sort_keys=True, default=str)
    return hashlib.md5(content.encode()).hexdigest()


def add_audit_columns(table: pa.Table, source_file: str, 
                      ingestion_ts: datetime) -> pa.Table:
    """
    Add metadata columns to track data lineage for each row.

    Audit columns added:
    - ingestion_ts: When this data was loaded (UTC timestamp)
    - src_filename: The source file name
    - src_row_hash: Unique hash of each row for duplicate detection

    Args:
        table: PyArrow Table to enrich
        source_file: Name of the source file
        ingestion_ts: Timestamp of when the row was ingested

    Returns:
        PyArrow Table with added audit columns
    """
    num_rows = len(table)

    # Create audit columns (same value for all rows from this file)
    ingestion_col = pa.array([ingestion_ts] * num_rows, type=pa.timestamp("us"))
    filename_col = pa.array([source_file] * num_rows, type=pa.string())

    # Compute unique hash for each row (for deduplication)
    hashes = []
    for i in range(num_rows):
        row_dict = {col: table[col][i].as_py() for col in table.column_names}
        hashes.append(compute_row_hash(row_dict))
    hash_col = pa.array(hashes, type=pa.string())

    # Append audit columns to the table
    table = table.append_column("ingestion_ts", ingestion_col)
    table = table.append_column("src_filename", filename_col)
    table = table.append_column("src_row_hash", hash_col)

    return table


# ============================================================================
# MANIFEST TRACKING (Idempotency)
# ============================================================================

class ManifestTracker:
    """
    Tracks which files have been processed to avoid reloading.

    Uses DuckDB to store a manifest (a record) of processed files with checksums.
    This ensures idempotency – running the pipeline multiple times won't create duplicates.
    
    WHY THIS MATTERS:
    - If ingestion fails halfway, you can re-run without duplicating data
    - You can add new files without reprocessing everything
    - File changes are detected via hash comparison
    """

    def __init__(self, db_path: str):
        """
        Initialize the manifest tracker with a DuckDB connection.

        Args:
            db_path: Path to the DuckDB database file that will store the manifest
        """
        self.db_path = pathlib.Path(db_path)
        ensure_dir(self.db_path.parent)  # Create parent folder if needed
        self.conn = duckdb.connect(str(self.db_path))
        self._create_manifest_table()

    def _create_manifest_table(self):
        """
        Create the bronze_manifest table if it doesn't already exist.

        Table structure:
        - table_name: Logical name of the table (e.g., "customers")
        - source_file: Full path to the source file
        - file_size: File size in bytes (to detect changes)
        - file_hash: MD5 hash of file content (to detect changes)
        - processed_at: When the file was successfully loaded
        - row_count: Number of rows loaded from this file
        
        Primary key: (table_name, source_file) - uniquely identifies each file
        """
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze_manifest (
                table_name VARCHAR,
                source_file VARCHAR,
                file_size BIGINT,
                file_hash VARCHAR,
                processed_at TIMESTAMP,
                row_count BIGINT,
                PRIMARY KEY (table_name, source_file)
            )
        """)

    def is_processed(self, table_name: str, source_file: str, 
                     file_size: int, file_hash: str) -> bool:
        """
        Check if a file has already been processed.

        A file is considered processed if:
        - It exists in the manifest table
        - The file size matches
        - The file hash matches

        If size or hash changed → file was modified → needs reprocessing

        Args:
            table_name: Logical table name
            source_file: Path to the file
            file_size: Current size of the file
            file_hash: Current hash of the file content

        Returns:
            True if file already processed and unchanged, False otherwise
        """
        result = self.conn.execute("""
            SELECT COUNT(*) 
            FROM bronze_manifest
            WHERE table_name = ? 
              AND source_file = ?
              AND file_size = ?
              AND file_hash = ?
        """, [table_name, source_file, file_size, file_hash]).fetchone()

        return result[0] > 0

    def mark_processed(self, table_name: str, source_file: str,
                       file_size: int, file_hash: str, row_count: int):
        """
        Record that a file has been successfully processed.

        Uses INSERT OR REPLACE to update if file was processed before
        (e.g., if file was modified and reprocessed).

        Args:
            table_name: Logical table name
            source_file: Path to the file
            file_size: File size in bytes
            file_hash: Hash of the file content
            row_count: Number of rows loaded
        """
        self.conn.execute("""
            INSERT OR REPLACE INTO bronze_manifest
            (table_name, source_file, file_size, file_hash, processed_at, row_count)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            table_name, source_file, file_size, file_hash,
            datetime.now(timezone.utc), row_count
        ])

    def close(self):
        """Close the DuckDB database connection."""
        self.conn.close()


# ============================================================================
# DATA QUALITY & REJECTS HANDLING
# ============================================================================

class RejectsHandler:
    """
    Handles rows that fail validation.

    Invalid rows are stored in memory and can be written to a separate 
    'rejects' directory with reason codes. This allows you to investigate 
    and fix problematic rows without stopping the entire data pipeline.
    
    COMMON REASONS FOR REJECTION:
    - Schema mismatch (wrong data type)
    - Missing required fields
    - Malformed JSON
    - Invalid foreign keys
    """

    def __init__(self, rejects_dir: pathlib.Path):
        """
        Initialize the rejects handler.

        Args:
            rejects_dir: Folder where rejected rows will be saved
        """
        self.rejects_dir = rejects_dir
        ensure_dir(self.rejects_dir)
        # Dictionary to store rejected rows, grouped by table name
        self.rejects: Dict[str, List[Dict]] = defaultdict(list)

    def add_reject(self, table_name: str, row_data: Dict, 
                   reason: str, source_file: str):
        """
        Record a rejected row with the reason why it failed.

        Args:
            table_name: Name of the table where the row came from
            row_data: The actual row data that failed validation
            reason: Why the row was rejected (e.g., "invalid schema")
            source_file: The file where the row came from
        """
        self.rejects[table_name].append({
            "row_data": row_data,
            "reason": reason,
            "source_file": source_file,
            "rejected_at": datetime.now(timezone.utc).isoformat()
        })

    def write_rejects(self):
        """
        Write all rejected rows to JSON files.

        Each table will have its own file named <table_name>_rejects.json.
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
# TABLE LOADERS - One loader per source format
# ============================================================================

def load_csv_files(raw_dir: pathlib.Path, pattern: str, schema: pa.Schema,
                   table_name: str, manifest: ManifestTracker,
                   rejects: RejectsHandler, force: bool, dry_run: bool) -> Optional[pa.Table]:
    """
    Load CSV files and validate against schema.
    
    Handles both single files (e.g., customers.csv) and partitioned directories
    (e.g., orders/order_dt=2024-01-01/*.csv).
    
    PROCESS:
    1. Find all files matching the pattern
    2. Check if already processed (via manifest)
    3. Read CSV and validate schema
    4. Separate valid rows from rejects
    5. Add audit columns
    6. Update manifest
    
    Args:
        raw_dir: Base directory containing raw files
        pattern: Glob pattern to match files (e.g., "*.csv" or "**/*.csv")
        schema: Expected PyArrow schema for validation
        table_name: Logical table name for logging
        manifest: Tracker to avoid reprocessing files
        rejects: Handler for invalid rows
        force: If True, reload files even if already processed
        dry_run: If True, validate only (don't write)
        
    Returns:
        Combined PyArrow Table if successful, None otherwise
    """
    # STEP 1: Find all matching files
    files = list(raw_dir.glob(pattern))
    if not files:
        print(f"  No files found matching {pattern}")
        return None
    
    all_tables = []
    total_rows = 0
    files_processed = 0
    
    # STEP 2: Process each file
    for file_path in files:
        # Compute file metadata for change detection
        file_size = file_path.stat().st_size
        file_hash = hashlib.md5(file_path.read_bytes()).hexdigest()
        
        # Skip if already processed (unless forced)
        if not force and manifest.is_processed(table_name, str(file_path), 
                                               file_size, file_hash):
            print(f"  Skipping {file_path.name} (already processed)")
            continue
        
        try:
            # STEP 3: Read CSV with PyArrow
            # First, read as all strings to avoid type inference issues
            table = pa_csv.read_csv(
                file_path,
                parse_options=pa_csv.ParseOptions(delimiter=","),
                convert_options=pa_csv.ConvertOptions(
                    column_types={field.name: pa.string() for field in schema}
                )
            )
            
            # STEP 4: Validate and cast to expected schema
            try:
                table = table.cast(schema)
            except Exception as e:
                # Schema validation failed - try to identify bad rows
                print(f"  Schema validation failed for {file_path.name}: {e}")
                
                # Row-by-row validation to isolate problematic rows
                for i in range(len(table)):
                    try:
                        row_table = table.slice(i, 1)
                        row_table.cast(schema)
                    except Exception:
                        # This row is bad - add to rejects
                        row_dict = {col: table[col][i].as_py() 
                                  for col in table.column_names}
                        rejects.add_reject(table_name, row_dict, 
                                         f"Schema validation: {e}", 
                                         str(file_path))
                continue
            
            if dry_run:
                # Dry run mode: just validate, don't process further
                print(f"  [DRY RUN] Validated {file_path.name}: {len(table):,} rows")
                continue
            
            # STEP 5: Add audit columns (source file, timestamp, hash)
            table = add_audit_columns(table, str(file_path), 
                                     datetime.now(timezone.utc))
            
            # STEP 6: Update stats and manifest
            all_tables.append(table)
            row_count = len(table)
            total_rows += row_count
            files_processed += 1
            
            manifest.mark_processed(table_name, str(file_path), 
                                   file_size, file_hash, row_count)
            
            print(f"  Loaded {file_path.name}: {row_count:,} rows")
            
        except Exception as e:
            print(f"  Error loading {file_path.name}: {e}")
            continue
    
    if not all_tables:
        return None
    
    # STEP 7: Combine all loaded tables
    combined = pa.concat_tables(all_tables)
    print(f"  Total: {total_rows:,} rows from {files_processed} files")
    
    return combined


def load_jsonl_files(raw_dir: pathlib.Path, pattern: str, schema: pa.Schema,
                     table_name: str, manifest: ManifestTracker,
                     rejects: RejectsHandler, force: bool, dry_run: bool) -> Optional[pa.Table]:
    """
    Load JSONL (JSON Lines) files into a PyArrow Table.

    JSONL format: Each line is a separate JSON object.
    Commonly used for event logs or streaming data.

    PROCESS:
    1. Find all matching JSONL files
    2. Check manifest to skip already-processed files
    3. Read line by line, parse JSON
    4. Extract envelope + payload (flatten structure)
    5. Separate valid records from malformed JSON
    6. Convert to PyArrow table
    7. Add audit columns
    
    Args:
        raw_dir: Folder containing JSONL files
        pattern: Glob pattern to match filenames
        schema: Expected table schema
        table_name: Logical name of the table
        manifest: Tracks processed files
        rejects: Handler for invalid rows
        force: Reload even if processed
        dry_run: Validate only, don't write
        
    Returns:
        Combined PyArrow Table, or None if no valid data
    """
    # STEP 1: Find files
    files = list(raw_dir.glob(pattern))
    if not files:
        print(f"  No files found matching {pattern}")
        return None

    all_tables = []
    total_rows = 0
    files_processed = 0

    # STEP 2: Process each file
    for file_path in files:
        file_size = file_path.stat().st_size
        file_hash = hashlib.md5(file_path.read_bytes()).hexdigest()

        if not force and manifest.is_processed(table_name, str(file_path),
                                               file_size, file_hash):
            print(f"  Skipping {file_path.name} (already processed)")
            continue

        try:
            # STEP 3: Read JSONL file
            with file_path.open("r", encoding="utf-8") as f:
                lines = f.readlines()

            valid_records = []

            # STEP 4: Process each line
            for line_num, line in enumerate(lines, 1):
                try:
                    record = json.loads(line.strip())

                    # STEP 5: Extract envelope & payload
                    if "envelope" in record and "payload" in record:
                        # Flatten: copy envelope fields, keep payload as JSON string
                        flat = {**record["envelope"]}
                        flat["payload_json"] = json.dumps(record["payload"])
                        valid_records.append(flat)
                    else:
                        # Missing required fields
                        rejects.add_reject(
                            table_name, {"line": line},
                            "Missing envelope or payload",
                            f"{file_path}:line{line_num}"
                        )

                except json.JSONDecodeError as e:
                    # Malformed JSON
                    rejects.add_reject(
                        table_name, {"line": line},
                        f"Invalid JSON: {e}",
                        f"{file_path}:line{line_num}"
                    )

            if not valid_records:
                continue

            if dry_run:
                print(f"  [DRY RUN] Validated {file_path.name}: {len(valid_records):,} rows")
                continue

            # STEP 6: Convert to PyArrow Table
            table = pa.Table.from_pylist(valid_records)

            # STEP 7: Add audit columns
            table = add_audit_columns(table, str(file_path),
                                      datetime.now(timezone.utc))

            # STEP 8: Update stats
            all_tables.append(table)
            row_count = len(table)
            total_rows += row_count
            files_processed += 1

            manifest.mark_processed(
                table_name, str(file_path), file_size, file_hash, row_count
            )

            print(f"  Loaded {file_path.name}: {row_count:,} rows "
                  f"({rejects.get_reject_count(table_name)} rejects)")

        except Exception as e:
            print(f"  Error loading {file_path.name}: {e}")
            continue

    if not all_tables:
        return None

    # STEP 9: Combine all tables
    combined = pa.concat_tables(all_tables)
    print(f"  Total: {total_rows:,} rows from {files_processed} files")

    return combined


def load_excel_file(raw_dir: pathlib.Path, filename: str, schema: pa.Schema,
                   table_name: str, manifest: ManifestTracker,
                   force: bool, dry_run: bool) -> Optional[pa.Table]:
    """Load an Excel file (.xlsx) into a PyArrow Table using DuckDB."""
    
    file_path = raw_dir / filename
    if not file_path.exists():
        print(f"  File not found: {filename}")
        return None
    
    file_size = file_path.stat().st_size
    file_hash = hashlib.md5(file_path.read_bytes()).hexdigest()
    
    if not force and manifest.is_processed(table_name, str(file_path),
                                           file_size, file_hash):
        print(f"  Skipping {filename} (already processed)")
        return None
    
    conn = duckdb.connect(":memory:")
    
    try:
        # Try read_excel first
        try:
            result = conn.execute(
                f"SELECT * FROM read_excel('{file_path}')"
            ).fetch_arrow_table()
        except:
            # Fallback to spatial extension
            conn.execute("INSTALL spatial; LOAD spatial;")
            result = conn.execute(
                f"SELECT * FROM st_read('{file_path}')"
            ).fetch_arrow_table()
        
        if dry_run:
            print(f"  [DRY RUN] Validated {filename}: {len(result):,} rows")
            return None
        
        # Validate and cast schema
        result = result.cast(schema)
        
        # Add audit columns
        result = add_audit_columns(
            result, str(file_path), datetime.now(timezone.utc)
        )
        
        # Update manifest
        row_count = len(result)
        manifest.mark_processed(
            table_name, str(file_path), file_size, file_hash, row_count
        )
        
        print(f"  Loaded {filename}: {row_count:,} rows")
        return result
        
    except Exception as e:
        print(f"  Error loading {filename}: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        conn.close()


def load_parquet_files(raw_dir: pathlib.Path, pattern: str, schema: pa.Schema,
                      table_name: str, manifest: ManifestTracker,
                      force: bool, dry_run: bool) -> Optional[pa.Table]:
    """
    Load Parquet files into a PyArrow table.
    
    WHY PARQUET?
    - Columnar format (efficient for analytics)
    - Built-in compression
    - Native schema preservation
    
    Args:
        raw_dir: Base directory containing raw data
        pattern: Glob pattern to find Parquet files
        schema: Expected PyArrow schema
        table_name: Logical table name
        manifest: Tracker for processed files
        force: Reload even if processed
        dry_run: Validate only, don't write
        
    Returns:
        Combined PyArrow Table if successful, None otherwise
    """
    # STEP 1: Find files
    files = list(raw_dir.glob(pattern))
    if not files:
        print(f"  No files found matching {pattern}")
        return None

    all_tables = []
    total_rows = 0
    files_processed = 0

    # STEP 2: Process each file
    for file_path in files:
        file_size = file_path.stat().st_size
        file_hash = hashlib.md5(file_path.read_bytes()).hexdigest()

        if not force and manifest.is_processed(
            table_name, str(file_path), file_size, file_hash
        ):
            print(f"  Skipping {file_path.name} (already processed)")
            continue

        try:
            # STEP 3: Load Parquet file
            table = pq.read_table(file_path)

            # STEP 4: Validate schema
            table = table.cast(schema)

            if dry_run:
                print(f"  [DRY RUN] Validated {file_path.name}: {len(table):,} rows")
                continue

            # STEP 5: Add audit columns
            table = add_audit_columns(
                table,
                str(file_path),
                datetime.now(timezone.utc)
            )

            # STEP 6: Update stats
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

    if not all_tables:
        return None

    # STEP 7: Combine all tables
    combined = pa.concat_tables(all_tables)
    print(f"  Total: {total_rows:,} rows from {files_processed} files")

    return combined


def load_delta_table(raw_dir: pathlib.Path, pattern: str, schema: pa.Schema,
                    table_name: str, dry_run: bool) -> Optional[pa.Table]:
    """
    Load a Delta Lake table into a PyArrow table.
    
    WHY DELTA LAKE?
    - ACID transactions
    - Time travel (query historical versions)
    - Schema evolution
    - Handles updates/deletes efficiently
    
    Args:
        raw_dir: Base directory containing raw data
        pattern: Glob pattern to locate Delta table directory
        schema: Expected PyArrow schema
        table_name: Logical table name
        dry_run: Validate only, don't write
        
    Returns:
        PyArrow Table if successful, None otherwise
    """
    # STEP 1: Check if Delta Lake is available
    if not DELTA_AVAILABLE:
        print(f"  Delta Lake not available, skipping {table_name}")
        return None

    # STEP 2: Find Delta table directory
    delta_dirs = list(raw_dir.glob(pattern))
    if not delta_dirs:
        print(f"  No Delta table found matching {pattern}")
        return None

    delta_path = delta_dirs[0]

    # STEP 3: Load Delta table
    try:
        dt = DeltaTable(str(delta_path))
        table = dt.to_pyarrow_table()

        if dry_run:
            print(f"  [DRY RUN] Validated Delta table: {len(table):,} rows")
            return None

        # STEP 4: Add audit columns
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
# BRONZE WRITER - Dual output (Parquet + Delta)
# ============================================================================

def write_bronze_table(table: pa.Table, table_name: str, 
                      lake_dir: pathlib.Path):
    """
    Write a PyArrow table to the Bronze layer in two formats:
    - Parquet (widely supported, efficient storage)
    - Delta Lake (adds ACID, time travel, schema evolution)

    BRONZE LAYER STRUCTURE:
    lake/
    ├── bronze/
    │   ├─- parquet/
    │   │   └── customers/
    │   │       └── customers.parquet
    │   └── delta/
    │       └── customers/
    │           ├── _delta_log/
    │           └── part-*.parquet

    Args:
        table: PyArrow Table containing the data
        table_name: Name of the table (used for file paths)
        lake_dir: Base directory for data lake storage
    """
    # Define output directories
    bronze_parquet = lake_dir / "bronze" / "parquet" / table_name
    bronze_delta = lake_dir / "bronze" / "delta" / table_name

    # STEP 1: Write Parquet Format
    ensure_dir(bronze_parquet)
    parquet_file = bronze_parquet / f"{table_name}.parquet"
    
    pq.write_table(table, str(parquet_file), compression="snappy")
    
    print(f"  Wrote Parquet: {parquet_file} ({len(table):,} rows)")

    # STEP 2: Write Delta Format
    if DELTA_AVAILABLE:
        try:
            ensure_dir(bronze_delta)
            write_deltalake(str(bronze_delta), table, mode="overwrite")
            
            print(f"  Wrote Delta: {bronze_delta}")
        except Exception as e:
            print(f"  Delta write failed: {e}")
    else:
        print(f"  ⚠ Delta Lake not available (Parquet only)")


# ============================================================================
# MAIN INGESTION ORCHESTRATOR
# ============================================================================

class BronzeIngestion:
    """
    Main orchestrator for loading all tables to the Bronze layer.
    
    RESPONSIBILITIES:
    - Coordinate loading of all data sources
    - Track timing and statistics
    - Handle errors gracefully
    - Provide detailed logging
    
    USAGE:
        ingestion = BronzeIngestion(
            raw_dir="data_raw",
            lake_dir="lake",
            db_path="duckdb/warehouse.duckdb",
            tables=["customers", "products"],
            force=False,
            dry_run=False
        )
        ingestion.run_all()
    """

    def __init__(self, raw_dir: str, lake_dir: str, db_path: str,
                 tables: Optional[List[str]] = None, force: bool = False,
                 dry_run: bool = False):
        """
        Initialize ingestion with paths and options.
        
        Args:
            raw_dir: Where raw files are located
            lake_dir: Where to write bronze data
            db_path: DuckDB database for manifest tracking
            tables: List of specific tables to load (None = all)
            force: Force reload even if files already processed
            dry_run: Validate only, don't write anything
        """
        self.raw_dir = pathlib.Path(raw_dir)
        self.lake_dir = pathlib.Path(lake_dir)
        self.tables = set(tables) if tables else set(SCHEMA_MAP.keys())
        self.force = force
        self.dry_run = dry_run

        # Ensure directories exist
        ensure_dir(self.lake_dir / "bronze" / "parquet")
        ensure_dir(self.lake_dir / "bronze" / "delta")
        ensure_dir(self.lake_dir / "_rejects")

        # Initialize tracking systems
        self.manifest = ManifestTracker(db_path)
        self.rejects = RejectsHandler(self.lake_dir / "_rejects")

        # Track statistics per table
        self.stats = defaultdict(lambda: {"rows": 0, "time": 0.0})

    def ingest_table(self, table_name: str):
        """
        Ingest one table from raw to bronze.

        PROCESS:
        1. Detect input format (CSV, JSONL, XLSX, Parquet, Delta)
        2. Load and validate data
        3. Write to Bronze layer (Parquet + Delta)
        4. Track timing and statistics
        
        Args:
            table_name: Name of the table to ingest
        """
        # STEP 1: Validate table name
        if table_name not in SCHEMA_MAP:
            print(f"  Unknown table: {table_name}")
            return

        # STEP 2: Start timing
        start_time = time.time()

        print(f"\n{'='*70}")
        print(f"INGESTING: {table_name}")
        print(f"{'='*70}")

        # STEP 3: Get schema and file pattern
        schema = SCHEMA_MAP[table_name]
        pattern = SOURCE_PATTERNS[table_name]

        table = None

        # STEP 4: Detect format and load
        if pattern.endswith(".csv") or "/**/*.csv" in pattern:
            # CSV files (single or partitioned)
            table = load_csv_files(self.raw_dir, pattern, schema, table_name,
                                   self.manifest, self.rejects, self.force, self.dry_run)

        elif "/**/*.jsonl" in pattern:
            # JSONL files (event logs)
            table = load_jsonl_files(self.raw_dir, pattern, schema, table_name,
                                     self.manifest, self.rejects, self.force, self.dry_run)

        elif pattern.endswith(".xlsx"):
            # Excel file
            table = load_excel_file(self.raw_dir, pattern, schema, table_name,
                                    self.manifest, self.force, self.dry_run)

        elif ".parquet" in pattern:
            # Parquet files
            table = load_parquet_files(self.raw_dir, pattern, schema, table_name,
                                       self.manifest, self.force, self.dry_run)

        elif "returns" in table_name:
            # Special case: Returns can be Delta or Parquet
            table = load_delta_table(self.raw_dir, pattern, schema, table_name, self.dry_run)
            if table is None:
                # Fallback to Parquet
                table = load_parquet_files(self.raw_dir, "returns/**/*.parquet",
                                           schema, table_name, self.manifest, self.force, self.dry_run)

        # STEP 5: Write to Bronze (if not dry run)
        if table is not None and len(table) > 0:
            if not self.dry_run:
                write_bronze_table(table, table_name, self.lake_dir)
            
            # STEP 6: Calculate timing and update stats
            elapsed = time.time() - start_time
            self.stats[table_name]["rows"] = len(table)
            self.stats[table_name]["time"] = elapsed

            mode_str = "[DRY RUN] " if self.dry_run else ""
            print(f"{mode_str}Completed {table_name} in {format_duration(elapsed)}")
        else:
            elapsed = time.time() - start_time
            print(f"No data loaded for {table_name} ({format_duration(elapsed)})")

    def run_all(self):
        """
        Ingest all specified tables with comprehensive logging and summary.
        
        PROCESS:
        1. Display configuration
        2. Process each table sequentially
        3. Write reject logs
        4. Display summary statistics
        """
        # STEP 1: Display banner
        print("\n" + "="*70)
        print("BRONZE LAYER INGESTION - OPTION A (Custom Python)")
        print("="*70)
        print(f"Raw directory: {self.raw_dir}")
        print(f"Lake directory: {self.lake_dir}")
        print(f"Tables to load: {', '.join(sorted(self.tables))}")
        print(f"Force reload: {self.force}")
        print(f"Dry run: {self.dry_run}")
        print(f"Delta Lake: {'Available' if DELTA_AVAILABLE else 'Not available'}")
        print("="*70)

        overall_start = time.time()

        # STEP 2: Process each table
        for table_name in sorted(self.tables):
            try:
                self.ingest_table(table_name)
            except Exception as e:
                print(f"Error ingesting {table_name}: {e}")
                import traceback
                traceback.print_exc()

        # STEP 3: Write reject logs
        print(f"\n{'='*70}")
        print("WRITING REJECT LOGS")
        print(f"{'='*70}")
        self.rejects.write_rejects()

        # STEP 4: Close manifest
        self.manifest.close()

        # STEP 5: Calculate overall timing
        overall_elapsed = time.time() - overall_start

        # STEP 6: Display summary
        print(f"\n{'='*70}")
        print("INGESTION COMPLETE")
        print(f"{'='*70}")
        
        # Count successful tables
        tables_loaded = sum(1 for stats in self.stats.values() if stats["rows"] > 0)
        total_rows = sum(stats["rows"] for stats in self.stats.values())
        
        print(f"Mode: {'DRY RUN (validation only)' if self.dry_run else 'Full ingestion'}")
        print(f"Tables processed: {tables_loaded}/{len(self.tables)}")
        print(f"Total rows: {total_rows:,}")
        print(f"Total time: {format_duration(overall_elapsed)}")
        
        if total_rows > 0 and overall_elapsed > 0:
            print(f"Throughput: {total_rows/overall_elapsed:,.0f} rows/sec")
        
        # Per-table breakdown
        if self.stats:
            print(f"\nPer-table timing:")
            for table_name in sorted(self.stats.keys()):
                stats = self.stats[table_name]
                if stats["rows"] > 0:
                    print(f"  {table_name:20s}: {stats['rows']:>10,} rows in {format_duration(stats['time'])}")
        
        print("="*70 + "\n")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """
    Main function - parse arguments and run ingestion.
    
    This is the entry point when running:
        python load_to_bronze.py
    """
    args = parse_args()
    
    # Parse table filter
    tables_list = None
    if args.tables:
        tables_list = [t.strip().lower() for t in args.tables.split(",")]
    
    # Create and run ingestion
    ingestion = BronzeIngestion(
        raw_dir=args.raw_dir,
        lake_dir=args.lake_dir,
        db_path=args.db_path,
        tables=tables_list,
        force=args.force,
        dry_run=args.dry_run
    )
    
    ingestion.run_all()


if __name__ == "__main__":
    main()