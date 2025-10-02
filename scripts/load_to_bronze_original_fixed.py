# Ingest raw files into Bronze (Parquet + Delta), with schema validation, partitioning,
# rejects, and manifest tracking in DuckDB.
# Usage: python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb
import argparse, pathlib, os, hashlib, json, datetime as dt
import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as pads
import pyarrow.parquet as pq
from schemas.schemas import customers_schema
from typing import Optional, List, Any

try:
    from deltalake import write_deltalake
except Exception as e:
    write_deltalake = None

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--raw', type=str, default='data_raw')
    ap.add_argument('--lake', type=str, default='lake')
    ap.add_argument('--manifest', type=str, default='duckdb/warehouse.duckdb')
    return ap.parse_args()

def ensure_dirs(lake_root):
    for sub in ['bronze/parquet','bronze/delta']:
        (lake_root/sub).mkdir(parents=True, exist_ok=True)
    (lake_root/'_rejects').mkdir(parents=True, exist_ok=True)

def init_manifest(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS manifest_processed_files (
            src_path TEXT PRIMARY KEY,
            processed_at TIMESTAMP,
            row_count BIGINT
        )
    ''')

def already_processed(conn, p): return conn.execute("SELECT 1 FROM manifest_processed_files WHERE src_path = ?", [str(p)]).fetchone() is not None
def mark_processed(conn, p, n): conn.execute("INSERT OR REPLACE INTO manifest_processed_files VALUES (?, ?, ?)", [str(p), dt.datetime.utcnow(), n])

def write_parquet_partitioned(table, base_path, partitioning=None):
    pads.write_dataset(table, base_dir=str(base_path), format='parquet', partitioning=partitioning, existing_data_behavior='overwrite_or_ignore')

# def write_delta(table, base_path, mode='append', partition_by=None, merge_schema=False):
#     if write_deltalake is None:
#         raise RuntimeError('deltalake not installed')
    # write_deltalake(
    #     str(base_path),
    #     table=table,
    #     mode=mode,
    #     partition_by=partition_by or [],
    #     overwrite_schema=False,
    #     engine='rust',
    #     schema_mode='merge' if merge_schema else 'fail'
    #     )

def write_delta(table: Any, base_path: str, mode: str = 'append', partition_by: Optional[List[str]] = None, merge_schema: bool = False):
    """
    Writes data to a Delta Lake table using the deltalake library (version 1.1.4 compatible).

    Args:
        table: The data to write (e.g., a Pandas DataFrame or PyArrow Table).
        base_path: The URI or path to the Delta Lake table.
        mode: The write mode ('append', 'overwrite', 'error_if_exists').
        partition_by: A list of column names to partition the table by.
        merge_schema: If True, enables schema evolution by merging the new schema.
    """
    
    # Start with the core required arguments
    write_kwargs = {
        'data': table,
        'mode': mode,
        'partition_by': partition_by,
    }

    # Correction: Only add 'schema_mode' if merge_schema is True, and set it to 'merge'.
    # If merge_schema is False, we omit the parameter entirely to avoid the 'error' value.
    if merge_schema:
        write_kwargs['schema_mode'] = 'merge'
    
    # Call write_deltalake with the path as the first positional argument
    write_deltalake(
        str(base_path), # Path as the first positional argument
        **write_kwargs  # Unpack all other arguments
    )

def load_customers(raw_root, lake_root, conn):
    src = raw_root/'customers.csv'
    if not src.exists(): return
    if already_processed(conn, src): return
    tbl = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
    tbl = tbl.cast(customers_schema, safe=False)
    now = pa.scalar(dt.datetime.utcnow(), type=pa.timestamp('us'))
    tbl = tbl.append_column('ingestion_ts', pa.array([now.as_py()]*len(tbl), type=pa.timestamp('us')))
    pq_base = lake_root/'bronze'/'parquet'/'customers'
    dl_base = lake_root/'bronze'/'delta'/'customers'
    write_parquet_partitioned(tbl, pq_base, partitioning=None)
    write_delta(tbl, dl_base, mode='append')
    mark_processed(conn, src, len(tbl))

def main():
    args = parse_args()
    raw_root = pathlib.Path(args.raw)
    lake_root = pathlib.Path(args.lake)
    ensure_dirs(lake_root)
    pathlib.Path(args.manifest).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(args.manifest)
    conn.execute("INSTALL delta; LOAD delta;")
    init_manifest(conn)

    load_customers(raw_root, lake_root, conn)

    print("✅ Bronze load completed for implemented loaders (extend for all tables).")

if __name__ == '__main__':
    main()