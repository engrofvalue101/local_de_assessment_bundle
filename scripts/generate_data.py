# Generate synthetic raw data locally with controlled edge cases.
# Usage: python scripts/generate_data.py --seed 42 --out data_raw
from __future__ import annotations

import argparse
import csv
import json
import pathlib
import random
import string
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Callable, Dict, List, Optional

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import xlsxwriter
from faker import Faker

# Optional delta-rs writer (try both common API forms)
DELTA_AVAILABLE = False
try:
    from deltalake import write_deltalake  # type: ignore
    DELTA_AVAILABLE = True
except Exception:
    try:
        from deltalake.writer import write_deltalake  # type: ignore
        DELTA_AVAILABLE = True
    except Exception:
        DELTA_AVAILABLE = False

# Import schemas (single source-of-truth)
try:
    from schemas.schemas import (
        customers_schema,
        products_schema,
        stores_schema,
        suppliers_schema,
        orders_header_schema,
        orders_lines_schema,
        events_schema,
        sensors_schema,
        exchange_rates_schema,
        shipments_schema,
        returns_day1_schema,
    )
except Exception as exc:
    raise ImportError(f"Failed to import schemas/schemas.py: {exc}")

# -----------------------
# Centralized configuration
# -----------------------
TARGET_COUNTS = {
    "customers": 80_000,
    "products": 25_000,
    "stores": 5_000,
    "suppliers": 8_000,
    "orders": 1_500_000,
    "order_lines": 3_500_000,
    "events": 2_000_000,
    "sensors": 7_000_000,
    "exchange_rates": 1100,
    "shipments": 1_000_000,
    "returns": 100_000,
}

ANOMALY_RATES = {
    "customers_malformed_email": 0.007,
    "customers_duplicate_natural_key": 0.002,
    "customers_phone_null_rate": 0.02,
    "customers_addr_null_rate": 0.01,
    "products_invalid_price": 0.003,
    "stores_impossible_coords": 0.002,
    "orders_fk_violations": 0.01,
    "orders_duplicate_ids": 0.0005,
    "order_lines_invalid_product": 0.01,
    "events_malformed_json": 0.0005,
    "sensors_out_of_range": 0.003,
    "sensors_missing_ts": 0.001,
}

CSV_BATCH_SIZE = 10_000
PARQUET_BATCH_SIZE = 50_000
TZ = timezone.utc

CATEGORIES = {
    "Electronics": ["Phones", "Computers", "Audio"],
    "Home": ["Furniture", "Decor", "Kitchen"],
    "Clothing": ["Men", "Women", "Kids"],
    "Food": ["Groceries", "Beverages"],
    "Sport": ["Fitness", "Outdoor"],
    "Toys": ["Indoor", "Educational"],
}
REGIONS = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "ACT"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY"]  # "AUD"
CARRIERS = ["AUSPOST", "TOLL", "DHL", "LOCAL"]

# -----------------------
# Helpers
# -----------------------
def parse_args():
    """
    Parse command-line arguments.
    """
    p = argparse.ArgumentParser(description="Generate synthetic retail datasets.")
    p.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    p.add_argument("--out", type=str, default="data_raw", help="Output directory")
    p.add_argument("--scale", type=float, default=1.0, help="Scale factor (0.01 = 1%)")
    p.add_argument("--max-days", type=int, default=365, help="Days span for time data")
    p.add_argument("--tables", type=str, default="", help="Comma-separated subset of tables to produce")
    return p.parse_args()


def ensure_dir(p: pathlib.Path):
    """Ensure a directory exists."""
    p.mkdir(parents=True, exist_ok=True)


def fmt_decimal(d: Decimal, scale: int) -> str:
    """Format Decimal with fixed decimal places using HALF_UP rounding."""
    quant = Decimal((0, (1,), -scale))
    return str(d.quantize(quant, rounding=ROUND_HALF_UP))


def make_code(prefix: str, length: int) -> str:
    """Generate a random code with prefix and uppercase alphanumerics."""
    chars = string.ascii_uppercase + string.digits
    return prefix + "-" + "".join(random.choices(chars, k=length))


def iso(dt: datetime) -> str:
    """Return ISO 8601 string for a datetime; ensure timezone awareness."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.isoformat()


def _validate_batch(batch_cols: Dict[str, List], schema: pa.Schema, table_name: str):
    """
    Validate a batch (dict of column -> list) against a PyArrow schema by casting.
    Raises RuntimeError on failure.
    """
    try:
        pa_cols = {f.name: pa.array(batch_cols[f.name], type=pa.string()) for f in schema}
        _ = pa.table(pa_cols).cast(schema)
    except Exception as e:
        raise RuntimeError(f"Schema validation failed for {table_name}: {e}")


def _stream_csv_writer(out_file: pathlib.Path, schema: pa.Schema, n: int,
                       row_fn: Callable[[int], List], table_name: str):
    """
    Stream rows to CSV while validating batches against PyArrow schema periodically.
    """
    header = [f.name for f in schema]
    batch_cols = {c: [] for c in header}
    batch_count = 0
    ensure_dir(out_file.parent)
    with out_file.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        for i in range(1, n + 1):
            row = row_fn(i)
            writer.writerow(row)
            for col, val in zip(header, row):
                batch_cols[col].append("" if val is None else str(val))
            batch_count += 1
            if batch_count >= CSV_BATCH_SIZE:
                _validate_batch(batch_cols, schema, table_name)
                batch_cols = {c: [] for c in header}
                batch_count = 0
    if batch_count > 0:
        _validate_batch(batch_cols, schema, table_name)


def _write_parquet_table(out_path: pathlib.Path, table: pa.Table):
    """Write a PyArrow table to Parquet (Snappy compression)."""
    ensure_dir(out_path.parent)
    pq.write_table(table, str(out_path), compression="snappy")


def _append_shipments_parquet(out_dir: pathlib.Path, buf: Dict[str, List]):
    """
    Convert buffered shipments to a PyArrow table and write a parquet part file.
    Clears buffer lists after writing.
    """
    table = pa.table({
        "shipment_id": pa.array(buf["shipment_id"], type=pa.int64()),
        "order_id": pa.array(buf["order_id"], type=pa.int64()),
        "carrier": pa.array(buf["carrier"], type=pa.string()),
        "shipped_at": pa.array(buf["shipped_at"], type=pa.timestamp("us")),
        "delivered_at": pa.array([d if d is not None else None for d in buf["delivered_at"]], type=pa.timestamp("us")),
        "ship_cost": pa.array(buf["ship_cost"], type=pa.decimal128(12, 2)),
    })
    out_path = out_dir / f"shipments_part_{random.randint(1, 1_000_000)}.parquet"
    _write_parquet_table(out_path, table)
    for k in list(buf.keys()):
        buf[k].clear()
    print(f"  wrote shipments parquet part {out_path}")

# -----------------------
# Generator
# -----------------------
class DataGenerator:
    def __init__(self, out_dir: str, seed: int = 42, scale: float = 1.0,
                 max_days: int = 365, tables: Optional[List[str]] = None):
        """
        Initialize generator: seeds, faker, sizes, selection, anomalies tracker.
        """
        self.out = pathlib.Path(out_dir)
        ensure_dir(self.out)
        self.seed = int(seed)
        random.seed(self.seed)
        np.random.seed(self.seed)
        self.fake = Faker("en_AU")
        if hasattr(Faker, "seed"):
            Faker.seed(self.seed)
        self.scale = float(scale)
        self.max_days = int(max_days)
        self.sizes = {k: max(1, int(v * self.scale)) for k, v in TARGET_COUNTS.items()}
        self.tables = {t.strip().lower() for t in tables} if tables else set()
        self.anomalies: Dict[str, List[Dict]] = defaultdict(list)

    def log_anomaly(self, table: str, kind: str, details):
        """Record anomaly observation."""
        self.anomalies[table].append({"kind": kind, "details": details})

    # ----------------------
    # Customers
    # ----------------------
    def generate_customers(self):
        """Generate customers.csv with occasional anomalies."""
        if self.tables and "customers" not in self.tables:
            return

        n = self.sizes["customers"]
        out_file = self.out / "customers.csv"
        header_schema = customers_schema

        # prepare natural keys and duplicates
        keys = [make_code("CUST", 8) for _ in range(n)]
        dup_count = max(1, int(n * ANOMALY_RATES["customers_duplicate_natural_key"]))
        for _ in range(dup_count):
            src = random.randrange(n)
            dst = random.randrange(n)
            keys[dst] = keys[src]
            self.log_anomaly("customers", "duplicate_natural_key", {"index": dst + 1, "value": keys[src]})

        malformed_rate = ANOMALY_RATES["customers_malformed_email"]
        phone_null = ANOMALY_RATES["customers_phone_null_rate"]
        addr_null = ANOMALY_RATES["customers_addr_null_rate"]

        def row_fn(i: int):
            nk = keys[i - 1]
            first = self.fake.first_name()
            last = self.fake.last_name()

            if random.random() < malformed_rate:
                email = random.choice(["no-at-sign.example.com", "bad@domain", ""])
                self.log_anomaly("customers", "malformed_email", {"customer_id": i, "email": email})
            else:
                email = self.fake.email()

            phone = "" if random.random() < phone_null else self.fake.phone_number().replace(",", " ")
            address1 = "" if random.random() < addr_null else self.fake.street_address().replace(",", " ")
            address2 = ""
            city = self.fake.city()
            state = self.fake.state_abbr()
            postcode = self.fake.postcode()
            country = "AU"

            lat = -35.0 + random.random() * 10.0
            lon = 115.0 + random.random() * 20.0
            if random.random() < ANOMALY_RATES["stores_impossible_coords"]:
                lat = random.choice([999.0, -999.0])
                lon = random.choice([999.0, -999.0])
                self.log_anomaly("customers", "impossible_coords", {"customer_id": i, "lat": lat, "lon": lon})

            birth = date(1960, 1, 1) + timedelta(days=random.randint(0, 20000))
            join_ts_tz = datetime(2024, 1, 1, tzinfo=TZ) + timedelta(
                days=random.randint(0, 730), seconds=random.randint(0, 86399)
            )
            join_ts = join_ts_tz.astimezone(timezone.utc).replace(tzinfo=None)
            is_vip = random.random() < 0.15
            gdpr = random.random() < 0.95

            return [
                i, nk, first, last, email, phone, address1, address2, city, state, postcode, country,
                f"{lat:.6f}", f"{lon:.6f}",
                birth.strftime("%Y-%m-%d"),
                join_ts.strftime("%Y-%m-%d %H:%M:%S"),
                str(is_vip), str(gdpr)
            ]

        _stream_csv_writer(out_file, header_schema, n, row_fn, "customers")
        print(f"[customers] wrote {out_file} ({n} rows)")

    # run_all orchestrator
    def run_all(self):
        """Run generators in an order that helps referential realism (dims -> facts)."""
        print("Starting data generation (scaled sizes):", self.sizes)
        self.generate_customers()
        # self.generate_products()
        # self.generate_stores()
        # self.generate_suppliers()
        # self.generate_exchange_rates()
        # self.generate_orders_and_shipments()
        # self.generate_events()
        # self.generate_sensors()
        # self.generate_returns()
        # write anomalies log
        anomalies_file = self.out / "anomalies_log.json"
        with anomalies_file.open("w", encoding="utf-8") as fh:
            json.dump(self.anomalies, fh, indent=2, default=str)
        print("Wrote anomalies_log.json to", anomalies_file)
        print("Generation complete. Output:", self.out)


# -----------------------
# Entrypoint
# -----------------------
def main():
    args = parse_args()
    tables = args.tables.split(",") if args.tables else None
    tables_list = [t.strip().lower() for t in tables] if tables else None
    gen = DataGenerator(out_dir=args.out, seed=args.seed, scale=args.scale, max_days=args.max_days, tables=tables_list)
    gen.run_all()


if __name__ == "__main__":
    main()
