# Generate synthetic raw data locally with controlled edge cases.
# Usage: python scripts/generate_data.py --seed 42 --out data_raw
import argparse, os, pathlib, random
from datetime import datetime, timedelta, date
import numpy as np
from faker import Faker
from mimesis import Person, Address
import rstr
import pyarrow as pa
import pyarrow.parquet as pq
import xlsxwriter

# Optional delta-rs writer
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


# -----------------------
# Helpers
# -----------------------
def parse_args():
    """
    Parse command-line arguments.
    """
    ap = argparse.ArgumentParser(description="Generate synthetic retail datasets.")
    ap.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    ap.add_argument("--out", type=str, default="data_raw", help="Output directory")
    ap.add_argument("--scale", type=float, default=1.0, help="Scale factor (0.01 = 1%)")
    ap.add_argument("--tables", type=str, default="", help="Comma-separated subset of tables to produce")
    return ap.parse_args()

def ensure_dir(p):
    """Ensure a directory exists."""
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def make_code(prefix: str, length: int) -> str:
    """Generate a random code with prefix and uppercase alphanumerics."""
    chars = string.ascii_uppercase + string.digits
    return prefix + "-" + "".join(random.choices(chars, k=length))

def main():
    args = parse_args()
    random.seed(args.seed); np.random.seed(args.seed)
    out = pathlib.Path(args.out); ensure_dir(out)

    # Minimal sample generation (expand to full volumes per docs)
    fake = Faker('en_AU')
    customers_path = out/'customers.csv'
    with customers_path.open('w', encoding='utf-8') as f:
        f.write('customer_id,natural_key,first_name,last_name,email,phone,address_line1,address_line2,city,state_region,postcode,country_code,latitude,longitude,birth_date,join_ts,is_vip,gdpr_consent\n')
        for i in range(1, 1001):  # TODO raise to 80_000
            nk = 'CUST-' + rstr.rstr('A-Z0-9', 8)
            email = fake.email() if random.random()>0.1 else 'bad_email'
            lat = -44 + random.random()*10; lon = 112 + random.random()*40
            birth = date(1960,1,1) + timedelta(days=random.randint(0, 20000))
            join_ts = datetime(2024,1,1) + timedelta(days=random.randint(0, 400), seconds=random.randint(0, 86399))
            f.write(f"{i},{nk},{fake.first_name()},{fake.last_name()},{email},{fake.phone_number().replace(',',' ')},{fake.street_address().replace(',',' ')},,{fake.city().replace(',',' ')},{fake.state_abbr()},{fake.postcode()},AU,{lat:.6f},{lon:.6f},{birth.isoformat()},{join_ts.isoformat()},{str(random.random()<0.15)},{str(random.random()>0.05)}\n")

    # Shipments parquet sample
    tbl = pa.table({
        'shipment_id': pa.array(range(1, 10001), type=pa.int64()),
        'order_id': pa.array(range(1, 10001), type=pa.int64()),
        'carrier': pa.array(['AUSPOST']*10000, type=pa.string()),
        'shipped_at': pa.array([datetime(2024,1,1)+timedelta(days=i%90) for i in range(10000)], type=pa.timestamp('us')),
        'delivered_at': pa.array([datetime(2024,1,2)+timedelta(days=i%90) for i in range(10000)], type=pa.timestamp('us')),
        'ship_cost': pa.array([1995]*10000, type=pa.int64()).cast(pa.decimal128(21,2)),
    })
    pq.write_table(tbl, out/'shipments.parquet', compression='snappy')

    print(f"✅ Sample raw written to {out}. Expand to required volumes per /docs.")
if __name__ == '__main__':
    main()
