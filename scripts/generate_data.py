#!/usr/bin/env python3
"""
Synthetic Data Generator for Fake Retail Firm
===================================================================
Author: Lorenz Alay-ay
Created Date: 02/OCT/2025

This Python script generates realistic fake retail data for testing and training purposes.

BASIC USAGE:
    python -m scripts.generate_data

SMALL TEST DATASET (1% of full size):
    python -m scripts.generate_data --scale 0.01

CUSTOM OUTPUT FOLDER:
    python -m scripts.generate_data --out my_data

GENERATE ONLY SPECIFIC TABLES:
    python -m scripts.generate_data --tables customers,products,orders
"""

# ============================================================================
# IMPORTS - External libraries we need
# ============================================================================
import argparse
import csv
import json
import pathlib
import random
import string
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Callable, Dict, List, Optional

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import xlsxwriter
from faker import Faker
from scripts.config import *

# Try to import Delta Lake support (optional - falls back to Parquet if not available)
DELTA_AVAILABLE = False
try:
    from deltalake import write_deltalake
    DELTA_AVAILABLE = True
except Exception:
    pass

# Import the data schemas (structure/rules for each table)
try:
    from schemas.schemas import (
        customers_schema, products_schema, stores_schema, suppliers_schema,
        orders_header_schema, orders_lines_schema, events_schema, sensors_schema,
        exchange_rates_schema, shipments_schema, returns_day1_schema,
    )
except Exception as exc:
    raise ImportError(f"Cannot find schemas/schemas.py: {exc}")

# ============================================================================
# HELPER FUNCTIONS - Utility functions used throughout the script
# ============================================================================

def parse_args():
    """Parse command-line arguments to customize data generation."""
    parser = argparse.ArgumentParser(description="Generate synthetic retail datasets")
    parser.add_argument("--seed", type=int, default=42,
                       help="Random seed for reproducibility (default: 42)")
    parser.add_argument("--out", type=str, default="data_raw",
                       help="Output directory (default: data_raw)")
    parser.add_argument("--scale", type=float, default=1.0,
                       help="Scale factor - use 0.01 for 1%% of data (default: 1.0)")
    parser.add_argument("--max-days", type=int, default=1028,
                       help="Time span for date-based data in days (default: 1028 - Jan 2023 to Oct 2025)")
    parser.add_argument("--tables", type=str, default="",
                       help="Generate only specific tables, comma-separated (e.g., customers,products)")
    return parser.parse_args()


def ensure_dir(path: pathlib.Path):
    """Create a directory if it doesn't exist."""
    path.mkdir(parents=True, exist_ok=True)


def fmt_decimal(value: Decimal, decimal_places: int) -> str:
    """
    Format a decimal number with exact number of decimal places.
    
    Example: fmt_decimal(Decimal("10.5"), 2) returns "10.50"
    """
    quantizer = Decimal((0, (1,), -decimal_places))
    return str(value.quantize(quantizer, rounding=ROUND_HALF_UP))


def make_code(prefix: str, length: int) -> str:
    """
    Generate a random alphanumeric code.
    
    Example: make_code("CUST", 8) might return "CUST-A5B9C2D7"
    """
    chars = string.ascii_uppercase + string.digits
    return prefix + "-" + "".join(random.choices(chars, k=length))


def iso(dt: datetime) -> str:
    """Convert a datetime to ISO 8601 format string."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.isoformat()


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable format.
    
    Example: 125.5 seconds → "2m 5.5s"
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
        
        
def _validate_batch(batch_data: Dict[str, List], schema: pa.Schema, table_name: str):
    """
    Check if a batch of data matches the expected schema.
    Raises an error if validation fails.
    """
    try:
        # Convert empty strings to None for proper NULL handling
        cleaned_data = {}
        for field in schema:
            col_data = batch_data[field.name]
            # Replace empty strings with None for nullable fields
            cleaned_data[field.name] = [None if val == "" else val for val in col_data]
        
        # Convert to string columns
        string_cols = {field.name: pa.array(cleaned_data[field.name], type=pa.string()) 
                      for field in schema}
        
        # Try to cast to the actual schema types
        _ = pa.table(string_cols).cast(schema)
    except Exception as e:
        raise RuntimeError(f"Data validation failed for {table_name}: {e}")


def _stream_csv_writer(output_file: pathlib.Path, schema: pa.Schema, num_rows: int,
                       row_generator: Callable[[int], List], table_name: str):
    """
    Write data to CSV file one row at a time, validating in batches.
    
    This approach uses less memory than building everything in memory first.
    """
    header = [field.name for field in schema]
    batch_data = {col: [] for col in header}
    batch_count = 0
    
    # Create output directory if needed
    ensure_dir(output_file.parent)
    
    # Write CSV file
    with output_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(header)  # Write header row
        
        # Generate and write each row
        for i in range(1, num_rows + 1):
            row = row_generator(i)
            writer.writerow(row)
            
            # Collect data for validation
            for col_name, value in zip(header, row):
                batch_data[col_name].append("" if value is None else str(value))
            batch_count += 1
            
            # Validate every CSV_BATCH_SIZE rows
            if batch_count >= CSV_BATCH_SIZE:
                _validate_batch(batch_data, schema, table_name)
                batch_data = {col: [] for col in header}
                batch_count = 0
    
    # Validate any remaining rows
    if batch_count > 0:
        _validate_batch(batch_data, schema, table_name)


def _write_parquet_table(output_path: pathlib.Path, table: pa.Table):
    """Write a PyArrow table to a Parquet file with compression."""
    ensure_dir(output_path.parent)
    pq.write_table(table, str(output_path), compression="snappy")


def _append_shipments_parquet(output_dir: pathlib.Path, buffer: Dict[str, List]):
    """
    Write buffered shipment data to a Parquet file.
    Clears the buffer after writing.
    """
    # Convert buffer to PyArrow table
    table = pa.table({
        "shipment_id": pa.array(buffer["shipment_id"], type=pa.int64()),
        "order_id": pa.array(buffer["order_id"], type=pa.int64()),
        "carrier": pa.array(buffer["carrier"], type=pa.string()),
        "shipped_at": pa.array(buffer["shipped_at"], type=pa.timestamp("us")),
        "delivered_at": pa.array(buffer["delivered_at"], type=pa.timestamp("us")),
        "ship_cost": pa.array(buffer["ship_cost"], type=pa.decimal128(12, 2)),
    })
    
    # Write to file
    output_path = output_dir / f"shipments_part_{random.randint(1, TARGET_COUNTS['shipments'])}.parquet"
    _write_parquet_table(output_path, table)
    
    # Clear buffer
    for key in buffer.keys():
        buffer[key].clear()
        
        
def date_or_empty(dt: Optional[date]) -> str:
    """Convert date to string, or empty string if None."""
    return dt.strftime("%Y-%m-%d") if dt else ""


# ============================================================================
# MAIN DATA GENERATOR CLASS
# ============================================================================

class DataGenerator:
    """
    Main class that generates all synthetic retail data.
    
    Usage:
        gen = DataGenerator(out_dir="data_raw", seed=42, scale=0.01)
        gen.run_all()
    """
    
    def __init__(self, out_dir: str, seed: int = 42, scale: float = 1.0,
                 max_days: int = 365, tables: Optional[List[str]] = None):
        """
        Initialize the data generator.
        
        Args:
            out_dir: Where to save generated data
            seed: Random seed for reproducibility
            scale: Multiplier for data volumes (0.01 = 1% of target)
            max_days: Time span for date-based data
            tables: List of specific tables to generate (None = all)
        """
        self.out = pathlib.Path(out_dir)
        ensure_dir(self.out)
        
        # Set random seeds for reproducibility
        self.seed = int(seed)
        random.seed(self.seed)
        np.random.seed(self.seed)
        
        # Initialize Faker for realistic fake data (Australian locale)
        self.fake = Faker("en_AU")
        if hasattr(Faker, "seed"):
            Faker.seed(self.seed)
        
        # Calculate actual row counts based on scale factor
        self.scale = float(scale)
        self.max_days = int(max_days)
        self.sizes = {table: max(1, int(count * self.scale)) 
                     for table, count in TARGET_COUNTS.items()}
        
        # Track which tables to generate (empty set = generate all tables)
        self.tables = {t.strip().lower() for t in tables} if tables else set()
        
        # Track anomalies we inject (for later analysis)
        self.anomalies: Dict[str, List[Dict]] = defaultdict(list)
    
    def log_anomaly(self, table: str, anomaly_type: str, details):
        """Record that we injected an anomaly."""
        self.anomalies[table].append({"kind": anomaly_type, "details": details})
        
    def maybe_inject_anomaly(self, table: str, anomaly_key: str, normal_value, 
                            anomaly_value, details: dict) -> any:
        """Conditionally inject an anomaly, returning either normal or anomaly value."""
        rate = ANOMALY_RATES.get(anomaly_key, 0.0)
        if random.random() < rate:
            self.log_anomaly(table, anomaly_key, details)
            return anomaly_value
        return normal_value
        
    def inject_duplicates(self, items: List, table: str, anomaly_type: str, rate_key: str):
        """Inject duplicate values into a list by copying random elements."""
        num_items = len(items)
        num_dups = max(1, int(num_items * ANOMALY_RATES[rate_key]))
        
        for _ in range(num_dups):
            source_idx = random.randrange(num_items)
            target_idx = random.randrange(num_items)
            items[target_idx] = items[source_idx]
            self.log_anomaly(table, anomaly_type,
                            {"from": source_idx + 1, "to": target_idx + 1, 
                             "value": items[source_idx]})
        
        return num_dups

    # ========================================================================
    # TABLE GENERATORS - One method per table
    # ========================================================================

    def generate_customers(self):
        """
        Generate customer master data with occasional data quality issues.
        
        PERFORMANCE: Uses streaming CSV writer - processes one row at a time
        to minimize memory usage.
        
        ANOMALIES INJECTED:
        - Malformed email addresses (~0.5%)
        - Duplicate customer codes (~0.2%)
        - Missing phone numbers (~5%)
        - Missing addresses (~2%)
        - Impossible coordinates (~0.1%)
        """
        # --------------------------------------------
        # STEP 1: Skip if not selected
        # --------------------------------------------
        # If user specified --tables and "customers" is not in that list → skip
        if self.tables and "customers" not in self.tables:
            return
        
        # Start timing this function
        start_time = time.time()
        print(f"\n{'='*70}")
        print(f"[customers] Starting generation...")
        print(f"{'='*70}")
        
        # --------------------------------------------
        # STEP 2: Setup basics
        # --------------------------------------------
        num_customers = self.sizes["customers"]     # Number of customers to generate
        output_file = self.out / "customers.csv"    # Output file path
        schema = customers_schema                   # Schema definition for validation
        
        print(f"[customers] Target rows: {num_customers:,}")
        print(f"[customers] Output file: {output_file}")
        
        # --------------------------------------------
        # STEP 3: PRE-GENERATE CUSTOMER CODES
        # --------------------------------------------
        print(f"[customers] Generating unique customer codes...")
        customer_codes = [make_code("CUST", 8) for _ in range(num_customers)]
        
        # Inject duplicates for anomaly testing                      
        num_dups = self.inject_duplicates(customer_codes, "customers", 
                                  "duplicate_natural_key", 
                                  "customers_duplicate_natural_key")
        
        print(f"[customers] Injected {num_dups:,} duplicate codes")
        
        # --------------------------------------------
        # STEP 4: ANOMALY RATES (for reference)
        # --------------------------------------------
        email_error_rate = ANOMALY_RATES["customers_malformed_email"]
        phone_null_rate = ANOMALY_RATES["customers_phone_null_rate"]
        addr_null_rate = ANOMALY_RATES["customers_addr_null_rate"]
        impossible_coords_rate = ANOMALY_RATES["stores_impossible_coords"]
        
        # --------------------------------------------
        # STEP 5: ROW GENERATOR FUNCTION
        # --------------------------------------------
        def generate_customer_row(customer_id: int) -> List:
            """
            Generate ONE customer row.
            
            This function is called once for each customer_id.
            It returns a list of values matching the schema columns.
            """
            # Base identifiers
            code = customer_codes[customer_id - 1]  # Get pre-generated code
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            
            # Email field (sometimes malformed)
            if random.random() < email_error_rate:
                # Inject bad email for testing
                email = random.choice(["no-at-sign.example.com", "bad@domain", ""])
                self.log_anomaly("customers", "malformed_email", 
                               {"customer_id": customer_id, "email": email})
            else:
                email = self.fake.email()
            
            # Phone field (sometimes missing)
            if random.random() < phone_null_rate:
                phone = ""
                self.log_anomaly("customers", "missing_phone_num", 
                               {"customer_id": customer_id})
            else:
                phone = self.fake.phone_number().replace(",", " ")
            
            # Address fields (sometimes missing)
            if random.random() < addr_null_rate:
                address1 = ""
                self.log_anomaly("customers", "missing_address_details",
                                 {"customer_id": customer_id})
            else:
                address1 = self.fake.street_address().replace(",", " ")
            
            address2 = ""  # Optional field left empty
            city = self.fake.city()
            state = self.fake.state_abbr()
            postcode = self.fake.postcode()
            country = "AU"
            
            # Coordinates (sometimes impossible values)
            latitude = -35.0 + random.random() * 10.0
            longitude = 115.0 + random.random() * 20.0
            if random.random() < impossible_coords_rate:
                latitude = random.choice([999.0, -999.0])
                longitude = random.choice([999.0, -999.0])
                self.log_anomaly("customers", "impossible_coords", 
                               {"customer_id": customer_id, "lat": latitude, "lon": longitude})
            
            # Dates
            birth_date = date(1960, 1, 1) + timedelta(days=random.randint(0, 20000))
            join_timestamp = datetime(2023, 1, 1, tzinfo=TZ) + timedelta(
                days=random.randint(0, 730), seconds=random.randint(0, 86399))
            join_timestamp = join_timestamp.astimezone(timezone.utc).replace(tzinfo=None)
            
            # Flags
            is_vip = random.random() < FLAG_RATES["vip_cust"]
            gdpr_consent = random.random() < FLAG_RATES["gdpr_consent"]
            
            # Return one complete customer row (must match schema column order!)
            return [
                customer_id, code, first_name, last_name, email, phone,
                address1, address2, city, state, postcode, country,
                f"{latitude:.6f}", f"{longitude:.6f}",
                birth_date.strftime("%Y-%m-%d"),
                join_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                str(is_vip), str(gdpr_consent)
            ]
        
        # --------------------------------------------
        # STEP 6: WRITE TO FILE (streaming)
        # --------------------------------------------
        print(f"[customers] Writing rows to CSV...")
        _stream_csv_writer(output_file, schema, num_customers, generate_customer_row, "customers")
        
        # Calculate and display timing
        elapsed = time.time() - start_time
        print(f"[customers] Generated {num_customers:,} rows in {format_duration(elapsed)}")
        print(f"[customers] Output: {output_file}")
        print(f"{'='*70}\n")

    def generate_products(self):
        """
        Generate product catalog with occasional invalid prices.
        
        PERFORMANCE: Streaming CSV writer for memory efficiency.
        
        ANOMALIES INJECTED:
        - Invalid negative prices (~0.3%)
        - Missing prices (~0.1%)
        """
        # --- STEP 1: Skip if not selected ---
        if self.tables and "products" not in self.tables:
            return
        
        start_time = time.time()
        print(f"\n{'='*70}")
        print(f"[products] Starting generation...")
        print(f"{'='*70}")
        
        # --- STEP 2: Setup variables ---
        num_products = self.sizes["products"]
        output_file = self.out / "products.csv"
        schema = products_schema
        invalid_price_rate = ANOMALY_RATES["products_invalid_price"]
        categories = list(CATEGORIES.keys())
        
        print(f"[products] Target rows: {num_products:,}")
        print(f"[products] Output file: {output_file}")
        
        # --- STEP 3: Row generator function ---
        def generate_product_row(product_id: int) -> List:
            """Generate one product row with possible anomalies."""
            
            # Unique product code
            sku = make_code("SKU", 6)
            
            # Product name (two random words)
            name = f"{self.fake.word().capitalize()} {self.fake.word().capitalize()}"
            
            # Category and subcategory
            category = random.choice(categories)
            subcategory = random.choice(CATEGORIES[category])               
                
            # Generate base price
            base_price = Decimal(str(abs(np.random.normal(loc=50.0, scale=30.0))))
            price = fmt_decimal(base_price.quantize(Decimal("0.0001")), 4)

            # Inject anomalies
            if random.random() < invalid_price_rate:
                anomaly_type = random.choice(["invalid", "missing"])
                price = "-10.0000" if anomaly_type == "invalid" else ""
                self.log_anomaly("products", f"{anomaly_type}_price", 
                                {"product_id": product_id, "value": price})
            
            currency = "AUD"
            
            # Some products are discontinued
            is_discontinued = random.random() < FLAG_RATES["discontinued_product"]
            
            # Dates
            introduced_date = date(2015, 1, 1) + timedelta(days=random.randint(0, 3650))
            
            if is_discontinued:
                if random.random() > FLAG_RATES["null_discontinued_date"]:
                    discontinued_date = introduced_date + timedelta(days=random.randint(30, 2000))
                else:
                    discontinued_date = None # NULL for legacy discontinued products
            else:
                discontinued_date = None # NULL for active products
            
            return [
                product_id, sku, name, category, subcategory, price, currency,
                str(is_discontinued),
                date_or_empty(introduced_date),
                date_or_empty(discontinued_date)
            ]
        
        # --- STEP 4: Stream rows into CSV ---
        print(f"[products] Writing rows to CSV...")
        _stream_csv_writer(output_file, schema, num_products, generate_product_row, "products")
        
        elapsed = time.time() - start_time
        print(f"[products] Generated {num_products:,} rows in {format_duration(elapsed)}")
        print(f"[products] Output: {output_file}")
        print(f"{'='*70}\n")

    def generate_stores(self):
        """Generate store locations with occasional invalid coordinates."""
        
        if self.tables and "stores" not in self.tables:
            return
        
        start_time = time.time()
        print(f"\n{'='*70}")
        print(f"[stores] Starting generation...")
        print(f"{'='*70}")
        
        num_stores = self.sizes["stores"]
        output_file = self.out / "stores.csv"
        schema = stores_schema
        
        print(f"[stores] Target rows: {num_stores:,}")
        print(f"[stores] Output file: {output_file}")
        
        # Pre-generate store codes (with some duplicates)
        store_codes = [make_code("STORE", 6) for _ in range(num_stores)]
        num_dups = self.inject_duplicates(store_codes, "stores",
                                          "duplicate_store_code",
                                          "stores_duplicate")
        
        print(f"[stores] Injected {num_dups:,} duplicate codes")
        
        def generate_store_row(store_id: int) -> List:
            """Generate one store row with possible anomalies."""
            
            code = store_codes[store_id - 1]
            name = f"{self.fake.company()} {store_id}"
            channel = random.choice(["web", "pos"])
            region = random.choice(REGIONS)
            state = region
            
            # Get coordinate range for the selected state
            if state in AUSTRALIA_COORDS:
                lat_range = AUSTRALIA_COORDS[state]["lat"]
                lon_range = AUSTRALIA_COORDS[state]["lon"]
                
                # Generate coordinates within the state's actual boundaries
                latitude = lat_range[0] + random.random() * (lat_range[1] - lat_range[0])
                longitude = lon_range[0] + random.random() * (lon_range[1] - lon_range[0])
            else:
                # Fallback for any unexpected region
                latitude = -35.0 + random.random() * 10.0
                longitude = 115.0 + random.random() * 20.0
            
            # Apply anomaly for impossible coordinates AFTER generating real ones
            if random.random() < ANOMALY_RATES["stores_impossible_coords"]:
                latitude = random.choice([999.0, -999.0])
                longitude = random.choice([999.0, -999.0])
                self.log_anomaly("stores", "impossible_coords", 
                                {"store_id": store_id, "lat": latitude, "lon": longitude})
            
            # Dates
            open_date = date(2000, 1, 1) + timedelta(days=random.randint(0, 9000))
            
            if random.random() <= FLAG_RATES["stores_closed"]:
                close_date = open_date + timedelta(days=random.randint(30, 5000))
            else:
                close_date = None  # NULL for active stores
            
            return [
                store_id, code, name, channel, region, state,
                f"{latitude:.6f}", f"{longitude:.6f}",
                date_or_empty(open_date), 
                date_or_empty(close_date)
            ]
        
        print(f"[stores] Writing rows to CSV...")
        _stream_csv_writer(output_file, schema, num_stores, generate_store_row, "stores")
        
        elapsed = time.time() - start_time
        print(f"[stores] Generated {num_stores:,} rows in {format_duration(elapsed)}")
        print(f"[stores] Output: {output_file}")
        print(f"{'='*70}\n")

    def generate_suppliers(self):
        """Generate supplier information as a CSV file."""
        
        if self.tables and "suppliers" not in self.tables:
            return
        
        start_time = time.time()
        print(f"\n{'='*70}")
        print(f"[suppliers] Starting generation...")
        print(f"{'='*70}")
        
        num_suppliers = self.sizes["suppliers"]
        output_file = self.out / "suppliers.csv"
        schema = suppliers_schema
        
        print(f"[suppliers] Target rows: {num_suppliers:,}")
        print(f"[suppliers] Output file: {output_file}")
        
        def generate_supplier_row(supplier_id: int) -> List:
            """Generate one supplier row with fake/random values."""
            code = make_code("SUPP", 6)
            name = self.fake.company()
            country = random.choice(["AU", "CN", "US", "VN", "TH", "ID", "PH"])
            lead_time_days = random.randint(1, 90)
            is_preferred = random.random() < FLAG_RATES["preferred"]
            
            return [supplier_id, code, name, country, lead_time_days, str(is_preferred)]
        
        print(f"[suppliers] Writing rows to CSV...")
        _stream_csv_writer(output_file, schema, num_suppliers, generate_supplier_row, "suppliers")
        
        elapsed = time.time() - start_time
        print(f"[suppliers] Generated {num_suppliers:,} rows in {format_duration(elapsed)}")
        print(f"[suppliers] Output: {output_file}")
        print(f"{'='*70}\n")

    def generate_exchange_rates(self):
        """
        Generate daily exchange rates in Excel format.
        Uses random walk to simulate realistic rate fluctuations.
        """
        
        if self.tables and "exchange_rates" not in self.tables:
            return
        
        start_time = time.time()
        print(f"\n{'='*70}")
        print(f"[exchange_rates] Starting generation...")
        print(f"{'='*70}")
        
        num_days = self.sizes["exchange_rates"]
        output_file = self.out / "exchange_rates.xlsx"
        
        print(f"[exchange_rates] Target days: {num_days:,}")
        print(f"[exchange_rates] Output file: {output_file}")
        
        # Create Excel workbook
        workbook = xlsxwriter.Workbook(str(output_file))
        worksheet = workbook.add_worksheet("rates")
        
        # Write headers
        headers = [field.name for field in exchange_rates_schema]
        for col, header in enumerate(headers):
            worksheet.write(0, col, header)
        
        # Initialize exchange rates (AUD base)
        start_date = date.today() - timedelta(days=num_days)
        current_rates = {
            "USD": Decimal("0.6947"),
            "EUR": Decimal("0.6540"),
            "GBP": Decimal("0.4897"),
            "JPY": Decimal("97.91")
        }
        
        print(f"[exchange_rates] Simulating random walk for {len(CURRENCIES)} currencies...")
        
        # Random walk simulation
        row = 1
        for day in range(num_days):
            current_date = start_date + timedelta(days=day)
            
            for currency in CURRENCIES:
                # Random walk step
                drift = Decimal(str(np.random.normal(scale=0.005)))
                new_rate = (current_rates[currency] + drift).quantize(
                    Decimal("0.00000001"), rounding=ROUND_HALF_UP
                )
                current_rates[currency] = new_rate
                
                # Write row
                worksheet.write(row, 0, current_date.isoformat())
                worksheet.write(row, 1, currency)
                worksheet.write(row, 2, float(new_rate))
                row += 1
        
        workbook.close()
        total_rows = num_days * len(CURRENCIES)
        elapsed = time.time() - start_time
        
        print(f"[exchange_rates] Generated {total_rows:,} rows in {format_duration(elapsed)}")
        print(f"[exchange_rates] Output: {output_file}")
        print(f"{'='*70}\n")

    def generate_orders_and_shipments(self):
            """
            Generate orders, order lines, and shipments together - OPTIMIZED VERSION.
            
            PERFORMANCE OPTIMIZATIONS:
            1. Batch generation: Generate 10,000 rows at a time
            2. Buffer accumulation: Collect rows in memory before writing
            3. Vectorized calculations: Use NumPy for faster math
            4. Reduced file I/O: Write partitions only when buffer is full
            
            PARTITIONING:
            - orders/order_dt=2024-01-15/part-*.csv
            - order_lines/order_dt=2024-01-15/part-*.csv
            - shipments_part_*.parquet
            """
            
            # Skip if not requested
            if self.tables and not ({"orders", "order_lines", "shipments"} & self.tables):
                return
            
            start_time = time.time()
            print(f"\n{'='*70}")
            print(f"[orders/lines/shipments] Starting generation...")
            print(f"{'='*70}")
            
            # Get sizes
            num_orders = self.sizes["orders"]
            num_products = self.sizes["products"]
            num_customers = self.sizes["customers"]
            num_stores = self.sizes["stores"]
            
            print(f"[orders] Target rows: {num_orders:,}")
            print(f"[orders] Generating with order lines and shipments...")
            
            # Create directories
            orders_dir = self.out / "orders"
            lines_dir = self.out / "order_lines"
            ensure_dir(orders_dir)
            ensure_dir(lines_dir)
            
            # Pre-determine anomalies
            fk_violation_count = int(num_orders * ANOMALY_RATES["orders_fk_violations"])
            fk_violations = set(random.sample(range(1, num_orders + 1), fk_violation_count)) if fk_violation_count > 0 else set()
            
            dup_count = max(1, int(num_orders * ANOMALY_RATES["orders_duplicate_ids"]))
            duplicate_ids = set(random.sample(range(1, num_orders + 1), dup_count))
            
            print(f"[orders] Injected {fk_violation_count:,} FK violations, {dup_count:,} duplicates")
            
            # Buffers for accumulating data
            order_buffers: Dict[str, list] = defaultdict(list)
            line_buffers: Dict[str, list] = defaultdict(list)
            shipment_buffer = {
                "shipment_id": [], "order_id": [], "carrier": [],
                "shipped_at": [], "delivered_at": [], "ship_cost": []
            }
            next_shipment_id = 1
            
            # Date/time setup
            start_datetime = datetime(2023, 1, 1, tzinfo=TZ)
            days_span = max(1, self.max_days)
            
            def get_order_timestamp(order_id: int) -> datetime:
                """Calculate timestamp for an order based on its ID."""
                day_offset = order_id % days_span
                seconds = (order_id * 97) % 86400
                micros = (order_id * 13) % 1_000_000
                return start_datetime + timedelta(days=day_offset, seconds=seconds, microseconds=micros)
            
            orders_written = 0
            lines_written = 0
            
            # Progress tracking
            progress_interval = max(1, num_orders // 10)  # Report every 10%
            
            print(f"[orders] Processing orders in batches...")
            
            # Main loop: Generate each order
            for order_id in range(1, num_orders + 1):
                
                # Progress indicator
                if order_id % progress_interval == 0:
                    pct = (order_id / num_orders) * 100
                    print(f"[orders] Progress: {order_id:,}/{num_orders:,} ({pct:.0f}%)")
                
                # Get timestamp for this order
                timestamp_tz = get_order_timestamp(order_id)
                timestamp_naive = timestamp_tz.astimezone(timezone.utc).replace(tzinfo=None)
                order_date = timestamp_tz.date().isoformat()
                
                # Foreign key violations (bad customer/store IDs)
                if order_id in fk_violations:
                    customer_id = num_customers + random.randint(1, 1000)
                    store_id = num_stores + random.randint(1, 1000)
                    self.log_anomaly("orders", "fk_violation",
                                    {"order_id": order_id, "customer_id": customer_id, "store_id": store_id})
                else:
                    customer_id = random.randint(1, num_customers)
                    store_id = random.randint(1, num_stores)
                
                # Order details
                channel = random.choice(["web", "pos"])
                payment_method = random.choice(["card", "cash", "paypal", "giftcard"])
                coupon = "" if random.random() > 0.05 else f"CPN{random.randint(1000,9999)}"
                shipping_fee = Decimal(str(max(0.0, np.random.normal(loc=5.0, scale=3.0)))).quantize(Decimal("0.01"))
                
                # The "order header" row
                order_row = [
                    order_id, timestamp_naive, order_date,
                    customer_id, store_id, channel, payment_method,
                    coupon, fmt_decimal(shipping_fee, 2), "AUD"
                ]
                order_buffers[order_date].append(order_row)
                orders_written += 1
                
                # Generate order lines (items inside the order)
                num_lines = (order_id * 7) % 5 + 1   # 1 to 5 lines
                for line_num in range(1, num_lines + 1):
                    # Product ID - sometimes invalid
                    if random.random() < ANOMALY_RATES["order_lines_invalid_product"]:
                        product_id = num_products + random.randint(1, 1000)
                        self.log_anomaly("order_lines", "invalid_product_id",
                                        {"order_id": order_id, "line": line_num, "product_id": product_id})
                    else:
                        product_id = random.randint(1, num_products)
                    
                    # Generate NORMAL quantity
                    quantity = max(1, int(abs(int(np.random.poisson(lam=2.0)))))

                    # Generate NORMAL unit_price
                    unit_price = Decimal(str(max(0.01, abs(np.random.normal(loc=20.0, scale=10.0))))).quantize(Decimal("0.0001"))
                                        
                    # Inject negative quantity anomaly (RARE)
                    quantity = self.maybe_inject_anomaly(
                        "order_lines", "order_lines_negative_qty", 
                        quantity, -random.randint(1, 5),
                        {"order_id": order_id, "line": line_num, "qty": -1}
                    )

                    # Inject zero price anomaly (RARE)
                    unit_price = self.maybe_inject_anomaly(
                        "order_lines", "order_lines_zero_price",
                        unit_price, Decimal("0.0000"),
                        {"order_id": order_id, "line": line_num, "price": "0.0000"}
                    )

                    # Discount and tax
                    discount_pct = Decimal(str(round(max(0.0, min(1.0, np.random.beta(1, 10))), 4)))
                    tax_pct = Decimal(str(round(max(0.0, min(1.0, np.random.beta(2, 50))), 4)))
                    
                    # Add order line row
                    line_row = [
                        order_id, line_num, product_id, quantity,
                        fmt_decimal(unit_price, 4),
                        fmt_decimal(discount_pct, 4),
                        fmt_decimal(tax_pct, 4)
                    ]
                    line_buffers[order_date].append(line_row)
                    lines_written += 1
                
                # Duplicate orders anomaly
                if order_id in duplicate_ids:
                    order_buffers[order_date].append(list(order_row))
                    self.log_anomaly("orders", "duplicate_order_id", {"order_id": order_id})
                
                # Shipments (90% of orders get shipped)
                if random.random() < 0.9:
                    shipped_at = timestamp_tz + timedelta(hours=random.randint(1, 72))
                    delivered_at = None if random.random() < 0.1 else shipped_at + timedelta(
                        days=random.randint(0, 7), hours=random.randint(0, 23))
                    
                    shipped_naive = shipped_at.astimezone(timezone.utc).replace(tzinfo=None)
                    delivered_naive = delivered_at.astimezone(timezone.utc).replace(tzinfo=None) if delivered_at else None
                    
                    ship_cost = Decimal(str(max(0.0, np.random.normal(loc=8.0, scale=3.0)))).quantize(Decimal("0.01"))
                    
                    # If delivered more than 5 days late → anomaly
                    if delivered_at and (delivered_at - shipped_at).days > 5:
                        self.log_anomaly("shipments", "late_delivery", {
                            "order_id": order_id,
                            "shipped_at": iso(shipped_at),
                            "delivered_at": iso(delivered_at)
                        })
                    
                    # Add shipment row
                    shipment_buffer["shipment_id"].append(next_shipment_id)
                    shipment_buffer["order_id"].append(order_id)
                    shipment_buffer["carrier"].append(random.choice(CARRIERS))
                    shipment_buffer["shipped_at"].append(shipped_naive)
                    shipment_buffer["delivered_at"].append(delivered_naive)
                    shipment_buffer["ship_cost"].append(ship_cost)
                    next_shipment_id += 1
                
                # Flush buffers when they get too large
                if len(order_buffers[order_date]) >= CSV_BATCH_SIZE:
                    self._flush_csv_partition(order_date, orders_dir, order_buffers,
                                            [f.name for f in orders_header_schema],
                                            orders_header_schema, "orders")
                if len(line_buffers[order_date]) >= CSV_BATCH_SIZE:
                    self._flush_csv_partition(order_date, lines_dir, line_buffers,
                                            [f.name for f in orders_lines_schema],
                                            orders_lines_schema, "order_lines")
                if len(shipment_buffer["shipment_id"]) >= PARQUET_BATCH_SIZE:
                    _append_shipments_parquet(self.out, shipment_buffer)
            
            # Flush any remaining data
            print(f"[orders] Flushing remaining buffers...")
            for date_key in list(order_buffers.keys()):
                self._flush_csv_partition(date_key, orders_dir, order_buffers,
                                        [f.name for f in orders_header_schema],
                                        orders_header_schema, "orders")
            for date_key in list(line_buffers.keys()):
                self._flush_csv_partition(date_key, lines_dir, line_buffers,
                                        [f.name for f in orders_lines_schema],
                                        orders_lines_schema, "order_lines")
            if shipment_buffer["shipment_id"]:
                _append_shipments_parquet(self.out, shipment_buffer)
            
            elapsed = time.time() - start_time
            print(f"[orders] Generated {orders_written:,} orders in {format_duration(elapsed)}")
            print(f"[orders] Output: {orders_dir}")
            print(f"[order_lines] Generated {lines_written:,} lines")
            print(f"[order_lines] Output: {lines_dir}")
            print(f"[shipments] Generated {next_shipment_id - 1:,} shipments")
            print(f"[shipments] Output: {self.out} (Parquet files)")
            print(f"{'='*70}\n")

    def _flush_csv_partition(self, partition_key: str, base_dir: pathlib.Path,
                         buffers: Dict[str, List[List]], header: List[str],
                         schema: pa.Schema, table_label: str):
        """
        Write one partition of data to CSV and validate it.
        
        This is called when a buffer reaches CSV_BATCH_SIZE rows.
        """
        # Remove rows from buffer for this partition key (date)
        rows = buffers.pop(partition_key, [])
        if not rows:
            return
        
        # Create partition directory
        partition_dir = base_dir / f"order_date={partition_key}"
        ensure_dir(partition_dir)
        
        # Create a random filename
        partition_file = partition_dir / f"part-{random.randint(1, 1_000_000)}.csv"
        
        # Write rows to CSV file
        with partition_file.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        
        # Validate that rows match schema
        try:
            columns = {name: pa.array([str(row[idx]) for row in rows], type=pa.string())
                    for idx, name in enumerate(header)}
            _ = pa.table(columns).cast(schema)
        except Exception as e:
            raise RuntimeError(f"{table_label} validation failed: {e}")

    def generate_events(self):
        """
        Generate clickstream events in JSONL format - OPTIMIZED VERSION.
        
        PERFORMANCE OPTIMIZATION:
        Instead of opening/closing files for each event, we:
        1. Group events by date in memory
        2. Write entire date partitions at once (ONE file per date)
        
        PARTITIONING: events/event_dt=2024-01-15/events.jsonl
        """
        
        if self.tables and "events" not in self.tables:
            return
        
        start_time = time.time()
        print(f"\n{'='*70}")
        print(f"[events] Starting generation...")
        print(f"{'='*70}")
        
        num_events = self.sizes["events"]
        output_base = self.out / "events"
        ensure_dir(output_base)
        
        print(f"[events] Target rows: {num_events:,}")
        
        malformed_rate = ANOMALY_RATES["events_malformed_json"]
        start_datetime = datetime(2023, 1, 1, tzinfo=TZ)
        days_span = max(1, self.max_days)
        
        # Buffer to accumulate events by date
        # Key: date string, Value: list of JSON strings
        event_buffers: Dict[str, List[str]] = defaultdict(list)
        
        print(f"[events] Generating events in memory...")
        progress_interval = max(1, num_events // 10)
        
        # Generate all events and group by date
        for i in range(1, num_events + 1):
            
            if i % progress_interval == 0:
                pct = (i / num_events) * 100
                print(f"[events] Progress: {i:,}/{num_events:,} ({pct:.0f}%)")
            
            # Assign event to a day
            day_offset = i % days_span
            event_date = (start_datetime + timedelta(days=day_offset)).date().isoformat()
            
            # Build the event JSON
            envelope = {
                "event_id": f"evt-{i}",
                "event_ts": iso(start_datetime + timedelta(days=day_offset, seconds=(i * 23) % 86400)),
                "event_type": random.choice(["page_view", "add_to_cart", "purchase", "login", "logout"]),
                "user_id": random.randint(1, self.sizes["customers"]) if random.random() > 0.01 else None,
                "session_id": f"ses-{random.randint(1, 10_000_000)}",
            }
            payload = {
                "details": {
                    "path": f"/{self.fake.slug()}",
                    "meta": {"x": random.randint(0, 100)}
                }
            }
            full_event = {"envelope": envelope, "payload": payload}
            
            # Sometimes write malformed JSON
            if random.random() < malformed_rate:
                if random.random() < 0.5:
                    json_str = json.dumps(full_event)[:-3]  # Truncate
                else:
                    json_str = json.dumps({"payload": payload})  # Missing envelope
                
                if random.random() < 0.01:  # Log only 1% to avoid spam
                    self.log_anomaly("events", "malformed_json", {"index": i})
            else:
                json_str = json.dumps(full_event)
            
            # Add to buffer for this date
            event_buffers[event_date].append(json_str)
        
        # Now write all buffers to files
        print(f"[events] Writing {len(event_buffers)} date partitions to disk...")
        
        for event_date, json_lines in event_buffers.items():
            # Create partition directory
            partition_dir = output_base / f"event_dt={event_date}"
            ensure_dir(partition_dir)
            
            # CHANGED: Use fixed filename instead of random
            # This ensures ONE file per date partition
            file_path = partition_dir / "events.jsonl"
            
            # Write to file (all events for this date in one file)
            with file_path.open("w", encoding="utf-8") as f:
                for line in json_lines:
                    f.write(line + "\n")
        
        elapsed = time.time() - start_time
        print(f"[events] Generated {num_events:,} events in {format_duration(elapsed)}")
        print(f"[events] Output: {output_base} ({len(event_buffers)} date partitions)")
        print(f"{'='*70}\n")

    def generate_sensors(self):
        """
        Generate IoT sensor readings - OPTIMIZED VERSION.
        
        PERFORMANCE OPTIMIZATION:
        Instead of opening files 700k+ times, we:
        1. Generate all data in batches using NumPy (vectorized)
        2. Group by partition in memory
        3. Write entire partitions at once
        
        PARTITIONING: sensors/store_id=1/month=2024-01/sensors.csv
        """
        
        if self.tables and "sensors" not in self.tables:
            return
        
        start_time = time.time()
        print(f"\n{'='*70}")
        print(f"[sensors] Starting generation...")
        print(f"{'='*70}")
        
        num_readings = self.sizes["sensors"]
        output_base = self.out / "sensors"
        ensure_dir(output_base)
        
        num_stores = self.sizes["stores"]
        start_datetime = datetime(2023, 1, 1)#, tzinfo=TZ)
        months_span = max(1, self.max_days // 30)
        
        print(f"[sensors] Target rows: {num_readings:,}")
        print(f"[sensors] Stores: {num_stores}, Months: {months_span}")
        
        # Buffer to accumulate sensor readings by partition
        # Key: (store_id, month_str), Value: list of rows
        partition_buffers: Dict[tuple, List[List]] = defaultdict(list)
        
        # Generate data in batches for performance
        batch_size = 50000
        total_batches = (num_readings + batch_size - 1) // batch_size
        
        print(f"[sensors] Processing in {total_batches} batches of {batch_size:,} rows...")
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, num_readings)
            batch_count = end_idx - start_idx
            
            print(f"[sensors] Batch {batch_num + 1}/{total_batches} ({batch_count:,} rows)...")
            
            # Generate indices for this batch
            indices = np.arange(start_idx + 1, end_idx + 1)
            
            # Vectorized calculations
            store_ids = (indices % num_stores) + 1
            month_offsets = (indices // num_stores) % months_span
            
            # Generate sensor readings (vectorized with NumPy)
            temperatures = np.random.normal(loc=22.0, scale=4.0, size=batch_count)
            humidities = np.random.normal(loc=45.0, scale=12.0, size=batch_count)
            
            # Apply anomalies
            missing_ts_mask = np.random.random(batch_count) < ANOMALY_RATES["sensors_missing_ts"]
            out_of_range_mask = np.random.random(batch_count) < ANOMALY_RATES["sensors_out_of_range"]
            
            # Out-of-range values
            anomaly_temps = np.random.choice([-100.0, 999.0, 200.0], size=batch_count)
            anomaly_humidity = np.random.choice([-10.0, 150.0, 999.0], size=batch_count)
            
            temperatures = np.where(out_of_range_mask, anomaly_temps, temperatures)
            humidities = np.where(out_of_range_mask, anomaly_humidity, humidities)
            
            # Round to 2 decimals
            temperatures = np.round(temperatures, 2)
            humidities = np.round(humidities, 2)
            
            # Generate battery levels and sensor IDs
            battery_levels = np.random.randint(3000, 4201, size=batch_count)
            sensor_ids = (indices % 50) + 1  # Sensor IDs 1-50
            
            # Generate seconds for timestamps
            seconds = (indices * 53) % 86400
            
            # Group rows by partition and buffer them
            for i in range(batch_count):
                idx = start_idx + i + 1
                store_id = int(store_ids[i])
                month_offset = int(month_offsets[i])
                
                # Calculate month string
                month_datetime = start_datetime + timedelta(days=30 * month_offset)
                month_str = month_datetime.strftime("%Y-%m")
                
                # Generate timestamp
                if missing_ts_mask[i]:
                    sensor_timestamp = None
                    if random.random() < 0.001:  # Log only 0.1% to avoid spam
                        self.log_anomaly("sensors", "missing_timestamp", {"index": idx})
                else:
                    ts = start_datetime + timedelta(days=month_offset, seconds=int(seconds[i]))
                    #ts_naive = ts.replace(tzinfo=None) 
                    sensor_timestamp = ts.isoformat()
                
                # Log out-of-range anomalies (sample only)
                if out_of_range_mask[i] and random.random() < 0.001:
                    self.log_anomaly("sensors", "out_of_range", 
                                    {"index": idx, "temp": temperatures[i], "humidity": humidities[i]})
                
                # Create row
                row = [
                    sensor_timestamp,
                    store_id,
                    f"sensor-{int(sensor_ids[i])}",
                    f"{temperatures[i]:.2f}",
                    f"{humidities[i]:.2f}",
                    int(battery_levels[i])
                ]
                
                # Add to partition buffer
                partition_key = (store_id, month_str)
                partition_buffers[partition_key].append(row)
        
        # Write all partitions to disk
        print(f"[sensors] Writing {len(partition_buffers)} partitions to disk...")
        
        rows_written = 0
        for (store_id, month_str), rows in partition_buffers.items():
            # Create partition directory
            partition_dir = output_base / f"store_id={store_id}" / f"month={month_str}"
            ensure_dir(partition_dir)
            output_file = partition_dir / "sensors.csv"
            
            # Write entire partition at once
            with output_file.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([field.name for field in sensors_schema])
                writer.writerows(rows)
                rows_written += len(rows)
        
        elapsed = time.time() - start_time
        print(f"[sensors] Generated {rows_written:,} readings in {format_duration(elapsed)}")
        print(f"[sensors] Output: {output_base}")
        print(f"{'='*70}\n")

    def generate_returns(self):
        """
        Generate product returns table with schema evolution.
        
        WHAT THIS DOES:
        - Creates version 1 of returns (basic return info)
        - Creates version 2 with added column (return_reason_code)
        - Saves as Delta Lake format (or Parquet if Delta not available)
        """
        
        if self.tables and "returns" not in self.tables:
            return
        
        start_time = time.time()
        print(f"\n{'='*70}")
        print(f"[returns] Starting generation...")
        print(f"{'='*70}")
        
        num_returns = self.sizes["returns"]
        output_base = self.out / "returns"
        ensure_dir(output_base)
        
        print(f"[returns] Target rows: {num_returns:,}")
        print(f"[returns] Demonstrating schema evolution (v1 → v2)")
        
        # Generate data for version 1
        return_ids = list(range(1, num_returns + 1))
        order_ids = [random.randint(1, self.sizes["orders"]) for _ in return_ids]
        product_ids = [random.randint(1, self.sizes["products"]) for _ in return_ids]
            
        # Generate return timestamps 1-60 days after corresponding orders
        def get_return_timestamp(order_id: int) -> datetime:
            base_date = datetime(2023, 1, 1, tzinfo=TZ) + timedelta(
                days=order_id % 365, hours=random.randint(0, 23))
            return base_date + timedelta(days=random.randint(1, 60), hours=random.randint(0, 23))

        timestamps = [get_return_timestamp(oid) for oid in order_ids]
            
        quantities = [max(1, int(np.random.poisson(lam=1.0))) for _ in return_ids]
        reasons = [random.choice(["defective", "changed_mind", "wrong_item", "other"]) 
                   for _ in return_ids]
        
        # Build version 1 table
        table_v1 = pa.table({
            "return_id": pa.array(return_ids, type=pa.int64()),
            "order_id": pa.array(order_ids, type=pa.int64()),
            "product_id": pa.array(product_ids, type=pa.int64()),
            "return_ts": pa.array(timestamps, type=pa.timestamp("us")),
            "qty": pa.array(quantities, type=pa.int32()),
            "reason": pa.array(reasons, type=pa.string()),
        })
        
        # Try saving as Delta Lake
        if DELTA_AVAILABLE:
            try:
                write_deltalake(str(output_base), table_v1, mode="overwrite")
                print(f"[returns] Wrote v1 as Delta Lake table")
                
                # Generate extra records for version 2 (with new column)
                extra_count = max(1, int(num_returns * 0.05))
                extra_ids = list(range(num_returns + 1, num_returns + 1 + extra_count))
                extra_order_ids = [random.randint(1, self.sizes["orders"]) for _ in extra_ids]
                extra_product_ids = [random.randint(1, self.sizes["products"]) for _ in extra_ids]
                extra_timestamps = [datetime(2023, 1, 1, tzinfo=TZ) + timedelta(
                    days=random.randint(0, 365)) for _ in extra_ids]
                
                # Version 2 table (adds return_reason_code)
                table_v2 = pa.table({
                    "return_id": pa.array(extra_ids, type=pa.int64()),
                    "order_id": pa.array(extra_order_ids, type=pa.int64()),
                    "product_id": pa.array(extra_product_ids, type=pa.int64()),
                    "return_ts": pa.array(extra_timestamps, type=pa.timestamp("us")),
                    "qty": pa.array([1 for _ in extra_ids], type=pa.int32()),
                    "reason": pa.array([random.choice(["defective", "other"]) for _ in extra_ids], type=pa.string()),
                    "return_reason_code": pa.array([random.choice(["RC1", "RC2", "RC3"]) for _ in extra_ids], type=pa.string()),
                })
                
                write_deltalake(str(output_base), table_v2, mode="append")
                print(f"[returns] Appended v2 (schema evolved: added return_reason_code)")
                
                
                # Demonstrate UPSERT by updating some existing returns
                if num_returns >= 10:
                    update_count = min(10, num_returns // 10)
                    update_ids = random.sample(return_ids[:num_returns], k=update_count)
                    
                    # Create records with same return_id but updated values (UPSERT)
                    upsert_table = pa.table({
                        "return_id": pa.array(update_ids, type=pa.int64()),
                        "order_id": pa.array([order_ids[i-1] for i in update_ids], type=pa.int64()),
                        "product_id": pa.array([product_ids[i-1] for i in update_ids], type=pa.int64()),
                        "return_ts": pa.array([timestamps[i-1] + timedelta(hours=1) for i in update_ids], type=pa.timestamp("us")),
                        "qty": pa.array([quantities[i-1] for i in update_ids], type=pa.int32()),
                        "reason": pa.array(["updated_reason" for _ in update_ids], type=pa.string()),
                        "return_reason_code": pa.array(["RC_UPDATED" for _ in update_ids], type=pa.string()),
                    })
                    
                    write_deltalake(str(output_base), upsert_table, mode="append")
                    print(f"[returns] Demonstrated UPSERT on {update_count} records")
                    print(f"[returns] Note: DELETE operations require DeltaTable API not available in basic write_deltalake")
                    
                    extra_count += update_count
                
                elapsed = time.time() - start_time
                print(f"[returns] Generated {num_returns + extra_count:,} returns in {format_duration(elapsed)}")
                print(f"[returns] Output: {output_base}")
                print(f"{'='*70}\n")
                return
                
            except Exception as e:
                print(f"[returns] Delta Lake failed, falling back to Parquet: {e}")
        
        # Fallback to Parquet
        v1_dir = output_base / "v1_parquet"
        v2_dir = output_base / "v2_parquet"
        ensure_dir(v1_dir)
        ensure_dir(v2_dir)
        
        pq.write_table(table_v1, str(v1_dir / "returns_v1.parquet"))
        print(f"[returns] Wrote v1 as Parquet")
        
        # Generate v2 data
        extra_count = max(1, int(num_returns * 0.05))
        v2_data = {
            "return_id": list(range(num_returns + 1, num_returns + 1 + extra_count)),
            "order_id": [random.randint(1, self.sizes["orders"]) for _ in range(extra_count)],
            "product_id": [random.randint(1, self.sizes["products"]) for _ in range(extra_count)],
            "return_ts": [datetime(2023, 1, 1, tzinfo=TZ) + timedelta(
                days=random.randint(0, 365)) for _ in range(extra_count)],
            "qty": [1 for _ in range(extra_count)],
            "reason": [random.choice(["defective", "other"]) for _ in range(extra_count)],
            "return_reason_code": [random.choice(["RC1", "RC2"]) for _ in range(extra_count)],
        }
        
        pq.write_table(pa.table(v2_data), str(v2_dir / "returns_v2.parquet"))
        print(f"[returns] Wrote v2 as Parquet (with return_reason_code)")
        
        elapsed = time.time() - start_time
        print(f"[returns] Generated {num_returns + extra_count:,} returns in {format_duration(elapsed)}")
        print(f"[returns] Output: {output_base}")
        print(f"{'='*70}\n")

    # ========================================================================
    # ORCHESTRATION
    # ========================================================================

    def run_all(self):
        """
        Run all data generators in order.
        
        Order matters: dimension tables (customers, products) before fact tables (orders).
        """
        overall_start = time.time()
        
        print("\n" + "="*70)
        print("STARTING DATA GENERATION")
        print("="*70)
        print(f"Output directory: {self.out}")
        print(f"Scale factor: {self.scale} ({self.scale * 100:.1f}%)")
        print(f"Random seed: {self.seed}")
        print(f"Max days span: {self.max_days}")
        print("\nRow counts:")
        for table, count in self.sizes.items():
            print(f"  {table:20s}: {count:,}")
        print("="*70)
        
        # Generate dimension tables first
        self.generate_customers()
        self.generate_products()
        self.generate_stores()
        self.generate_suppliers()
        self.generate_exchange_rates()
        
        # Then fact tables
        self.generate_orders_and_shipments()
        self.generate_events()
        self.generate_sensors()
        self.generate_returns()
        
        # Write anomaly log
        print(f"\n{'='*70}")
        print("WRITING ANOMALY LOG")
        print(f"{'='*70}")
        
        anomaly_file = self.out / "anomalies_log.json"
        with anomaly_file.open("w", encoding="utf-8") as f:
            json.dump(self.anomalies, f, indent=2, default=str)
        
        print(f"[anomalies] Anomaly log written to: {anomaly_file}")
        
        # Calculate total time
        overall_elapsed = time.time() - overall_start
        
        print(f"\n{'='*70}")
        print("GENERATION COMPLETE")
        print(f"{'='*70}")
        print(f"Total time: {format_duration(overall_elapsed)}")
        print(f"Data written to: {self.out}")
        print(f"Anomaly log: {anomaly_file}")
        
        # Summarize anomalies
        total_anomalies = sum(len(issues) for issues in self.anomalies.values())
        print(f"\nTotal anomalies injected: {total_anomalies:,}")
        
        if total_anomalies > 0:
            print("\nAnomalies by table:")
            for table, issues in sorted(self.anomalies.items()):
                if issues:
                    # Count by type
                    type_counts = defaultdict(int)
                    for issue in issues:
                        type_counts[issue["kind"]] += 1
                    
                    print(f"  {table}:")
                    for anomaly_type, count in sorted(type_counts.items()):
                        print(f"    - {anomaly_type}: {count:,}")
        
        print("="*70 + "\n")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """
    Main function - parse arguments and run generation.
    
    This is the entry point when you run:
        python -m scripts.generate_data --scale 0.1
    """
    args = parse_args()
    
    # Parse table filter if provided
    tables_list = None
    if args.tables:
        tables_list = [t.strip().lower() for t in args.tables.split(",")]
        print(f"Generating only tables: {', '.join(tables_list)}")
    
    # Create generator and run
    generator = DataGenerator(
        out_dir=args.out,
        seed=args.seed,
        scale=args.scale,
        max_days=args.max_days,
        tables=tables_list
    )
    
    generator.run_all()


if __name__ == "__main__":
    main()