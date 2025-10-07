# ============================================================================
# IMPORTS - External libraries we need
# ============================================================================

from datetime import timezone

# ============================================================================
# CONFIGURATION - Control how much data to generate and error rates
# ============================================================================

# How many rows to generate for each table (at 100% scale)
TARGET_COUNTS = {
    "customers": 80_000,          # Customer master data
    "products": 25_000,           # Product catalog
    "stores": 5_000,              # Store locations
    "suppliers": 8_000,           # Supplier information
    "orders": 1_500_000,          # Order headers
    "order_lines": 3_500_000,     # Order line items
    "events": 2_000_000,          # Website clickstream events
    "sensors": 7_000_000,         # IoT sensor readings
    "exchange_rates": 1100,       # Currency exchange rates
    "shipments": 1_000_000,       # Shipping information
    "returns": 100_000,           # Product returns
}

# Special flag rates
FLAG_RATES = {
    "vip_cust": 0.15,                   # vup customers
    "gdpr_consent": 0.95,               # gdpr consent
    "discontinued_product": 0.05,       # product discontinued
    "null_discontinued_date" : 0.2,     # discontinued products with blank disc. date
    "preferred" : 0.12,                 # preferred supplier
    "stores_closed": 0.05,              # closed stores
}

# Probability of injecting each type of anomaly (0.01 = 1% of rows)
ANOMALY_RATES = {
    "customers_malformed_email": 0.007,        # Invalid email formats
    "customers_duplicate_natural_key": 0.002,  # Duplicate customer codes
    "customers_phone_null_rate": 0.02,         # Missing phone numbers
    "customers_addr_null_rate": 0.01,          # Missing addresses
    "products_invalid_price": 0.003,           # Negative prices
    "stores_impossible_coords": 0.002,         # Invalid GPS coordinates
    "stores_duplicate": 0.001,                 # Duplicate stores
    "orders_fk_violations": 0.01,              # References to non-existent customers/stores
    "orders_duplicate_ids": 0.0005,            # Duplicate order IDs
    "order_lines_invalid_product": 0.01,       # References to non-existent products
    "events_malformed_json": 0.0005,           # Broken JSON in event files
    "sensors_out_of_range": 0.003,             # Impossible temperature/humidity values
    "sensors_missing_ts": 0.001,               # Missing timestamps
}

# Performance tuning
CSV_BATCH_SIZE = 10_000      # Validate CSV data every N rows
PARQUET_BATCH_SIZE = 50_000  # Write Parquet files in chunks of N rows
TZ = timezone.utc            # Use UTC for all timestamps

# Reference data for realistic fake data
CATEGORIES = {
    "Electronics": ["Phones", "Computers", "Audio"],
    "Home": ["Furniture", "Decor", "Kitchen"],
    "Clothing": ["Men", "Women", "Kids"],
    "Food": ["Groceries", "Beverages"],
    "Sport": ["Fitness", "Outdoor"],
    "Toys": ["Indoor", "Educational"],
}
REGIONS = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "ACT"]  # Australian states
CURRENCIES = ["USD", "EUR", "GBP", "JPY"]  # Exchange rates to generate
CARRIERS = ["AUSPOST", "TOLL", "DHL", "LOCAL"]  # Shipping companies