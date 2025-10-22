# ============================================================================
# DATA GENERATION & BRONZE INGESTION CONFIGURATION
# ============================================================================
"""
Configuration for synthetic data generation and bronze layer ingestion.

This file contains all constants, rates, and mappings used across:
- Data generation (generate_data.py)
- Bronze ingestion (load_to_bronze.py)

Centralizing configuration here makes it easy to:
- Adjust data volumes
- Tune anomaly rates
- Modify partitioning strategies
"""

# ============================================================================
# IMPORTS
# ============================================================================

import os
from datetime import timezone
from typing import Dict, List, Optional

# Import schemas - these define table structures
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
    SCHEMAS_AVAILABLE = True
except ImportError:
    # Schemas not available yet - okay during initial setup
    SCHEMAS_AVAILABLE = False
    print("Warning: schemas.schemas not available - some features disabled")


# ============================================================================
# ENVIRONMENT CONFIGURATION
# ============================================================================

# Support different environments (dev/staging/prod)
ENVIRONMENT = os.getenv("DATA_ENV", "dev")  # dev, staging, or prod

# Scale multiplier based on environment
ENV_SCALE_FACTORS = {
    "dev": 0.01,      # 1% of data for fast local testing
    "staging": 0.10,  # 10% for staging environment
    "prod": 1.0,      # 100% for production
}

# Get scale factor for current environment
DEFAULT_SCALE = ENV_SCALE_FACTORS.get(ENVIRONMENT, 1.0)


# ============================================================================
# DATA GENERATION - TARGET ROW COUNTS
# ============================================================================

# Target row counts at 100% scale (scale=1.0)
# Multiply by scale factor to get actual counts
TARGET_COUNTS = {
    # Dimension tables (master data)
    "customers": 80_000,
    "products": 25_000,
    "stores": 5_000,
    "suppliers": 8_000,
    
    # Fact tables (transactional data)
    "orders": 1_500_000,
    "order_lines": 3_500_000,
    "shipments": 1_000_000,
    "returns": 100_000,
    
    # Event/IoT data
    "events": 2_000_000,
    "sensors": 7_000_000,
    
    # Reference data
    "exchange_rates": 1_100,  # ~3 years of daily rates for 4 currencies
}


# ============================================================================
# DATA GENERATION - BUSINESS LOGIC FLAGS
# ============================================================================

# Probability that boolean flags are True
FLAG_RATES = {
    "vip_cust": 0.15,                # 15% of customers are VIP
    "gdpr_consent": 0.95,            # 95% gave GDPR consent
    "discontinued_product": 0.05,    # 5% of products discontinued
    "null_discontinued_date": 0.20,  # 20% of discontinued lack date (data quality issue)
    "preferred": 0.12,               # 12% of suppliers are preferred
    "stores_closed": 0.05,           # 5% of stores are closed
}


# ============================================================================
# DATA GENERATION - ANOMALY INJECTION RATES
# ============================================================================

# Probability of injecting data quality issues (for testing DQ checks)
# Format: 0.01 = 1% of rows will have this issue
ANOMALY_RATES = {
    # Customer anomalies
    "customers_malformed_email": 0.007,         # 0.7% invalid email formats
    "customers_duplicate_natural_key": 0.002,   # 0.2% duplicate customer codes
    "customers_phone_null_rate": 0.02,          # 2% missing phone numbers
    "customers_addr_null_rate": 0.01,           # 1% missing addresses
    
    # Product anomalies
    "products_invalid_price": 0.003,            # 0.3% negative/invalid prices
    
    # Store anomalies
    "stores_impossible_coords": 0.002,          # 0.2% invalid GPS coordinates
    "stores_duplicate": 0.001,                  # 0.1% duplicate store codes
    
    # Order anomalies
    "orders_fk_violations": 0.01,               # 1% broken foreign keys
    "orders_duplicate_ids": 0.0005,             # 0.05% duplicate order IDs
    
    # Order line anomalies
    "order_lines_invalid_product": 0.01,        # 1% invalid product references
    "order_lines_negative_qty": 0.001,          # 0.1% negative quantities
    "order_lines_zero_price": 0.001,            # 0.1% zero prices
    
    # Event anomalies
    "events_malformed_json": 0.0005,            # 0.05% broken JSON
    
    # Sensor anomalies
    "sensors_out_of_range": 0.003,              # 0.3% impossible readings
    "sensors_missing_ts": 0.001,                # 0.1% missing timestamps
}


# ============================================================================
# DATA GENERATION - PERFORMANCE TUNING
# ============================================================================

# Batch sizes for processing
CSV_BATCH_SIZE = 10_000       # Validate CSV every N rows
PARQUET_BATCH_SIZE = 50_000   # Write Parquet in chunks of N rows

# Timezone for all timestamps
TZ = timezone.utc


# ============================================================================
# DATA GENERATION - REFERENCE DATA
# ============================================================================

# Product categories and subcategories
CATEGORIES = {
    "Electronics": ["Phones", "Computers", "Audio", "Cameras"],
    "Home": ["Furniture", "Decor", "Kitchen", "Bedding"],
    "Clothing": ["Men", "Women", "Kids", "Accessories"],
    "Food": ["Groceries", "Beverages", "Snacks"],
    "Sports": ["Fitness", "Outdoor", "Team Sports"],
    "Toys": ["Indoor", "Educational", "Games"],
}

# Australian states/territories
REGIONS = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "ACT"]

# Currencies for exchange rates
CURRENCIES = ["USD", "EUR", "GBP", "JPY"]

# Shipping carriers
CARRIERS = ["AUSPOST", "TOLL", "DHL", "FEDEX", "LOCAL"]


# ============================================================================
# BRONZE INGESTION - SCHEMA MAPPING
# ============================================================================

# Map table names to their PyArrow schemas
# Only populated if schemas are available
SCHEMA_MAP = {}

if SCHEMAS_AVAILABLE:
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


# ============================================================================
# BRONZE INGESTION - PRIMARY KEYS (FOR UPSERT)
# ============================================================================

# Define primary keys for each table
# Used for UPSERT operations in Delta Lake
PRIMARY_KEYS: Dict[str, List[str]] = {
    # Simple primary keys (single column)
    "customers": ["customer_id"],
    "products": ["product_id"],
    "stores": ["store_id"],
    "suppliers": ["supplier_id"],
    "orders": ["order_id"],
    "shipments": ["shipment_id"],
    "returns": ["return_id"],
    
    # Composite primary keys (multiple columns)
    "order_lines": ["order_id", "line_number"],
}


# ============================================================================
# BRONZE INGESTION - SOURCE FILE PATTERNS
# ============================================================================

# Glob patterns to locate raw source files
SOURCE_PATTERNS: Dict[str, str] = {
    # Single CSV files
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "suppliers": "suppliers.csv",
    
    # Partitioned CSV files
    "orders": "orders/**/*.csv",
    "order_lines": "order_lines/**/*.csv",
    "sensors": "sensors/**/*.csv",
    
    # JSONL files (JSON Lines)
    "events": "events/**/*.jsonl",
    
    # Excel file
    "exchange_rates": "exchange_rates.xlsx",
    
    # Parquet files
    "shipments": "shipments_*.parquet",
    
    # Delta Lake directory (or Parquet fallback)
    "returns": "returns/**/*",
}


# ============================================================================
# BRONZE INGESTION - PARTITIONING STRATEGY
# ============================================================================

# Define how to partition each table
PARTITION_COLUMNS: Dict[str, Optional[List[str]]] = {
    "customers": None,
    "products": None,
    "stores": None,
    "suppliers": None,
    "exchange_rates": None,
    "shipments": None,
    "returns": None,
    "order_lines": None,
    
    # Partitioned by date (time-series queries)
    "orders": ["order_dt_local"],
    "events": ["event_date"],
    
    # Multi-level partitioning (for very large tables)
    "sensors": ["store_id", "month"],
}


# ============================================================================
# BRONZE INGESTION - FILE SIZE OPTIMIZATION
# ============================================================================

# Target file sizes for optimal query performance

MIN_FILE_SIZE_MB = 100      # Warn if files smaller than this
TARGET_FILE_SIZE_MB = 150   # Target size when splitting large files
MAX_FILE_SIZE_MB = 250      # Split files larger than this


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_config() -> bool:
    """
    Validate configuration for common issues.
    
    Returns:
        True if config is valid, False otherwise
    """
    issues = []
    
    # Check all TARGET_COUNTS are positive
    for table, count in TARGET_COUNTS.items():
        if count <= 0:
            issues.append(f"TARGET_COUNTS['{table}'] must be positive, got {count}")
    
    # Check anomaly rates are between 0 and 1
    for anomaly, rate in ANOMALY_RATES.items():
        if not 0 <= rate <= 1:
            issues.append(f"ANOMALY_RATES['{anomaly}'] must be 0-1, got {rate}")
    
    # Check flag rates are between 0 and 1
    for flag, rate in FLAG_RATES.items():
        if not 0 <= rate <= 1:
            issues.append(f"FLAG_RATES['{flag}'] must be 0-1, got {rate}")
    
    # Check PRIMARY_KEYS match SCHEMA_MAP keys
    if SCHEMAS_AVAILABLE:
        schema_tables = set(SCHEMA_MAP.keys())
        pk_tables = set(PRIMARY_KEYS.keys())
        
        if schema_tables != pk_tables:
            missing_in_pk = schema_tables - pk_tables
            missing_in_schema = pk_tables - schema_tables
            
            if missing_in_pk:
                issues.append(f"PRIMARY_KEYS missing tables: {missing_in_pk}")
            if missing_in_schema:
                issues.append(f"PRIMARY_KEYS has extra tables: {missing_in_schema}")
    
    # Check SOURCE_PATTERNS match SCHEMA_MAP keys
    if SCHEMAS_AVAILABLE:
        pattern_tables = set(SOURCE_PATTERNS.keys())
        if schema_tables != pattern_tables:
            issues.append(f"SOURCE_PATTERNS doesn't match SCHEMA_MAP")
    
    # Check file sizes make sense
    if not MIN_FILE_SIZE_MB < TARGET_FILE_SIZE_MB < MAX_FILE_SIZE_MB:
        issues.append(f"File size progression invalid: {MIN_FILE_SIZE_MB} < {TARGET_FILE_SIZE_MB} < {MAX_FILE_SIZE_MB}")
    
    # Report issues
    if issues:
        print(" Configuration validation failed:")
        for issue in issues:
            print(f"  - {issue}")
        return False
    
    return True


def get_table_list() -> List[str]:
    """Get list of all available tables."""
    if SCHEMAS_AVAILABLE:
        return sorted(SCHEMA_MAP.keys())
    return sorted(TARGET_COUNTS.keys())


def get_scaled_counts(scale: float = 1.0) -> Dict[str, int]:
    """
    Get row counts scaled by factor.
    
    Args:
        scale: Scale factor (0.01 = 1%, 1.0 = 100%)
    
    Returns:
        Dictionary of table -> scaled row count
    """
    return {
        table: max(1, int(count * scale))
        for table, count in TARGET_COUNTS.items()
    }


# ============================================================================
# INITIALIZATION
# ============================================================================

# Validate config on import
if __name__ == "__main__":
    # Only validate when run directly, not on import
    if validate_config():
        print("Configuration validated successfully")
        print(f"Environment: {ENVIRONMENT}")
        print(f"Default scale: {DEFAULT_SCALE}")
        print(f"Available tables: {len(get_table_list())}")
    else:
        exit(1)