"""
COMP5339 Assignment 2 – Task 2: Data Integration & Materialization
Team: Priyansh Khandelwal, Zhenzhe Wu
"""

import os
import json
import pandas as pd
from glob import glob
from typing import List, Dict, Optional
import warnings

# Suppress harmless warnings
warnings.filterwarnings("ignore", category=UserWarning)

# ================================
# CONFIG
# ================================
JSON_DIR = "output_data"
META_CSV = "outputs/facilities_nem.csv"
OUTPUT_CSV = "consolidated_facility_data.csv"
MARKET_JSON_PATTERNS = [
    "facility_market_nem_price_demand_*.json",
    "market_nem_price_demand_*.json",
]

_keep_env = os.getenv("INTEGRATION_MAX_INTERVALS")
KEEP_INTERVALS: Optional[int]
try:
    KEEP_INTERVALS = int(_keep_env) if _keep_env is not None else 288
    if KEEP_INTERVALS <= 0:
        KEEP_INTERVALS = None
except (TypeError, ValueError):
    KEEP_INTERVALS = 288

FINAL_COLUMNS_BASE = [
    "duid",
    "interval_start",
    "power_mw",
    "emissions_tco2e",
    "name",
    "fuel_type",
    "region",
    "latitude",
    "longitude",
    "capacity_mw",
]

# Emission factors used when estimating values from market demand
EMISSION_FACTORS = {
    "COAL": 0.90,
    "GAS": 0.45,
    "DISTILLATE": 0.70,
    "BIOGAS": 0.10,
    "HYDRO": 0.0,
    "PUMPED HYDRO": 0.0,
    "WIND": 0.0,
    "SOLAR": 0.0,
    "BATTERY": 0.0,
    "OTHER": 0.30,
}

# Ensure output directory
os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True) if os.path.dirname(OUTPUT_CSV) else None

# ================================
# DATA LOADING
# ================================
def load_facility_metadata(meta_csv_path: str) -> pd.DataFrame:
    """Load and validate facility metadata."""
    if not os.path.exists(meta_csv_path):
        raise FileNotFoundError(f"Missing {meta_csv_path} — Run Task 1 first!")
    
    meta_df = pd.read_csv(meta_csv_path, dtype={"duid": str})
    print(f" Loaded {len(meta_df)} facilities from metadata")
    return meta_df

def parse_json_file(file_path: str) -> List[Dict]:
    """Parse one JSON file → list of unit-level records."""
    rows = []
    filename = os.path.basename(file_path)

    # Extract parent_duid from filename: facility_{PARENT}_power_*.json
    try:
        if not filename.startswith("facility_"):
            raise ValueError("unexpected prefix")
        remainder = filename[len("facility_"):]
        parent_duid = remainder.split("_power_", 1)[0]
        if not parent_duid:
            raise ValueError("empty parent")
    except Exception:
        print(f" Warning: Invalid filename format: {filename}")
        return rows

    # Load JSON
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f" Warning: Failed to read {filename}: {e}")
        return rows

    data_blocks = data.get("data", [])
    if not data_blocks:
        return rows

    for block in data_blocks:
        metric = block.get("metric")
        if metric not in ["power", "emissions"]:
            continue

        results = block.get("results", [])
        for res in results:
            unit_code = res.get("columns", {}).get("unit_code")
            if not unit_code:
                continue

            data_points = res.get("data", [])
            for point in data_points:
                if not isinstance(point, list) or len(point) != 2:
                    continue
                timestamp, value = point
                if not timestamp or value is None:
                    continue
                try:
                    float_value = float(value)
                except (ValueError, TypeError):
                    continue

                rows.append({
                    "unit_duid": str(unit_code),
                    "parent_duid": str(parent_duid),
                    "interval_start": str(timestamp),
                    "metric": metric,
                    "value": float_value
                })

    return rows

def load_all_json_files(json_dir: str) -> pd.DataFrame:
    """Load all JSON files → raw long-format DataFrame."""
    json_pattern = os.path.join(json_dir, "facility_*_power_*.json")
    json_files = [
        path for path in glob(json_pattern)
        if "market" not in os.path.basename(path).lower()
    ]

    if not json_files:
        print(f" Warning: No facility power/emissions JSON files found in {json_dir}")
        return pd.DataFrame(columns=["unit_duid", "parent_duid", "interval_start", "metric", "value"])

    print(f" Found {len(json_files)} JSON files. Parsing...")
    all_rows = []

    for file_path in json_files:
        rows = parse_json_file(file_path)
        all_rows.extend(rows)

    if not all_rows:
        print(" Warning: No valid time-series data found in any facility JSON file!")
        return pd.DataFrame(columns=["unit_duid", "parent_duid", "interval_start", "metric", "value"])

    raw_df = pd.DataFrame(all_rows)
    print(f" Parsed {len(raw_df):,} raw records from {raw_df['unit_duid'].nunique()} units")
    return raw_df

def load_market_data(json_dir: str) -> pd.DataFrame:
    """Load optional market price/demand JSON (one row per region + interval)."""
    market_files = []
    for pattern in MARKET_JSON_PATTERNS:
        market_files.extend(glob(os.path.join(json_dir, pattern)))

    if not market_files:
        print(" Warning: No market price/demand JSON files found.")
        return pd.DataFrame(columns=["interval_start", "network_region", "price", "demand"])

    # Use the most recently modified file if multiple matches exist
    market_file = max(market_files, key=os.path.getmtime)
    print(f" Loading market data from {os.path.basename(market_file)}")

    try:
        with open(market_file, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        print(f" Warning: Failed to read market file {market_file}: {exc}")
        return pd.DataFrame(columns=["interval_start", "network_region", "price", "demand"])

    records = payload.get("data", [])
    if not isinstance(records, list):
        print(" Warning: Market file missing 'data' list. Skipping.")
        return pd.DataFrame(columns=["interval_start", "network_region", "price", "demand"])

    clean_rows = []
    for rec in records:
        interval = rec.get("interval_start")
        region = rec.get("network_region")
        if not interval or not region:
            continue

        price = rec.get("price")
        demand = rec.get("demand")
        try:
            price_val = float(price) if price is not None else None
        except (TypeError, ValueError):
            price_val = None
        try:
            demand_val = float(demand) if demand is not None else None
        except (TypeError, ValueError):
            demand_val = None

        clean_rows.append({
            "interval_start": interval,
            "network_region": str(region).upper(),
            "price": price_val,
            "demand": demand_val,
        })

    market_df = pd.DataFrame(clean_rows)
    if market_df.empty:
        print(" Warning: Market JSON contained no usable records.")
        return pd.DataFrame(columns=["interval_start", "network_region", "price", "demand"])

    # Ensure numeric and clip negatives that occasionally appear in market feeds
    neg_info = []
    for col in ("price", "demand"):
        market_df[col] = pd.to_numeric(market_df[col], errors="coerce")
        negatives = market_df[col].notna() & (market_df[col] < 0)
        if negatives.any():
            count = int(negatives.sum())
            neg_info.append(f"{col}={count}")
            market_df.loc[negatives, col] = 0.0
    if neg_info:
        joined = ", ".join(neg_info)
        print(f" Warning: Clipped negative market values ({joined}) to 0.0")

    market_df["interval_start"] = pd.to_datetime(
        market_df["interval_start"], errors="coerce", utc=True
    )
    market_df = market_df.dropna(subset=["interval_start"])
    print(f" Loaded {len(market_df):,} market records across {market_df['network_region'].nunique()} regions")
    return market_df

def build_market_only_dataset(meta_df: pd.DataFrame, market_df: pd.DataFrame) -> pd.DataFrame:
    """Fallback dataset when facility time-series are unavailable."""
    print("\nBuilding dataset from metadata + market price/demand (facility data missing)...")
    if market_df.empty:
        print(" Warning: Market data unavailable. Returning empty dataset.")
        columns = FINAL_COLUMNS_BASE + ["price", "demand"]
        return pd.DataFrame(columns=columns)

    meta_cols = ["duid", "name", "fuel_type", "region", "latitude", "longitude", "capacity_mw"]
    meta_subset = meta_df[meta_cols].copy()
    meta_subset["region"] = meta_subset["region"].astype(str).str.upper()
    meta_subset["fuel_type"] = meta_subset["fuel_type"].astype(str).str.upper()
    meta_subset["capacity_mw"] = pd.to_numeric(meta_subset["capacity_mw"], errors="coerce").fillna(0.0)
    meta_subset["region_capacity_total"] = meta_subset.groupby("region")["capacity_mw"].transform("sum")

    market_ready = market_df.rename(columns={"network_region": "region"}).copy()
    market_ready["region"] = market_ready["region"].astype(str).str.upper()

    joined = meta_subset.merge(market_ready, on="region", how="inner")
    if joined.empty:
        print(" Warning: No region overlap between facilities and market data.")
        columns = FINAL_COLUMNS_BASE + ["price", "demand"]
        return pd.DataFrame(columns=columns)

    joined = joined.rename(columns={"capacity_mw": "capacity_mw"})
    joined["demand"] = pd.to_numeric(joined["demand"], errors="coerce")
    demand_valid = joined["demand"].notna() & (joined["region_capacity_total"] > 0)
    joined["allocation_ratio"] = 0.0
    joined.loc[demand_valid, "allocation_ratio"] = (
        joined.loc[demand_valid, "capacity_mw"] / joined.loc[demand_valid, "region_capacity_total"]
    )
    joined["power_mw"] = joined["demand"] * joined["allocation_ratio"]
    joined["power_mw"] = joined["power_mw"].clip(lower=0.0)
    joined["demand"] = (joined["demand"] * joined["allocation_ratio"]).clip(lower=0.0)

    def emission_factor(fuel: str) -> float:
        return EMISSION_FACTORS.get(str(fuel).upper(), EMISSION_FACTORS["OTHER"])

    joined["emission_factor"] = joined["fuel_type"].apply(emission_factor)
    joined["emissions_tco2e"] = (joined["power_mw"] * joined["emission_factor"]).clip(lower=0.0)
    joined["interval_start"] = pd.to_datetime(joined["interval_start"], utc=True, errors="coerce")
    joined = joined.dropna(subset=["interval_start"])

    joined = joined.drop(columns=["region_capacity_total", "allocation_ratio", "emission_factor"], errors="ignore")

    joined = joined[
        ["duid", "interval_start", "power_mw", "emissions_tco2e",
         "name", "fuel_type", "region", "latitude", "longitude",
         "capacity_mw", "price", "demand"]
    ]
    joined = joined.sort_values(["duid", "interval_start"]).reset_index(drop=True)

    if KEEP_INTERVALS:
        unique_intervals = joined["interval_start"].dropna().sort_values().unique()
        if len(unique_intervals) > KEEP_INTERVALS:
            cutoff = unique_intervals[-KEEP_INTERVALS]
            joined = joined[joined["interval_start"] >= cutoff].reset_index(drop=True)

    return joined

# ================================
# TRANSFORMATION
# ================================
def pivot_to_unit_level(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Pivot: one row per unit + timestamp + power + emissions."""
    print("\nPivoting to unit-level (power & emissions side-by-side)...")
    
    pivoted = raw_df.pivot_table(
        index=["unit_duid", "parent_duid", "interval_start"],
        columns="metric",
        values="value",
        aggfunc="first"
    ).reset_index()

    pivoted.columns.name = None
    pivoted = pivoted.rename(columns={
        "power": "power_mw",
        "emissions": "emissions_tco2e"
    })

    # Filter invalid
    pivoted = pivoted.dropna(subset=["interval_start"])
    pivoted = pivoted[(pivoted["power_mw"].isna()) | (pivoted["power_mw"] >= 0)]
    pivoted = pivoted[(pivoted["emissions_tco2e"].isna()) | (pivoted["emissions_tco2e"] >= 0)]

    print(f" Unit-level: {len(pivoted):,} rows")
    return pivoted

def aggregate_to_parent_facility(unit_df: pd.DataFrame) -> pd.DataFrame:
    """Sum power & emissions per parent facility + timestamp."""
    print("\nAggregating units → parent facility...")
    
    agg_df = unit_df.groupby(["parent_duid", "interval_start"], as_index=False).agg({
        "power_mw": "sum",
        "emissions_tco2e": "sum"
    })

    print(f" Aggregated to {agg_df['parent_duid'].nunique()} parent facilities")
    return agg_df

def join_metadata(parent_df: pd.DataFrame, meta_df: pd.DataFrame, market_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Join metadata (name, fuel_type, region, lat, lon, capacity)."""
    print("Joining facility metadata...")
    
    # JOIN ON parent_duid == duid
    final_df = parent_df.merge(
        meta_df,
        left_on="parent_duid",
        right_on="duid",
        how="left"
    )

    # RENAME parent_duid → duid (FINAL KEY) and drop duplicate key columns from metadata
    final_df = final_df.rename(columns={"parent_duid": "duid"})
    final_df = final_df.loc[:, ~final_df.columns.duplicated()]

    # Select final columns
    final_df = final_df[[
        "duid", "interval_start", "power_mw", "emissions_tco2e",
        "name", "fuel_type", "region", "latitude", "longitude", "capacity_mw"
    ]]

    # Convert timestamp
    final_df["interval_start"] = pd.to_datetime(final_df["interval_start"], utc=True, errors="coerce")
    final_df = final_df.dropna(subset=["interval_start"])
    region_mask = final_df["region"].notna()
    final_df.loc[region_mask, "region"] = final_df.loc[region_mask, "region"].astype(str).str.upper()
    final_df.loc[~region_mask, "region"] = pd.NA
    final_df["power_mw"] = pd.to_numeric(final_df["power_mw"], errors="coerce")
    final_df["emissions_tco2e"] = pd.to_numeric(final_df["emissions_tco2e"], errors="coerce")

    # If emissions are missing but power is available, estimate using fuel-type emission factors
    missing_emissions = final_df["emissions_tco2e"].isna() & final_df["power_mw"].notna()
    if missing_emissions.any():
        def _factor(fuel: str) -> float:
            return EMISSION_FACTORS.get(str(fuel).upper(), EMISSION_FACTORS["OTHER"])

        factors = final_df.loc[missing_emissions, "fuel_type"].apply(_factor).fillna(EMISSION_FACTORS["OTHER"])
        est_values = (final_df.loc[missing_emissions, "power_mw"].fillna(0.0) * factors).clip(lower=0.0)
        final_df.loc[missing_emissions, "emissions_tco2e"] = est_values
        print(f"   Estimated emissions for {missing_emissions.sum():,} rows based on fuel-type factors")

    if market_df is not None and not market_df.empty:
        market_ready = market_df.rename(columns={"network_region": "region"}).copy()
        market_ready["region"] = market_ready["region"].astype(str).str.upper()
        market_ready["interval_start"] = pd.to_datetime(
            market_ready["interval_start"], utc=True, errors="coerce"
        )
        market_ready = market_ready.dropna(subset=["interval_start"])

        final_df = final_df.merge(
            market_ready,
            on=["region", "interval_start"],
            how="left"
        )

    # Sort
    final_df = final_df.sort_values(["duid", "interval_start"]).reset_index(drop=True)

    if KEEP_INTERVALS:
        unique_intervals = final_df["interval_start"].dropna().sort_values().unique()
        if len(unique_intervals) > KEEP_INTERVALS:
            cutoff = unique_intervals[-KEEP_INTERVALS]
            final_df = final_df[final_df["interval_start"] >= cutoff].reset_index(drop=True)

    final_columns = FINAL_COLUMNS_BASE.copy()
    if market_df is not None and not market_df.empty:
        final_columns += ["price", "demand"]
    final_columns = [col for col in final_columns if col in final_df.columns]
    final_df = final_df[final_columns]

    return final_df

# ================================
# MAIN PIPELINE
# ================================
def main():
    print("\n")
    print("=" * 70)
    print("TASK 2: DATA INTEGRATION & MATERIALIZATION")
    print("=" * 70)

    # Step 1
    print("\n[1/6] Loading facility metadata...")
    meta_df = load_facility_metadata(META_CSV)

    # Step 2
    print("\n[2/6] Loading market price/demand data...")
    market_df = load_market_data(JSON_DIR)

    # Step 3
    print("\n[3/6] Loading facility time-series JSON files...")
    raw_df = load_all_json_files(JSON_DIR)

    # Step 4
    if raw_df.empty:
        print("\n[4/6] Facility data unavailable → using market-only fallback.")
        final_df = build_market_only_dataset(meta_df, market_df)
    else:
        print("\n[4/6] Transforming facility data...")
        print("   Pivoting to unit-level...")
        unit_df = pivot_to_unit_level(raw_df)

        print("   Aggregating to parent facility...")
        parent_df = aggregate_to_parent_facility(unit_df)

        print("   Joining metadata & market price/demand...")
        final_df = join_metadata(parent_df, meta_df, market_df)

    # Step 5
    print(f"\n[5/6] Saving final CSV → {OUTPUT_CSV}")
    final_df.to_csv(OUTPUT_CSV, index=False)

    # Summary
    print("\n[6/6] Final dataset summary:")
    print("\n" + "=" * 70)
    print("SUCCESS: CONSOLIDATED DATASET READY")
    print("=" * 70)
    print("\n")
    print(f" File: {OUTPUT_CSV}")
    print(f" Rows: {len(final_df):,}")
    print(f" Facilities: {final_df['duid'].nunique()}")
    print(f" Time range: {final_df['interval_start'].min()} → {final_df['interval_start'].max()}")
    print(f" Columns: {list(final_df.columns)}")
    print(f" Missing power: {final_df['power_mw'].isna().sum():,}")
    print(f" Missing emissions: {final_df['emissions_tco2e'].isna().sum():,}")
    interval_count = final_df["interval_start"].nunique()
    if KEEP_INTERVALS:
        print(f" Intervals retained: {interval_count} (limit={KEEP_INTERVALS})")
    else:
        print(f" Intervals retained: {interval_count}")
    print("\n")
    print("=" * 70)
    print("Task 2 Complete — Ready for Task 3!")
    print("=" * 70)

if __name__ == "__main__":
    main()
