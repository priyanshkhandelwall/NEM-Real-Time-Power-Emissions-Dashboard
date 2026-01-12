"""
COMP5339 Assignment 2 – Task 3: MQTT Publisher (Only New Records)
Team: Priyansh Khandelwal, Zhenzhe Wu
"""

import json
import pandas as pd
import time
import os
from glob import glob
from paho.mqtt import client as mqtt_client
from paho.mqtt.client import CallbackAPIVersion
import math

# ================================
# NEW: Track last published values to skip unchanged records
# ================================
_last_published = {}  # {(duid, interval_iso): (power, emissions)} or {(region, interval_iso): (price, demand)}

def _clean_number(value):
    """Return a JSON-safe float or None."""
    try:
        if value is None:
            return None
        value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def _interval_iso(ts):
    """Convert timestamps to ISO-8601 strings, returning None when invalid."""
    if isinstance(ts, pd.Timestamp):
        if pd.isna(ts):
            return None
        return ts.isoformat()
    try:
        parsed = pd.to_datetime(ts, utc=True, errors="coerce")
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.isoformat()


# ------------------- Load Data -------------------
def load_facility_dataframe(csv_file: str) -> pd.DataFrame:
    if not csv_file or not os.path.exists(csv_file):
        return pd.DataFrame()
    df = pd.read_csv(csv_file)
    if df.empty:
        return df
    df["interval_start"] = pd.to_datetime(df["interval_start"], utc=True, errors="coerce")
    df = df.dropna(subset=["interval_start"])
    df = df.sort_values(["interval_start", "duid"])
    return df


def load_market_dataframe(json_dir: str) -> pd.DataFrame:
    if not json_dir:
        return pd.DataFrame()
    candidates = []
    for pattern in MARKET_JSON_PATTERNS:
        candidates.extend(glob(os.path.join(json_dir, pattern)))
    if not candidates:
        return pd.DataFrame()
    latest = max(candidates, key=os.path.getmtime)
    try:
        with open(latest, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError):
        return pd.DataFrame()
    records = payload.get("data", [])
    if not isinstance(records, list):
        return pd.DataFrame()
    df = pd.DataFrame.from_records(records)
    if df.empty:
        return df
    df = df.rename(columns={"network_region": "region"})
    df["region"] = df["region"].astype(str).str.upper()
    df["interval_start"] = pd.to_datetime(df["interval_start"], utc=True, errors="coerce")
    df["price"] = pd.to_numeric(df.get("price"), errors="coerce")
    df["demand"] = pd.to_numeric(df.get("demand"), errors="coerce")
    for col in ("price", "demand"):
        if col in df:
            negatives = df[col] < 0
            if negatives.any():
                df.loc[negatives, col] = pd.NA
    df = df.dropna(subset=["interval_start", "region"])
    df = df.dropna(subset=["price", "demand"], how="all")
    if not df.empty:
        df["source"] = "real"
    return df


# ------------------- MQTT Client Setup -------------------
def connect_mqtt():
    client_id = f"publisher-{time.time()}"
    client = mqtt_client.Client(callback_api_version=CallbackAPIVersion.VERSION1, client_id=client_id)
    client.connect("test.mosquitto.org", 1883)
    client.loop_start()
    return client


# ------------------- Publish with Change Detection -------------------
def publish_facility_batch(client, interval_ts, group):
    if group.empty:
        return
    interval_iso = _interval_iso(interval_ts)
    for _, row in group.sort_values("duid").iterrows():
        duid = row["duid"]
        key = (duid, interval_iso)
        new_vals = (_clean_number(row.get("power_mw")), _clean_number(row.get("emissions_tco2e")))
        if _last_published.get(key) == new_vals:
            continue  # unchanged → skip
        _last_published[key] = new_vals
        payload = {
            "facility_code": duid,
            "interval_start": interval_iso,
            "power_mw": new_vals[0],
            "emissions_tco2e": new_vals[1],
        }
        client.publish("COMP5339/power_emissions", json.dumps(payload))
        time.sleep(0.01)  # tiny delay to avoid broker overload


def publish_market_batch(client, interval_ts, rows):
    if not rows:
        return
    interval_iso = _interval_iso(interval_ts)
    for row in (rows if isinstance(rows, list) else [rows]):
        region = row.get("region")
        key = (region, interval_iso)
        new_vals = (_clean_number(row.get("price")), _clean_number(row.get("demand")))
        if _last_published.get(key) == new_vals:
            continue
        _last_published[key] = new_vals
        payload = {
            "region": region,
            "interval_start": interval_iso,
            "price": new_vals[0],
            "demand": new_vals[1],
        }
        client.publish("COMP5339/AEMO/market", json.dumps(payload))
        time.sleep(0.01)


# ------------------- Main Publishing Logic -------------------
MARKET_JSON_PATTERNS = [
    "facility_market_nem_price_demand_*.json",
    "market_nem_price_demand_*.json",
]
CONSOLIDATED_CSV = "consolidated_facility_data.csv"
MARKET_JSON_DIR = "output_data"


def publish_data():
    global _last_published
    _last_published = {}  # reset on each run

    client = connect_mqtt()
    time.sleep(1)  # wait for connection

    facility_df = load_facility_dataframe(CONSOLIDATED_CSV)
    if facility_df.empty:
        print("No facility data to publish.")
    else:
        print(f"Publishing facility data ({facility_df['interval_start'].nunique()} intervals)")

    market_df = load_market_dataframe(MARKET_JSON_DIR)
    if market_df.empty:
        print("No market data available.")
    else:
        print(f"Publishing market data ({market_df['interval_start'].nunique()} intervals)")

    # Group & schedule
    facility_groups = {k: g for k, g in facility_df.groupby("interval_start")} if not facility_df.empty else {}
    market_groups = {k: g for k, g in market_df.groupby("interval_start")} if not market_df.empty else {}

    facility_schedule = []
    for interval_key, group in facility_groups.items():
        ts = pd.to_datetime(interval_key, utc=True, errors="coerce")
        for _, row in group.iterrows():
            facility_schedule.append((ts, interval_key, row.get("duid")))
    facility_schedule.sort(key=lambda x: (x[0] if x[0] is not pd.NaT else pd.Timestamp.min, x[2] or ""))

    market_schedule = []
    for interval_key, rows in market_groups.items():
        ts = pd.to_datetime(interval_key, utc=True, errors="coerce")
        for row in rows.to_dict("records"):
            market_schedule.append((ts, interval_key, row))
    market_schedule.sort(key=lambda x: (x[0] if x[0] is not pd.NaT else pd.Timestamp.min, x[2].get("region", "")))

    f_idx = m_idx = 0
    total_f = len(facility_schedule)
    total_m = len(market_schedule)

    if total_f == 0 and total_m == 0:
        print("Nothing to publish.")
    else:
        while f_idx < total_f or m_idx < total_m:
            if f_idx < total_f:
                _, interval_str, duid = facility_schedule[f_idx]
                group = facility_groups.get(interval_str)
                if group is not None:
                    single_row = group[group["duid"] == duid]
                    publish_facility_batch(client, pd.to_datetime(interval_str, utc=True), single_row)
                f_idx += 1
            if m_idx < total_m:
                ts, interval_str, row = market_schedule[m_idx]
                publish_market_batch(client, ts if ts is not pd.NaT else pd.to_datetime(interval_str, utc=True), [row])
                m_idx += 1
            time.sleep(0.1)

    client.loop_stop()
    client.disconnect()
    print("All **new** data published.")


if __name__ == "__main__":
    publish_data()