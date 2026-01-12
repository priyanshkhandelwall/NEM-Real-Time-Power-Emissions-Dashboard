"""
COMP5339 Assignment 2 – MQTT Subscriber (Verification) with Assignment 1 Integration
Team: Priyansh Khandelwal, Zhenzhe Wu

Purpose:
- Subscribes to live power/emissions and market data via MQTT
- Collects messages in memory
- On Ctrl+C: aggregates latest interval → yearly totals → merges with Assignment 1
- Outputs: integrated_ass1_ass2.csv (same schema as Ass1)
"""

import json
import os
import time
import signal
import sys
import pandas as pd
from paho.mqtt import client as mqtt_client
from paho.mqtt.client import CallbackAPIVersion

# ================================
# CONFIG
# ================================
BROKER = 'test.mosquitto.org'
PORT = 1883
FACILITY_TOPIC = "COMP5339/power_emissions"
MARKET_TOPIC = "COMP5339/AEMO/market"

ASS1_CSV = "assignment1_data.csv"           # From preprocess_ass1.py
INTEGRATED_CSV = "integrated_ass1_ass2.csv"
META_CSV = "outputs/facilities_nem.csv"     # For name, region, fuel_type

# In-memory storage
facility_messages = []
market_messages = []

# ================================
# MQTT CALLBACKS
# ================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(FACILITY_TOPIC)
        client.subscribe(MARKET_TOPIC)
        print(f"Subscribed to: {FACILITY_TOPIC}, {MARKET_TOPIC}")
    else:
        print(f"Connection failed (code: {rc})")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        timestamp = time.strftime('%H:%M:%S')
        print(f"[{timestamp}] {msg.topic} → {payload}")

        data = json.loads(payload)
        if msg.topic == FACILITY_TOPIC:
            facility_messages.append(data)
        elif msg.topic == MARKET_TOPIC:
            market_messages.append(data)
    except Exception as e:
        print(f"Error decoding message: {e}")


# ================================
# INTEGRATION WITH ASSIGNMENT 1
# ================================
def integrate_with_assignment1():
    print("\n" + "="*70)
    print("INTEGRATING LIVE MQTT DATA WITH ASSIGNMENT 1")
    print("="*70)

    if not facility_messages:
        print("No facility data received. Skipping integration.")
        return

    # 1. Convert to DataFrame
    df = pd.DataFrame(facility_messages)
    if df.empty:
        print("Empty facility data.")
        return

    # 2. Clean & normalize
    df['interval_start'] = pd.to_datetime(df['interval_start'], utc=True, errors='coerce')
    df['power_mw'] = pd.to_numeric(df['power_mw'], errors='coerce')
    df['emissions_tco2e'] = pd.to_numeric(df['emissions_tco2e'], errors='coerce')
    df['facility_code'] = df['facility_code'].astype(str).str.upper()
    df = df.dropna(subset=['interval_start', 'facility_code'])

    if df.empty:
        print("No valid facility data after cleaning.")
        return

    # 3. Convert 5-min power to MWh
    df['interval_hours'] = 5 / 60.0
    df['power_mwh_yearly'] = df['power_mw'] * df['interval_hours']

    # 4. Extract year and aggregate
    df['year'] = df['interval_start'].dt.year
    agg = df.groupby(['facility_code', 'year']).agg(
        power_mwh_yearly=('power_mwh_yearly', 'sum'),
        emissions_tco2e_yearly=('emissions_tco2e', 'sum')
    ).reset_index()
    agg = agg.rename(columns={'facility_code': 'duid'})

    # 5. Load Assignment 1 data
    if not os.path.exists(ASS1_CSV):
        print(f"Missing {ASS1_CSV} — run: python preprocess_ass1.py")
        return
    ass1 = pd.read_csv(ASS1_CSV)
    ass1['duid'] = ass1['duid'].astype(str).str.upper()

    # 6. Add metadata (name, region, fuel_type)
    if os.path.exists(META_CSV):
        meta = pd.read_csv(META_CSV)[['duid', 'name', 'region', 'fuel_type']]
        agg = agg.merge(meta, on='duid', how='left')
    else:
        print(f"Warning: {META_CSV} not found — name/region/fuel_type will be missing.")

    # 7. Ensure same schema
    cols = ['duid', 'name', 'region', 'fuel_type', 'year', 'power_mwh_yearly', 'emissions_tco2e_yearly']
    ass1 = ass1[[c for c in cols if c in ass1.columns]]
    agg = agg[[c for c in cols if c in agg.columns]]

    # 8. Combine and dedupe
    integrated = pd.concat([ass1, agg], ignore_index=True)
    integrated = integrated.drop_duplicates(subset=['duid', 'year'])

    # 9. Save
    integrated.to_csv(INTEGRATED_CSV, index=False)
    print(f"Integration complete!")
    print(f"   • Ass1 rows: {len(ass1)}")
    print(f"   • Live (this session): {len(agg)}")
    print(f"   • Total integrated rows: {len(integrated)}")
    print(f"   • Saved: {INTEGRATED_CSV}")
    print("="*70)


# ================================
# GRACEFUL SHUTDOWN
# ================================
def signal_handler(sig, frame):
    print("\n\nSubscriber stopped by user (Ctrl+C).")
    integrate_with_assignment1()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


# ================================
# MAIN
# ================================
if __name__ == "__main__":
    client = mqtt_client.Client(callback_api_version=CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"Connecting to {BROKER}:{PORT}...")
    try:
        client.connect(BROKER, PORT, keepalive=60)
        print("Starting subscriber loop... (Press Ctrl+C to stop and integrate)")
        client.loop_forever()
    except Exception as e:
        print(f"Connection error: {e}")
        sys.exit(1)