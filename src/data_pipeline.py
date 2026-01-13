"""
COMP5339 Assignment 2 – Task 1: Data Retrieval
Team: Priyansh Khandelwal, Zhenzhe Wu
"""

import os
import json
import warnings
import threading
from glob import glob
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from urllib3.exceptions import InsecureRequestWarning
from tqdm import tqdm

# Suppress SSL warnings
warnings.filterwarnings('ignore', category=InsecureRequestWarning)

# ================================
# CONFIG
# ================================
API_KEY = ""  # ← keep secret in env in real use
BASE_URL = "https://api.openelectricity.org.au/v4"
NEM_REGIONS = ["nsw", "qld", "vic", "sa", "tas"]
FACILITY_METRICS = ["power", "emissions"]
FALLBACK_METRICS = ["power"]  # used if emissions are not available
INTERVAL = "5m"
OUT_DIR = "output_data"
META_CSV = "outputs/facilities_nem.csv"
MIN_CAPACITY_MW = 100
MAX_FACILITY_WORKERS = int(os.getenv("FACILITY_WORKERS", "8"))
CACHE_DIR = "nemosis_cache"
WINDOW_DAYS = int(os.getenv("FACILITY_WINDOW_DAYS", "7"))
MARKET_WINDOW_DAYS = int(os.getenv("MARKET_WINDOW_DAYS", str(WINDOW_DAYS)))
FACILITY_DATE_START_OVERRIDE = os.getenv("FACILITY_DATE_START") or "2025-10-01T00:00:00"
FACILITY_DATE_END_OVERRIDE = os.getenv("FACILITY_DATE_END") or "2025-10-07T23:55:00"
MARKET_DATE_START_OVERRIDE = os.getenv("MARKET_DATE_START") or "2024-10-01T00:00:00"
MARKET_DATE_END_OVERRIDE = os.getenv("MARKET_DATE_END") or "2024-10-07T23:55:00"


# ================================
# SETUP
# ================================
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(META_CSV), exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

def _build_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"])  # explicit to avoid surprises
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update({
        "Authorization": f"Bearer {API_KEY}",
        "User-Agent": "Mozilla/5.0 (COMP5339 Assignment)"
    })
    return session


SESSION = _build_session()
_THREAD_LOCAL = threading.local()


def _get_thread_session():
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = _build_session()
        _THREAD_LOCAL.session = session
    return session

# ================================
# TIME WINDOW
# ================================
NEM_TZ = ZoneInfo("Australia/Brisbane")

def to_naive_net(t: datetime) -> str:
    return t.astimezone(NEM_TZ).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")

def parse_with_tz(value: str) -> datetime:
    """Parse ISO date/datetime strings and shift into the NEM timezone."""
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize(NEM_TZ)
    else:
        ts = ts.tz_convert(NEM_TZ)
    return ts.to_pydatetime()

def align_to_interval(dt: datetime, minutes: int = 5) -> datetime:
    """Floor to the nearest lower interval boundary (defaults to 5 minutes)."""
    return pd.Timestamp(dt).floor(f"{minutes}min").to_pydatetime()

def compute_window(start_override: str, end_override: str, window_days: int):
    if end_override:
        end_dt = align_to_interval(parse_with_tz(end_override))
    else:
        end_dt = align_to_interval(datetime.now(tz=NEM_TZ) - pd.Timedelta(minutes=5))

    if start_override:
        start_dt = align_to_interval(parse_with_tz(start_override))
    else:
        start_dt = align_to_interval(
            end_dt - (pd.Timedelta(days=window_days) - pd.Timedelta(minutes=5))
        )

    if start_dt >= end_dt:
        raise ValueError("Configured start date must be earlier than end date.")

    return start_dt, end_dt


facility_start_dt, facility_end_dt = compute_window(
    FACILITY_DATE_START_OVERRIDE, FACILITY_DATE_END_OVERRIDE, WINDOW_DAYS
)
market_start_dt, market_end_dt = compute_window(
    MARKET_DATE_START_OVERRIDE, MARKET_DATE_END_OVERRIDE, MARKET_WINDOW_DAYS
)

DATE_START, DATE_END = to_naive_net(facility_start_dt), to_naive_net(facility_end_dt)
MARKET_DATE_START, MARKET_DATE_END = to_naive_net(market_start_dt), to_naive_net(market_end_dt)

# ================================
# SAFE GET — Accept 200, 400, 404, 416, 422
# ================================
def _safe_get(url, params=None, session=None):
    try:
        session = session or SESSION
        r = session.get(url, params=params, timeout=120, verify=False)
        if r.status_code in (200, 400, 404, 416, 422):
            return r, None
        return None, f"HTTP {r.status_code}"
    except requests.exceptions.RequestException as e:
        return None, str(e)
    except Exception as e:
        return None, f"Unexpected error: {e}"

# ================================
# GET FACILITIES  (FIXED UNPACKING)
# ================================
def get_facilities_df(network_id: str = "NEM") -> pd.DataFrame:
    url = f"{BASE_URL}/facilities/"
    print(f"[GET] Fetching facility list from {url}")
    resp, err = _safe_get(url, params={"network_id": network_id})  # ← FIX: unpack
    if resp is None:
        print(f"   FATAL: Could not get facility list. Error: {err}")
        return pd.DataFrame()
    if resp.status_code != 200:
        print(f"   FATAL: Could not get facility list. Status: {resp.status_code}")
        return pd.DataFrame()

    try:
        payload = resp.json()
    except ValueError:
        print("   FATAL: Non-JSON response from facilities endpoint.")
        return pd.DataFrame()

    raw = payload.get("data", [])
    if not isinstance(raw, list):
        print("   FATAL: Unexpected facilities payload shape.")
        return pd.DataFrame()

    records = []

    # ---- PREFIX → FUEL LOOKUP (expanded for more matches) ----
    PREFIX_FUEL_MAP = {
        # COAL (expanded)
        "BAYSWATER": "COAL", "BW0": "COAL", "ER0": "COAL", "GLADSTONE": "COAL", "GSTONE": "COAL",
        "LD0": "COAL", "LIDDELL": "COAL", "MP": "COAL", "STAN": "COAL", "TARONG": "COAL",
        "VP": "COAL", "YALL": "COAL", "LYA": "COAL", "LYB": "COAL", "MUSWELL": "COAL",
        "CALLIDE": "COAL", "MILLMER": "COAL", "WALLUMB": "COAL", "VOYSEY": "COAL",

        # GAS (expanded)
        "AGLHAL": "GAS", "AGL_SOM": "GAS", "APPIN": "GAS", "BARROW": "GAS", "BARK": "GAS",
        "BELLBAY": "GAS", "BELB": "GAS", "BRAEMAR": "GAS", "COLONGRA": "GAS", "CSTONE": "GAS",
        "DARLING": "GAS", "DRYCRK": "GAS", "JERRA": "GAS", "KPP": "GAS", "KOGAN": "GAS",
        "MCG": "GAS", "MICA": "GAS", "MOUNT": "GAS", "NEWPORT": "GAS", "OSB": "GAS",
        "PELICAN": "GAS", "QPS": "GAS", "QUARANT": "GAS", "ROM": "GAS", "SHOAL": "GAS",
        "SITHE": "GAS", "SMPS": "GAS", "SNUG": "GAS", "SWAN": "GAS", "TALL": "GAS",
        "TAMW": "GAS", "TORRA": "GAS", "URANQ": "GAS", "WILP": "GAS", "OR_": "GAS", "AG_": "GAS",
        "HALLETT": "GAS", "TAMAR": "GAS", "RONA": "GAS",

        # WIND (expanded)
        "WF": "WIND", "WIND": "WIND", "ARWF": "WIND", "BALDH": "WIND", "BLUFF": "WIND",
        "BOCOR": "WIND", "CAPL": "WIND", "CATH": "WIND", "CHALL": "WIND", "CLEM": "WIND",
        "COLL": "WIND", "CROOK": "WIND", "GULLR": "WIND", "HALLET": "WIND", "HORN": "WIND",
        "KIK": "WIND", "LAKEBN": "WIND", "MACARTH": "WIND", "MTGEL": "WIND", "MTMILL": "WIND",
        "MURRA": "WIND", "OAKL": "WIND", "PORT": "WIND", "RYEP": "WIND", "SNOW": "WIND",
        "STARH": "WIND", "TARAL": "WIND", "WATERL": "WIND", "WAUB": "WIND", "WINDY": "WIND",
        "WOAK": "WIND", "WOL": "WIND", "WOODLWN": "WIND", "WPWF": "WIND", "YAMBUK": "WIND",
        "CATHROCK": "WIND", "CRRW": "WIND", "CULLEN": "WIND",

        # SOLAR (expanded)
        "SF": "SOLAR", "SOLAR": "SOLAR", "AGS": "SOLAR", "BANANA": "SOLAR", "BARCS": "SOLAR",
        "BROKENH": "SOLAR", "BULLI": "SOLAR", "CHICH": "SOLAR", "CLARE": "SOLAR", "COLEAMB": "SOLAR",
        "DARL": "SOLAR", "DAYD": "SOLAR", "EMERLD": "SOLAR", "GANN": "SOLAR", "GERM": "SOLAR",
        "HAYMAN": "SOLAR", "KIDSTON": "SOLAR", "LIMON": "SOLAR", "MANILDRA": "SOLAR", "MOREESF": "SOLAR",
        "NYNGAN": "SOLAR", "PARKES": "SOLAR", "ROBIN": "SOLAR", "ROSSR": "SOLAR", "SUNRAYS": "SOLAR",
        "SUSF": "SOLAR", "TAILEM": "SOLAR", "WHYALLA": "SOLAR", "WILGA": "SOLAR", "WINTON": "SOLAR", "YARRO": "SOLAR",
        "BROKEN": "SOLAR", "RIVERVIEW": "SOLAR",

        # BATTERY (expanded)
        "BAT": "BATTERY", "BESS": "BATTERY", "HBESS": "BATTERY", "HPRG": "BATTERY", "LBB": "BATTERY",
        "VBB": "BATTERY", "BALLBESS": "BATTERY", "DALBESS": "BATTERY", "HORNSDALE": "BATTERY",
        "TENNANT": "BATTERY", "LAKEBON": "BATTERY",

        # PUMPED HYDRO
        "PS": "PUMPED HYDRO", "TUMUT3": "PUMPED HYDRO", "UPPTUMUT": "PUMPED HYDRO", "SHOALHAVEN": "PUMPED HYDRO",
        "WHEALAHS": "PUMPED HYDRO",

        # HYDRO
        "HY": "HYDRO", "BLOWERING": "HYDRO", "GUTHEGA": "HYDRO", "HUME": "HYDRO", "KAREEYA": "HYDRO",
        "MURRAY": "HYDRO", "TUMUT1": "HYDRO", "TUMUT2": "HYDRO", "WARRAGAMBA": "HYDRO",
        "KIEWA": "HYDRO", "HINTON": "HYDRO",

        # DISTILLATE
        "DSL": "DISTILLATE", "DIESEL": "DISTILLATE", "BARKER": "DISTILLATE", "MACKAY": "DISTILLATE",
        "MCMAHON": "DISTILLATE", "MOUNT_STUART": "DISTILLATE", "RONA": "DISTILLATE",

        # BIOGAS
        "BG": "BIOGAS", "EARTHP": "BIOGAS", "MALABAR": "BIOGAS", "WOODLAWN": "BIOGAS",
    }

    for rec in raw:
        code = rec.get("facility_code") or rec.get("code")
        if not code:
            continue

        # ---- Capacity ----
        total_cap = 0.0
        for u in rec.get("units", []) or []:
            cap = u.get("capacity_registered")
            if isinstance(cap, (int, float)):
                total_cap += cap

        # ---- INITIAL FUEL: PREFIX MATCH ----
        fuel = "OTHER"
        code_upper = code.upper()
        name_upper = (rec.get("name", "") or "").upper()
        region = (rec.get("network_region", "") or "").upper()
        for prefix, ftype in PREFIX_FUEL_MAP.items():
            if prefix in code_upper:
                fuel = ftype
                break

        # ---- NAME-BASED INFERENCE (if still OTHER) ----
        if fuel == "OTHER":
            if any(word in name_upper for word in ["COAL", "BLACK", "BROWN", "THERMAL"]):
                fuel = "COAL"
            elif any(word in name_upper for word in ["GAS", "CCGT", "OCGT", "COMBINED CYCLE"]):
                fuel = "GAS"
            elif "HYDRO" in name_upper or "PUMP" in name_upper:
                fuel = "HYDRO" if "PUMP" not in name_upper else "PUMPED HYDRO"
            elif "BATTERY" in name_upper or "BESS" in name_upper:
                fuel = "BATTERY"
            elif "DIESEL" in name_upper or "DISTILLATE" in name_upper:
                fuel = "DISTILLATE"
            elif "BIO" in name_upper or "WASTE" in name_upper:
                fuel = "BIOGAS"

        # ---- REGION-BASED INFERENCE (if still OTHER) ----
        if fuel == "OTHER":
            if region == "QLD":
                fuel = "COAL"
            elif region == "VIC":
                fuel = "COAL"
            elif region == "NSW":
                fuel = "COAL"
            elif region == "SA":
                fuel = "GAS"
            elif region == "TAS":
                fuel = "HYDRO"

        # ---- Location ----
        lat = lon = None
        loc = rec.get("location")
        if isinstance(loc, dict):
            lat, lon = loc.get("lat"), loc.get("lng")
        if lat is None:
            lat = rec.get("latitude")
        if lon is None:
            lon = rec.get("longitude")

        records.append({
            "duid": code,
            "name": rec.get("name", "Unknown"),
            "fuel_type": fuel,
            "region": rec.get("network_region", "Unknown"),
            "latitude": lat,
            "longitude": lon,
            "capacity_mw": total_cap
        })

    df = pd.DataFrame(records)
    df.to_csv(META_CSV, index=False)
    print(f"\n   Saved {len(df)} facilities to {META_CSV}")
    if not df.empty and "fuel_type" in df:
        print("   Fuel-type distribution:")
        print(df["fuel_type"].value_counts().to_string())
    return df

# ================================
# FETCH ONE FACILITY
# ================================
def fetch_facility(code):
    """Fetch single facility’s power/emissions data (uses correct query structure)."""
    filename = f"facility_{code}_power_emissions_{DATE_START[:10]}_to_{DATE_END[:10]}.json"
    path = os.path.join(OUT_DIR, filename)
    if os.path.exists(path):
        return "success", "cached"

    url = f"{BASE_URL}/data/facilities/{'NEM'}"  # ✅ correct endpoint
    params = [
        ("metrics", "power"),
        ("metrics", "emissions"),
        ("interval", INTERVAL),
        ("facility_code", code),
        ("date_start", DATE_START),
        ("date_end", DATE_END),
    ]

    # Try API
    session = _get_thread_session()
    r, err = _safe_get(url, params=params, session=session)
    if not r:
        with open(path, "w") as f:
            json.dump({"error": err or "network error", "facility": code}, f, indent=2)
        return "failed", f"network error ({err})"

    if r.status_code == 200:
        try:
            data = r.json()
        except ValueError:
            data = {"raw": r.text}

        data.setdefault("metadata", {}).update({
            "metrics_requested": FACILITY_METRICS,
            "interval": INTERVAL,
            "date_start": DATE_START,
            "date_end": DATE_END,
            "fetched_at": datetime.utcnow().isoformat() + "Z"
        })

        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        return "success", "200 OK"

    elif r.status_code == 416:
        with open(path, "w") as f:
            json.dump({"facility": code, "note": "No data for selected window"}, f, indent=2)
        return "no_data", "HTTP 416"

    else:
        with open(path, "w") as f:
            json.dump({"facility": code, "error": f"HTTP {r.status_code}", "body": r.text[:500]}, f, indent=2)
        return "failed", f"HTTP {r.status_code}"

# ================================
# FETCH REAL MARKET DATA (AEMO)
# ================================
def fetch_market_data_aemo():
    print("\nFetching REAL market data from AEMO (this may take 3–5 min first time)...")

    start = market_start_dt.strftime("%Y/%m/%d %H:%M:%S")
    end = market_end_dt.strftime("%Y/%m/%d %H:%M:%S")
    filename = f"market_nem_price_demand_{MARKET_DATE_START[:10]}_to_{MARKET_DATE_END[:10]}.json"
    path = os.path.join(OUT_DIR, filename)

    if os.path.exists(path):
        print(f"   Already exists: {filename}")
        return

    print(f"   Downloading from {start} to {end}...")

    try:
        from nemosis import dynamic_data_compiler

        price_df = dynamic_data_compiler(
            start_time=start, end_time=end,
            table_name="DISPATCHPRICE", raw_data_location=CACHE_DIR,
            filter_cols=["REGIONID", "RRP"],
            filter_values=(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"], None)
        ).rename(columns={"SETTLEMENTDATE": "interval_start", "RRP": "price", "REGIONID": "network_region"})

        demand_df = dynamic_data_compiler(
            start_time=start, end_time=end,
            table_name="DISPATCHREGIONSUM", raw_data_location=CACHE_DIR,
            filter_cols=["REGIONID", "TOTALDEMAND"],
            filter_values=(["NSW1", "QLD1", "VIC1", "SA1", "TAS1"], None)
        ).rename(columns={"SETTLEMENTDATE": "interval_start", "TOTALDEMAND": "demand", "REGIONID": "network_region"})

        market_df = pd.merge(price_df, demand_df, on=["interval_start", "network_region"], how="outer")
        market_df = market_df[["interval_start", "network_region", "price", "demand"]]
        market_df["interval_start"] = market_df["interval_start"].dt.strftime('%Y-%m-%dT%H:%M:%S')

        market_json = {"data": market_df.to_dict(orient="records")}
        with open(path, "w") as f:
            json.dump(market_json, f, indent=2)

        print(f"   SUCCESS: Saved {len(market_df)} records → {filename}")

    except ImportError:
        print("   ERROR: nemosis not installed. Run: pip install nemosis")
    except Exception as e:
        print(f"   NEMOSIS ERROR: {e}")
        print("   Try again in 1 minute.")

# ================================
# MAIN
# ================================
def main():
    print("\nStarting COMP5339 Assignment 2 – Task 1")
    print(f"Facility Time: {DATE_START} to {DATE_END}")

    facilities_df = get_facilities_df()
    if facilities_df.empty:
        print("No facilities found. Exiting.")
        return

    fetch_market_data_aemo()

    big = facilities_df[facilities_df['capacity_mw'] >= MIN_CAPACITY_MW]
    print(f"\nFound {len(big)} big facilities (>=100 MW)")

    # Safely detect already-downloaded files
    existing = set()
    for f in glob(os.path.join(OUT_DIR, "facility_*.json")):
        try:
            duid = os.path.basename(f).split('_', 2)[1]
            if duid:
                existing.add(duid)
        except Exception:
            pass

    to_get = [c for c in big['duid'] if c not in existing]

    if not to_get:
        print("All facility data already downloaded!")
    else:
        print(f"\nDownloading {len(to_get)} facility files with up to {MAX_FACILITY_WORKERS} threads...")
        failures = []
        fallback_used = []

        workers = max(1, min(MAX_FACILITY_WORKERS, len(to_get)))
        try:
            progress = tqdm(total=len(to_get), desc="Facilities", unit="file", leave=False, colour="green")
        except TypeError:
            progress = tqdm(total=len(to_get), desc="Facilities", unit="file", leave=False)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {executor.submit(fetch_facility, code): code for code in to_get}
            for future in as_completed(future_map):
                code = future_map[future]
                try:
                    status, detail = future.result()
                except Exception as exc:
                    status, detail = "failed", f"exception: {exc}"

                if status != "success":
                    failures.append((code, detail))
                elif detail and "fallback" in detail.lower():
                    fallback_used.append(code)
                progress.update(1)

        progress.close()
        if failures:
            print(f"   Warning: {len(failures)} facilities failed (check {OUT_DIR})")
            for code, detail in failures[:5]:
                print(f"      - {code}: {detail}")
        if fallback_used:
            sample = ", ".join(fallback_used[:5])
            more = "" if len(fallback_used) <= 5 else " ..."
            print(f"   Fallback (power only) used for {len(fallback_used)} facilities: {sample}{more}")

    print("\n")
    print("=" * 70)
    print("TASK 1 COMPLETE! — Ready for Task 2!")
    print("=" * 70)

if __name__ == "__main__":
    main()

