"""COMP5339 Assignment 2 - Task 5: Live MQTT Dashboard (Dash implementation)
Run with: python dashboard.py
"""

import json
import os
import queue
import threading
import uuid
from typing import Any, Dict, List, Optional

import pandas as pd
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

from dash import Dash, Input, Output, State, dash_table, dcc, html
import plotly.graph_objects as go

MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
FACILITY_TOPIC = "COMP5339/power_emissions"
MARKET_TOPIC = "COMP5339/AEMO/market"
META_CSV = "outputs/facilities_nem.csv"
MAX_INTERVAL_HISTORY = 24
DEFAULT_REFRESH_SECONDS = 10
DEFAULT_MAP_CENTER = (-25.0, 134.0)
DEFAULT_LAT_RANGE = [-45, -5]
DEFAULT_LON_RANGE = [110, 160]

FUEL_COLOR_MAPPING = {
    "SOLAR": "#f6d55c",
    "WIND": "#3caea3",
    "HYDRO": "#20639b",
    "COAL": "#ed553b",
    "GAS": "#c70039",
}
DEFAULT_MARKER_COLOR = "#636efa"

dashboard_state: Dict[str, Any] = {
    "latest_facility": {},
    "latest_market": {},
    "facility_intervals": {},
    "market_intervals": {},
    "current_facility_interval": None,
    "current_market_interval": None,
    "mqtt_connected": False,
    "mqtt_error": None,
    "message_count": 0,
}
state_lock = threading.Lock()
message_queue: "queue.Queue[Any]" = queue.Queue()
mqtt_client: Optional[mqtt.Client] = None
worker_thread: Optional[threading.Thread] = None


def normalize_interval(value: Optional[Any]) -> Optional[str]:
    if value is None:
        return None
    try:
        ts = pd.to_datetime(value, utc=True)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return ts.isoformat()


def prune_interval_store(store: Dict[str, Dict[str, Any]]) -> None:
    if len(store) <= MAX_INTERVAL_HISTORY:
        return
    parsed: List[Any] = []
    for key in list(store.keys()):
        ts = pd.to_datetime(key, utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        parsed.append((ts, key))
    parsed.sort()
    while len(parsed) > MAX_INTERVAL_HISTORY:
        _, old_key = parsed.pop(0)
        store.pop(old_key, None)


def _latest_interval_key(interval_dict: Dict[str, Dict[str, Any]]) -> Optional[str]:
    if not interval_dict:
        return None
    parsed: List[Any] = []
    for key in interval_dict:
        ts = pd.to_datetime(key, utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        parsed.append((ts, key))
    if not parsed:
        return None
    parsed.sort()
    return parsed[-1][1]


def update_current_interval_locked() -> None:
    dashboard_state["current_facility_interval"] = _latest_interval_key(
        dashboard_state["facility_intervals"]
    )
    dashboard_state["current_market_interval"] = _latest_interval_key(
        dashboard_state["market_intervals"]
    )


def to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
        if pd.isna(out):
            return None
        return out
    except (TypeError, ValueError):
        return None


def format_number(value: Optional[float], decimals: int = 2, suffix: str = "") -> str:
    if value is None:
        return "N/A"
    try:
        if pd.isna(value):
            return "N/A"
    except TypeError:
        pass
    return f"{value:,.{decimals}f}{suffix}"


def format_price(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    try:
        if pd.isna(value):
            return "N/A"
    except TypeError:
        pass
    return f"${value:,.2f}/MWh"


def format_demand(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    try:
        if pd.isna(value):
            return "N/A"
    except TypeError:
        pass
    return f"{value:,.0f} MW"


def assign_marker_color(fuel: Any) -> str:
    if fuel is None:
        return DEFAULT_MARKER_COLOR
    fuel_text = str(fuel).upper()
    for key, color in FUEL_COLOR_MAPPING.items():
        if key in fuel_text:
            return color
    return DEFAULT_MARKER_COLOR


def handle_facility_message(message: Dict[str, Any]) -> None:
    raw_code = message.get("facility_code")
    code = str(raw_code).strip().upper() if raw_code is not None else ""
    interval_iso = normalize_interval(message.get("interval_start"))
    raw_region = message.get("region")
    region = str(raw_region).strip().upper() if raw_region is not None else ""

    if not code or not interval_iso:
        return

    normalized: Dict[str, Any] = dict(message)
    normalized["facility_code"] = code
    normalized["interval_start"] = interval_iso
    if region:
        normalized["region"] = region

    price_val = to_float(message.get("price"))
    demand_val = to_float(message.get("demand"))
    power_val = to_float(message.get("power_mw"))
    emissions_val = to_float(message.get("emissions_tco2e"))

    if price_val is not None:
        normalized["price"] = price_val
    if demand_val is not None:
        normalized["demand"] = demand_val
    if power_val is not None:
        normalized["power_mw"] = power_val
    if emissions_val is not None:
        normalized["emissions_tco2e"] = emissions_val

    dashboard_state["latest_facility"][code] = normalized

    facility_store = dashboard_state["facility_intervals"].setdefault(interval_iso, {})
    facility_store[code] = normalized
    prune_interval_store(dashboard_state["facility_intervals"])

    update_current_interval_locked()


def handle_market_message(message: Dict[str, Any]) -> None:
    raw_region = message.get("region")
    region = str(raw_region).strip().upper() if raw_region is not None else ""
    interval_iso = normalize_interval(message.get("interval_start"))

    if not region or not interval_iso:
        return

    normalized: Dict[str, Any] = dict(message)
    normalized["region"] = region
    normalized["interval_start"] = interval_iso

    price_val = to_float(message.get("price"))
    demand_val = to_float(message.get("demand"))
    if price_val is not None:
        normalized["price"] = price_val
    if demand_val is not None:
        normalized["demand"] = demand_val

    dashboard_state["latest_market"][region] = normalized

    market_store = dashboard_state["market_intervals"].setdefault(interval_iso, {})
    market_store[region] = normalized
    prune_interval_store(dashboard_state["market_intervals"])

    update_current_interval_locked()


def on_connect(
    client: mqtt.Client, userdata: Dict[str, Any], flags: Dict[str, Any], rc: int, props: Any = None
) -> None:
    queue_ref = userdata.get("queue") if isinstance(userdata, dict) else None
    if queue_ref:
        queue_ref.put(("connect", rc))


def on_message(client: mqtt.Client, userdata: Dict[str, Any], msg: mqtt.MQTTMessage) -> None:
    queue_ref = userdata.get("queue") if isinstance(userdata, dict) else None
    if queue_ref:
        queue_ref.put(("message", msg.topic, msg.payload))


def message_worker() -> None:
    while True:
        item = message_queue.get()
        if not item:
            message_queue.task_done()
            continue
        kind = item[0]
        if kind == "connect":
            rc = item[1]
            if rc == 0:
                with state_lock:
                    dashboard_state["mqtt_connected"] = True
                    dashboard_state["mqtt_error"] = None
                if mqtt_client is not None:
                    mqtt_client.subscribe(FACILITY_TOPIC)
                    mqtt_client.subscribe(MARKET_TOPIC)
            else:
                with state_lock:
                    dashboard_state["mqtt_connected"] = False
                    dashboard_state["mqtt_error"] = f"Connection failed: return code {rc}"
        elif kind == "message":
            topic, payload = item[1], item[2]
            try:
                data = json.loads(payload.decode())
            except Exception:
                message_queue.task_done()
                continue
            with state_lock:
                dashboard_state["message_count"] += 1
                if topic == FACILITY_TOPIC:
                    handle_facility_message(data)
                elif topic == MARKET_TOPIC:
                    handle_market_message(data)
        message_queue.task_done()


def start_mqtt() -> None:
    global mqtt_client, worker_thread
    if mqtt_client is not None:
        return

    client = mqtt.Client(
        client_id=f"dash-{uuid.uuid4()}",
        callback_api_version=CallbackAPIVersion.VERSION1,
    )
    client.user_data_set({"queue": message_queue})
    client.on_connect = on_connect
    client.on_message = on_message

    mqtt_client = client
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()
    except Exception as exc:
        with state_lock:
            dashboard_state["mqtt_error"] = f"MQTT connection failed: {exc}"
            dashboard_state["mqtt_connected"] = False
    else:
        with state_lock:
            dashboard_state["mqtt_error"] = None

    if worker_thread is None or not worker_thread.is_alive():
        worker_thread = threading.Thread(target=message_worker, daemon=True)
        worker_thread.start()


def load_metadata() -> pd.DataFrame:
    if not os.path.exists(META_CSV):
        return pd.DataFrame(columns=["duid", "name", "fuel_type", "region", "latitude", "longitude"])
    df = pd.read_csv(META_CSV, dtype={"duid": str})
    df = df.dropna(subset=["latitude", "longitude"], how="any")
    df["duid_clean"] = df["duid"].astype(str).str.strip().str.upper()
    return df


start_mqtt()
meta_df = load_metadata()
if "duid_clean" not in meta_df.columns:
    meta_df["duid_clean"] = meta_df.get("duid", pd.Series(dtype=str)).astype(str).str.strip().str.upper()

if meta_df.empty:
    region_options: List[str] = []
    fuel_options: List[str] = []
else:
    region_options = sorted(meta_df["region"].dropna().unique().tolist())
    fuel_options = sorted(meta_df["fuel_type"].dropna().unique().tolist())

default_regions = region_options.copy()
default_fuels = fuel_options.copy()

app = Dash(__name__)
app.title = "NEM Live Dashboard"


def build_metric_card(
    title: str, value: str, subtitle: Optional[str] = None, value_color: str = "#1f2937"
) -> html.Div:
    return html.Div(
        [
            html.Div(title, style={"fontSize": "0.85rem", "color": "#69707a"}),
            html.Div(
                value,
                style={
                    "fontSize": "1.75rem",
                    "fontWeight": "600",
                    "marginTop": "0.25rem",
                    "color": value_color,
                },
            ),
            html.Div(
                subtitle,
                style={"fontSize": "0.75rem", "color": "#878e96", "marginTop": "0.25rem"},
            )
            if subtitle
            else None,
        ],
        style={
            "padding": "1rem",
            "border": "1px solid #e1e4e8",
            "borderRadius": "0.75rem",
            "backgroundColor": "#ffffff",
            "minWidth": "180px",
            "boxShadow": "0 1px 2px rgba(15, 23, 42, 0.08)",
        },
    )


def build_market_tiles(rows: List[Dict[str, Any]]) -> List[html.Div]:
    tiles: List[html.Div] = []
    for row in rows:
        tiles.append(
            html.Div(
                [
                    html.Div(row.get("region", "Unknown"), style={"fontSize": "1rem", "fontWeight": "600"}),
                    html.Div(
                        format_price(row.get("price")),
                        style={"fontSize": "1.35rem", "marginTop": "0.5rem"},
                    ),
                    html.Div(
                        format_demand(row.get("demand")),
                        style={"fontSize": "0.95rem", "marginTop": "0.25rem", "color": "#555"},
                    ),
                    html.Div(
                        f"Time: {row.get('interval_start', 'N/A')}",
                        style={"fontSize": "0.75rem", "marginTop": "0.75rem", "color": "#666"},
                    ),
                ],
                style={
                    "padding": "1rem",
                    "border": "1px solid #e1e4e8",
                    "borderRadius": "0.75rem",
                    "backgroundColor": "#ffffff",
                    "boxShadow": "0 1px 2px rgba(15, 23, 42, 0.08)",
                },
            )
        )
    return tiles


def build_live_table(live_df: pd.DataFrame) -> dash_table.DataTable:
    display_df = live_df.copy()
    numeric_cols = [col for col in ["power_mw", "emissions_tco2e", "price", "demand"] if col in display_df.columns]
    for col in numeric_cols:
        display_df[col] = display_df[col].apply(lambda x: None if pd.isna(x) else round(float(x), 4))
    display_df = display_df.reset_index().rename(columns={"index": "facility_code"})
    columns = [{"name": col.replace("_", " ").title(), "id": col} for col in display_df.columns]
    return dash_table.DataTable(
        columns=columns,
        data=display_df.to_dict("records"),
        page_size=15,
        sort_action="native",
        filter_action="native",
        style_table={"overflowX": "auto"},
        style_header={"fontWeight": "600", "backgroundColor": "#f6f8fa"},
        style_cell={"fontSize": "0.85rem", "padding": "0.4rem"},
        style_data={"backgroundColor": "#ffffff", "border": "1px solid #f0f0f0"},
    )


def build_map_figure(
    filtered_live: pd.DataFrame,
    latest_facilities: Dict[str, Dict[str, Any]],
    metric_key: str,
    metric_label: str,
    lock_map_view: bool,
) -> go.Figure:
    fig = go.Figure()
    markers_lat: List[float] = []
    markers_lon: List[float] = []
    hover_text: List[str] = []
    marker_colors: List[str] = []
    marker_sizes: List[float] = []

    if not filtered_live.empty:
        for _, row in filtered_live.iterrows():
            lat = row.get("latitude")
            lon = row.get("longitude")
            if pd.isna(lat) or pd.isna(lon):
                continue
            code = str(row.get("duid_clean"))
            latest = latest_facilities.get(code, {})
            value = latest.get(metric_key)
            display_value = format_number(value)
            power_text = format_number(latest.get("power_mw"))
            emissions_text = format_number(latest.get("emissions_tco2e"))
            hover_text.append(
                (
                    f"<b>{row.get('name', 'Unknown')}</b><br>"
                    f"Fuel: {row.get('fuel_type', 'Unknown')}<br>"
                    f"Region: {row.get('region', 'Unknown')}<br>"
                    f"{metric_label}: {display_value}<br>"
                    f"Power (MW): {power_text}<br>"
                    f"Emissions (tCO₂e): {emissions_text}"
                )
            )
            markers_lat.append(lat)
            markers_lon.append(lon)
            marker_colors.append(assign_marker_color(row.get("fuel_type")))
            if isinstance(value, (int, float)) and not pd.isna(value):
                marker_sizes.append(max(9, min(22, 9 + abs(float(value)) ** 0.5)))
            else:
                marker_sizes.append(10)

    if markers_lat:
        fig.add_trace(
            go.Scattergeo(
                lat=markers_lat,
                lon=markers_lon,
                mode="markers",
                marker=dict(
                    size=marker_sizes,
                    color=marker_colors,
                    line=dict(width=0.5, color="#1f1f1f"),
                    opacity=0.85,
                ),
                text=hover_text,
                hovertemplate="%{text}<extra></extra>",
            )
        )

    if lock_map_view or not markers_lat:
        fig.update_geos(
            showcountries=True,
            landcolor="#f8f9fb",
            lataxis=dict(range=DEFAULT_LAT_RANGE),
            lonaxis=dict(range=DEFAULT_LON_RANGE),
            center=dict(lat=DEFAULT_MAP_CENTER[0], lon=DEFAULT_MAP_CENTER[1]),
        )
    else:
        fig.update_geos(
            showcountries=True,
            landcolor="#f8f9fb",
            fitbounds="locations",
        )

    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        geo=dict(bgcolor="rgba(0,0,0,0)"),
        paper_bgcolor="#ffffff",
    )
    return fig


app.layout = html.Div(
    [
        dcc.Interval(id="refresh-interval", interval=DEFAULT_REFRESH_SECONDS * 1000, disabled=False),
        html.H1("NEM Live Electricity Dashboard"),
        html.P("Real-time power, emissions, price & demand"),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Region"),
                        dcc.Dropdown(
                            id="region-filter",
                            options=[{"label": region, "value": region} for region in region_options],
                            value=default_regions,
                            multi=True,
                            placeholder="Select region(s)",
                        ),
                    ],
                    style={"flex": "1", "minWidth": "220px"},
                ),
                html.Div(
                    [
                        html.Label("Fuel Type"),
                        dcc.Dropdown(
                            id="fuel-filter",
                            options=[{"label": fuel, "value": fuel} for fuel in fuel_options],
                            value=default_fuels,
                            multi=True,
                            placeholder="Select fuel type(s)",
                        ),
                    ],
                    style={"flex": "1", "minWidth": "220px"},
                ),
                html.Div(
                    [
                        html.Label("Metric"),
                        dcc.RadioItems(
                            id="metric-toggle",
                            options=[
                                {"label": "Power (MW)", "value": "power"},
                                {"label": "Emissions (tCO₂e)", "value": "emissions"},
                            ],
                            value="power",
                            labelStyle={"display": "inline-block", "marginRight": "0.75rem"},
                            inputStyle={"marginRight": "0.25rem"},
                        ),
                    ],
                    style={"display": "flex", "flexDirection": "column", "minWidth": "220px"},
                ),
            ],
            style={"display": "flex", "gap": "1rem", "flexWrap": "wrap"},
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Auto-refresh"),
                        dcc.Checklist(
                            id="auto-refresh-toggle",
                            options=[{"label": " Enable", "value": "auto"}],
                            value=["auto"],
                            style={"marginTop": "0.5rem"},
                        ),
                        html.Label("Refresh interval (seconds)", style={"marginTop": "0.75rem"}),
                        dcc.Slider(
                            id="refresh-interval-slider",
                            min=5,
                            max=60,
                            value=DEFAULT_REFRESH_SECONDS,
                            step=5,
                            marks={5: "5", 10: "10", 20: "20", 30: "30", 45: "45", 60: "60"},
                        ),
                        html.Button("Refresh now", id="refresh-button", style={"marginTop": "0.75rem"}),
                    ],
                    style={"minWidth": "220px"},
                ),
                html.Div(
                    [
                        html.Label("Display options"),
                        dcc.Checklist(
                            id="map-options",
                            options=[{"label": " Lock map view", "value": "lock"}],
                            value=["lock"],
                            style={"marginTop": "0.5rem"},
                        ),
                        dcc.Checklist(
                            id="show-table-toggle",
                            options=[{"label": " Show live facility table", "value": "table"}],
                            value=[],
                            style={"marginTop": "0.75rem"},
                        ),
                    ],
                    style={"minWidth": "220px"},
                ),
            ],
            style={"display": "flex", "gap": "2rem", "flexWrap": "wrap", "marginTop": "1rem"},
        ),
        html.Div(id="metadata-warning", style={"color": "#b22222", "marginTop": "0.75rem", "fontWeight": "600"}),
        html.Div(id="status-banner", style={"marginTop": "0.5rem"}),
        html.Div(id="metric-container", style={"display": "flex", "gap": "1rem", "flexWrap": "wrap", "marginTop": "1rem"}),
        html.Div(id="interval-caption", style={"marginTop": "0.5rem", "fontSize": "0.95rem", "color": "#555"}),
        html.Div(id="map-note", style={"marginTop": "0.5rem", "color": "#666"}),
        dcc.Graph(id="facility-map", style={"height": "600px", "marginTop": "0.5rem"}),
        html.Hr(),
        html.H3("Market Data (Live)", style={"marginTop": "1.5rem"}),
        html.Div(
            id="market-metrics",
            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))", "gap": "1rem"},
        ),
        html.Div(id="live-table-container", style={"marginTop": "1.5rem"}),
    ],
    style={"padding": "1.5rem", "fontFamily": "Arial, sans-serif", "backgroundColor": "#f5f7fb"},
)


@app.callback(
    Output("refresh-interval", "interval"),
    Output("refresh-interval", "disabled"),
    Input("refresh-interval-slider", "value"),
    Input("auto-refresh-toggle", "value"),
)
def configure_refresh_interval(slider_value: Optional[float], auto_value: Optional[List[str]]):
    seconds = int(slider_value or DEFAULT_REFRESH_SECONDS)
    enabled = auto_value is not None and "auto" in auto_value
    return seconds * 1000, not enabled


@app.callback(
    Output("metadata-warning", "children"),
    Output("status-banner", "children"),
    Output("metric-container", "children"),
    Output("interval-caption", "children"),
    Output("facility-map", "figure"),
    Output("market-metrics", "children"),
    Output("live-table-container", "children"),
    Output("map-note", "children"),
    Input("refresh-interval", "n_intervals"),
    Input("refresh-button", "n_clicks"),
    State("region-filter", "value"),
    State("fuel-filter", "value"),
    State("metric-toggle", "value"),
    State("map-options", "value"),
    State("show-table-toggle", "value"),
)
def refresh_dashboard(
    _n_intervals: int,
    _n_clicks: Optional[int],
    selected_regions: Optional[List[str]],
    selected_fuels: Optional[List[str]],
    metric_choice: Optional[str],
    map_options: Optional[List[str]],
    table_options: Optional[List[str]],
):
    metric_choice = metric_choice or "power"
    metric_key = "power_mw" if metric_choice == "power" else "emissions_tco2e"
    metric_label = "Power (MW)" if metric_choice == "power" else "Emissions (tCO₂e)"

    regions = selected_regions if selected_regions is not None else default_regions
    fuels = selected_fuels if selected_fuels is not None else default_fuels

    if meta_df.empty:
        filtered = meta_df.copy()
    else:
        filtered = meta_df[
            meta_df["region"].isin(regions)
            & meta_df["fuel_type"].isin(fuels)
        ].copy()
    filtered_codes = set(filtered["duid_clean"].tolist()) if not filtered.empty else set()

    with state_lock:
        facility_interval = dashboard_state["current_facility_interval"]
        market_interval = dashboard_state["current_market_interval"]
        current_facilities = (
            dict(dashboard_state["facility_intervals"].get(facility_interval, {}))
            if facility_interval
            else {}
        )
        current_market = (
            dict(dashboard_state["market_intervals"].get(market_interval, {}))
            if market_interval
            else {}
        )
        latest_facilities = dict(dashboard_state["latest_facility"])
        latest_market_snapshot = dict(dashboard_state["latest_market"])
        message_count = dashboard_state["message_count"]
        mqtt_connected = dashboard_state["mqtt_connected"]
        mqtt_error = dashboard_state["mqtt_error"]
        facility_interval_count = len(dashboard_state["facility_intervals"])
        market_interval_count = len(dashboard_state["market_intervals"])

    live_codes = {code for code in latest_facilities.keys() if code in filtered_codes}
    filtered_live = (
        filtered[filtered["duid_clean"].isin(live_codes)].copy()
        if live_codes and not filtered.empty
        else pd.DataFrame(columns=filtered.columns)
    )
    live_facility_count = len(live_codes)

    lock_map_view = map_options is not None and "lock" in map_options
    show_table = table_options is not None and "table" in table_options

    metadata_warning = (
        ""
        if not meta_df.empty
        else "Missing outputs/facilities_nem.csv — run Task 1 first!"
    )

    if mqtt_error:
        status_banner = html.Div(
            mqtt_error,
            style={"color": "#b22222", "fontWeight": "600"},
        )
        mqtt_value = "Error"
        mqtt_color = "#b22222"
        mqtt_subtitle = "See message above"
    elif mqtt_connected:
        status_banner = html.Div(
            "MQTT connected.",
            style={"color": "#2e8b57", "fontWeight": "600"},
        )
        mqtt_value = "Connected"
        mqtt_color = "#2e8b57"
        mqtt_subtitle = "Broker online"
    else:
        status_banner = html.Div(
            "Connecting to MQTT broker...",
            style={"color": "#8a6d3b", "fontWeight": "600"},
        )
        mqtt_value = "Connecting"
        mqtt_color = "#8a6d3b"
        mqtt_subtitle = "Awaiting connection"

    metrics = [
        build_metric_card("MQTT", mqtt_value, subtitle=mqtt_subtitle, value_color=mqtt_color),
        build_metric_card("Live Facilities", f"{live_facility_count}", subtitle="Matching filters"),
        build_metric_card("Messages Received", f"{message_count:,}"),
    ]

    facility_display = facility_interval or "Waiting for facility data..."
    market_display = market_interval or "Waiting for market data..."
    interval_caption_children = [
        html.Span(f"Facility interval: {facility_display}"),
        html.Br(),
        html.Span(f"Market interval: {market_display}"),
        html.Br(),
        html.Span(
            f"Stored intervals — Facilities: {facility_interval_count} | Market: {market_interval_count}"
        ),
    ]

    map_note = "No live facility data matching your filters yet." if filtered_live.empty else ""

    map_fig = build_map_figure(filtered_live, latest_facilities, metric_key, metric_label, lock_map_view)

    if current_market:
        market_rows = sorted(current_market.values(), key=lambda row: row.get("region", ""))
        lead = html.Div(
            f"Interval: {market_interval}",
            style={"fontSize": "0.85rem", "color": "#555", "marginBottom": "0.5rem"},
        )
    elif latest_market_snapshot:
        market_rows = sorted(latest_market_snapshot.values(), key=lambda row: row.get("region", ""))
        lead = html.Div(
            "Waiting for matching facility interval — showing latest available market snapshot.",
            style={"fontSize": "0.85rem", "color": "#555", "marginBottom": "0.5rem"},
        )
    else:
        market_rows = []
        lead = html.Div(
            "Waiting for market data...",
            style={"fontSize": "0.95rem", "color": "#666"},
        )

    market_children = [lead] + build_market_tiles(market_rows) if market_rows else [lead]

    table_children: List[Any] = []
    if show_table:
        if current_facilities:
            live_df = pd.DataFrame.from_dict(current_facilities, orient="index")
            table_children.append(build_live_table(live_df))
        elif latest_facilities:
            table_children.append(
                html.Div(
                    "Waiting for matching market interval — live table will populate once data aligns.",
                    style={"fontSize": "0.9rem", "color": "#555"},
                )
            )
        else:
            table_children.append(
                html.Div(
                    "No facility data yet.",
                    style={"fontSize": "0.9rem", "color": "#555"},
                )
            )

    return (
        metadata_warning,
        status_banner,
        metrics,
        interval_caption_children,
        map_fig,
        market_children,
        table_children,
        map_note,
    )

# --------------------------------------------------------------
# BONUS: Integrate live MQTT data with Assignment 1 (yearly schema)
# --------------------------------------------------------------
import signal
import sys

ASS1_CSV = "assignment1_data.csv"           # From preprocess_ass1.py
INTEGRATED_CSV = "integrated_ass1_ass2.csv"

def integrate_with_assignment1_on_shutdown():
    print("\n" + "="*60)
    print("INTEGRATING LIVE MQTT DATA WITH ASSIGNMENT 1...")
    print("="*60)

    # 1. Get the latest facility interval from dashboard state
    with state_lock:
        interval_key = dashboard_state["current_facility_interval"]
        if not interval_key or interval_key not in dashboard_state["facility_intervals"]:
            print("No live facility data available yet.")
            return
        live_data = dashboard_state["facility_intervals"][interval_key]

    if not live_data:
        print("Empty live data.")
        return

    # 2. Convert to DataFrame
    df = pd.DataFrame.from_dict(live_data, orient="index")
    df["interval_start"] = pd.to_datetime(df["interval_start"], utc=True)
    df["power_mw"] = pd.to_numeric(df["power_mw"], errors="coerce")
    df["emissions_tco2e"] = pd.to_numeric(df["emissions_tco2e"], errors="coerce")
    df["year"] = df["interval_start"].dt.year

    # 3. Convert 5-min power to MWh (for yearly total)
    df["interval_hours"] = 5 / 60.0
    df["power_mwh_yearly"] = df["power_mw"] * df["interval_hours"]

    # 4. Aggregate to yearly per DUID
    agg = df.groupby(["facility_code", "year"]).agg(
        power_mwh_yearly=("power_mwh_yearly", "sum"),
        emissions_tco2e_yearly=("emissions_tco2e", "sum")
    ).reset_index()
    agg = agg.rename(columns={"facility_code": "duid"})

    # 5. Load Assignment 1 data
    if not os.path.exists(ASS1_CSV):
        print(f"Missing {ASS1_CSV} — run: python preprocess_ass1.py")
        return
    ass1 = pd.read_csv(ASS1_CSV)
    ass1["duid"] = ass1["duid"].str.upper()

    # 6. Pull metadata (name, region, fuel_type) from facilities_nem.csv
    meta_path = "outputs/facilities_nem.csv"
    if os.path.exists(meta_path):
        meta = pd.read_csv(meta_path)[["duid", "name", "region", "fuel_type"]]
        agg = agg.merge(meta, on="duid", how="left")

    # 7. Ensure same schema and append
    cols = ["duid", "name", "region", "fuel_type", "year", "power_mwh_yearly", "emissions_tco2e_yearly"]
    ass1 = ass1[[c for c in cols if c in ass1.columns]]
    agg = agg[[c for c in cols if c in agg.columns]]

    integrated = pd.concat([ass1, agg], ignore_index=True)
    integrated = integrated.drop_duplicates(subset=["duid", "year"])

    # 8. Save
    integrated.to_csv(INTEGRATED_CSV, index=False)
    print(f"Integrated data saved: {INTEGRATED_CSV}")
    print(f"   • Assignment 1 rows: {len(ass1)}")
    print(f"   • Live (this interval): {len(agg)}")
    print(f"   • Total rows: {len(integrated)}")
    print("="*60)

# Hook into Ctrl+C
def signal_handler(sig, frame):
    print("\n\nDashboard stopped. Running Assignment 1 integration...")
    integrate_with_assignment1_on_shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    app.run(debug=True)
