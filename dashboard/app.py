import os, warnings
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, html, dcc, Input, Output
from influxdb_client import InfluxDBClient
from influxdb_client.client.warnings import MissingPivotFunction
warnings.simplefilter("ignore", MissingPivotFunction)
warnings.filterwarnings("ignore", category=FutureWarning)

INFLUX_URL    = os.getenv("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "")
DASH_PORT     = int(os.getenv("DASH_PORT", "8050"))

client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()

def query_latest(minutes=10):
    """Último valor de kills y damage por jugador."""
    q = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{minutes}m)
      |> filter(fn: (r) => r._measurement == "gaming_events")
      |> filter(fn: (r) => r._field == "kills" or r._field == "damage_dealt" or r._field == "kd_ratio")
      |> last()
      |> pivot(rowKey:["source_id"], columnKey: ["_field"], valueColumn: "_value")
    '''
    try:
        tables = query_api.query(q)
        rows = []
        seen = set()
        for table in tables:
            for r in table.records:
                sid = r.values.get("source_id", "")
                if sid in seen:
                    continue
                seen.add(sid)
                rows.append({
                    "source_id":    sid,
                    "level":        r.values.get("level", "normal"),
                    "kills":        r.values.get("kills", 0) or 0,
                    "damage_dealt": r.values.get("damage_dealt", 0) or 0,
                    "kd_ratio":     r.values.get("kd_ratio", 0) or 0,
                })
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        print(f"[DASH] query_latest error: {e}")
        return pd.DataFrame()

def query_timeseries(source_id, minutes=15):
    """Serie de tiempo de damage_dealt para un jugador."""
    q = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{minutes}m)
      |> filter(fn: (r) => r._measurement == "gaming_events")
      |> filter(fn: (r) => r._field == "damage_dealt")
      |> filter(fn: (r) => r["source_id"] == "{source_id}")
      |> aggregateWindow(every: 30s, fn: mean, createEmpty: false)
    '''
    try:
        df = query_api.query_data_frame(q)
        if df.empty:
            return pd.DataFrame()
        df = df.groupby("_time", as_index=False)["_value"].mean()
        return df.rename(columns={"_time": "time", "_value": "damage_dealt"})
    except Exception as e:
        print(f"[DASH] query_timeseries error: {e}")
        return pd.DataFrame()

# ── Layout ────────────────────────────────────────────────────
app = Dash(__name__, title="Gaming Pipeline Dashboard")

app.layout = html.Div(style={"backgroundColor": "#0f0f0f", "minHeight": "100vh", "padding": "24px"}, children=[

    html.H1("🎮 Gaming Events — Live Dashboard",
            style={"color": "#C9A84C", "fontFamily": "Arial",
                   "fontSize": "22px", "fontWeight": "600", "marginBottom": "24px"}),

    # ── Gráfica 1: Bar chart — Kills por jugador ──────────────
    html.Div([
        html.H3("Kills por jugador (últimos 10 min)",
                style={"color": "#F5F4F0", "fontSize": "14px",
                       "fontFamily": "Arial", "marginBottom": "8px"}),
        dcc.Graph(id="chart-kills", style={"height": "280px"}),
    ], style={"marginBottom": "24px"}),

    # ── Gráfica 2: Bubble chart — Daño vs KD Ratio ───────────
    html.Div([
        html.H3("Daño causado vs K/D Ratio por jugador",
                style={"color": "#F5F4F0", "fontSize": "14px",
                       "fontFamily": "Arial", "marginBottom": "8px"}),
        dcc.Graph(id="chart-bubble", style={"height": "320px"}),
    ], style={"marginBottom": "24px"}),

    # ── Gráfica 3: Line chart — Serie de tiempo ───────────────
    html.Div([
        html.H3("Daño en el tiempo — jugador seleccionado",
                style={"color": "#F5F4F0", "fontSize": "14px",
                       "fontFamily": "Arial", "marginBottom": "8px"}),
        dcc.Dropdown(id="source-dropdown",
                     placeholder="Selecciona un jugador...",
                     style={"marginBottom": "8px", "width": "300px"}),
        dcc.Graph(id="chart-timeseries", style={"height": "240px"}),
    ], style={"marginBottom": "24px"}),

    dcc.Interval(id="tick", interval=5_000, n_intervals=0),
])

# ── Callbacks ─────────────────────────────────────────────────
@app.callback(
    Output("chart-kills",   "figure"),
    Output("chart-bubble",  "figure"),
    Output("source-dropdown", "options"),
    Input("tick", "n_intervals"),
)
def update_charts(n):
    df = query_latest()
    if df.empty:
        empty = go.Figure()
        empty.update_layout(
            paper_bgcolor="#1a1a1a", plot_bgcolor="#1a1a1a",
            font_color="#888", title_text="Esperando datos..."
        )
        return empty, empty, []

    # Chart 1 — Bar chart: kills por jugador
    fig1 = px.bar(
        df, x="source_id", y="kills",
        color="level",
        color_discrete_map={"rampage": "#e74c3c", "agresivo": "#f39c12", "normal": "#2ecc71"},
        title="Kills por jugador",
        labels={"source_id": "Jugador", "kills": "Kills"},
    )
    fig1.update_layout(
        paper_bgcolor="#1a1a1a", plot_bgcolor="#1a1a1a",
        font_color="#F5F4F0", margin={"t": 40, "b": 20},
        uirevision="chart1"
    )

    # Chart 2 — Bubble chart: damage_dealt vs kd_ratio
    fig2 = px.scatter(
        df, x="kd_ratio", y="damage_dealt",
        size="kills", color="level",
        color_discrete_map={"rampage": "#e74c3c", "agresivo": "#f39c12", "normal": "#2ecc71"},
        hover_name="source_id",
        title="Daño vs K/D Ratio",
        labels={"kd_ratio": "K/D Ratio", "damage_dealt": "Daño causado", "kills": "Kills"},
        size_max=40,
    )
    fig2.update_layout(
        paper_bgcolor="#1a1a1a", plot_bgcolor="#1a1a1a",
        font_color="#F5F4F0", margin={"t": 40, "b": 20},
        uirevision="chart2"
    )

    options = [{"label": r["source_id"], "value": r["source_id"]}
               for _, r in df.iterrows()]
    return fig1, fig2, options

@app.callback(
    Output("chart-timeseries", "figure"),
    Input("source-dropdown", "value"),
    Input("tick", "n_intervals"),
)
def update_timeseries(source_id, n):
    if not source_id:
        empty = go.Figure()
        empty.update_layout(
            paper_bgcolor="#1a1a1a", plot_bgcolor="#1a1a1a",
            font_color="#888", title_text="Selecciona un jugador..."
        )
        return empty
    df = query_timeseries(source_id)
    if df.empty:
        return go.Figure()
    fig = px.line(
        df, x="time", y="damage_dealt",
        title=f"Daño en el tiempo — {source_id}",
        labels={"time": "Tiempo", "damage_dealt": "Daño causado"},
    )
    fig.update_layout(
        paper_bgcolor="#1a1a1a", plot_bgcolor="#1a1a1a",
        font_color="#F5F4F0", margin={"r": 50, "t": 40, "b": 40},
        uirevision=source_id
    )
    return fig

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=DASH_PORT, debug=False)