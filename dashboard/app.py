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

# ── Paleta de colores ─────────────────────────────────────────
BG_PAGE   = "#0b0c10"
BG_CARD   = "#1a1a2e"
BG_CHART  = "#16213e"
GOLD      = "#C9A84C"
ACCENT    = "#e94560"
GREEN     = "#0f9b58"
ORANGE    = "#f39c12"
RED       = "#e74c3c"
TEXT      = "#eaeaea"
TEXT_DIM  = "#888da8"
BORDER    = "1px solid rgba(201,168,76,0.2)"

COLOR_MAP = {"rampage": RED, "agresivo": ORANGE, "normal": GREEN}

CHART_LAYOUT = dict(
    paper_bgcolor=BG_CHART,
    plot_bgcolor=BG_CHART,
    font=dict(color=TEXT, family="Arial"),
    margin=dict(t=45, b=30, l=50, r=20),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="rgba(255,255,255,0.1)", borderwidth=1),
    xaxis=dict(gridcolor="rgba(255,255,255,0.05)", zerolinecolor="rgba(255,255,255,0.1)"),
    yaxis=dict(gridcolor="rgba(255,255,255,0.05)", zerolinecolor="rgba(255,255,255,0.1)"),
)

def query_latest(minutes=10):
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

# ── Helpers de UI ─────────────────────────────────────────────
def stat_card(label, value, color=GOLD):
    return html.Div(style={
        "background": BG_CARD,
        "border": BORDER,
        "borderTop": f"3px solid {color}",
        "borderRadius": "6px",
        "padding": "16px 20px",
        "flex": "1",
        "minWidth": "140px",
        "textAlign": "center",
    }, children=[
        html.Div(value, style={"fontSize": "28px", "fontWeight": "700",
                               "color": color, "fontFamily": "Arial"}),
        html.Div(label, style={"fontSize": "11px", "color": TEXT_DIM,
                               "letterSpacing": "0.1em", "textTransform": "uppercase",
                               "marginTop": "4px"}),
    ])

def section_title(text):
    return html.Div(text, style={
        "color": GOLD,
        "fontSize": "11px",
        "fontFamily": "Arial",
        "letterSpacing": "0.2em",
        "textTransform": "uppercase",
        "marginBottom": "10px",
        "borderLeft": f"3px solid {GOLD}",
        "paddingLeft": "10px",
    })

# ── Layout ────────────────────────────────────────────────────
app = Dash(__name__, title="🎮 Gaming Pipeline")

app.layout = html.Div(style={"backgroundColor": BG_PAGE, "minHeight": "100vh",
                              "fontFamily": "Arial", "color": TEXT}, children=[

    # ── Header ───────────────────────────────────────────────
    html.Div(style={
        "background": "linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%)",
        "borderBottom": f"1px solid {GOLD}",
        "padding": "28px 40px",
        "display": "flex",
        "alignItems": "center",
        "justifyContent": "space-between",
    }, children=[
        html.Div([
            html.Div("VISUALIZATION TOOLS · UPY · 2026", style={
                "fontSize": "10px", "color": GOLD, "letterSpacing": "0.25em",
                "marginBottom": "6px",
            }),
            html.Div([
                html.Span("🎮 ", style={"fontSize": "26px"}),
                html.Span("Gaming Events", style={
                    "fontSize": "26px", "fontWeight": "700", "color": TEXT,
                }),
                html.Span("  —  Live Pipeline Dashboard", style={
                    "fontSize": "16px", "color": TEXT_DIM, "fontWeight": "300",
                }),
            ]),
        ]),
        html.Div(style={"textAlign": "right"}, children=[
            html.Div("LIVE", style={
                "background": RED, "color": "white", "fontSize": "10px",
                "fontWeight": "700", "letterSpacing": "0.15em",
                "padding": "4px 10px", "borderRadius": "2px", "display": "inline-block",
                "marginBottom": "6px",
            }),
            html.Div("Actualización cada 5s", style={"fontSize": "11px", "color": TEXT_DIM}),
        ]),
    ]),

    # ── Body ─────────────────────────────────────────────────
    html.Div(style={"padding": "32px 40px"}, children=[

        # ── Stat cards ───────────────────────────────────────
        html.Div(id="stat-cards", style={
            "display": "flex", "gap": "16px", "marginBottom": "36px", "flexWrap": "wrap",
        }),

        # ── Fila 1: Bar + Bubble ─────────────────────────────
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                        "gap": "20px", "marginBottom": "20px"}, children=[
            html.Div(style={"background": BG_CARD, "border": BORDER,
                            "borderRadius": "6px", "padding": "20px"}, children=[
                section_title("Kills por jugador"),
                dcc.Graph(id="chart-kills", style={"height": "280px"},
                          config={"displayModeBar": False}),
            ]),
            html.Div(style={"background": BG_CARD, "border": BORDER,
                            "borderRadius": "6px", "padding": "20px"}, children=[
                section_title("Daño vs K/D Ratio"),
                dcc.Graph(id="chart-bubble", style={"height": "280px"},
                          config={"displayModeBar": False}),
            ]),
        ]),

        # ── Fila 2: Line chart ───────────────────────────────
        html.Div(style={"background": BG_CARD, "border": BORDER,
                        "borderRadius": "6px", "padding": "20px"}, children=[
            section_title("Daño en el tiempo — jugador seleccionado"),
            dcc.Dropdown(
                id="source-dropdown",
                placeholder="Selecciona un jugador...",
                style={"width": "260px", "marginBottom": "12px",
                       "backgroundColor": BG_CHART, "color": TEXT,
                       "border": "1px solid rgba(201,168,76,0.3)"},
            ),
            dcc.Graph(id="chart-timeseries", style={"height": "240px"},
                      config={"displayModeBar": False}),
        ]),

        # ── Footer ───────────────────────────────────────────
        html.Div("Real-Time Data Pipeline · Docker · Kafka · InfluxDB · Plotly Dash",
                 style={"textAlign": "center", "color": TEXT_DIM, "fontSize": "11px",
                        "letterSpacing": "0.1em", "marginTop": "36px",
                        "borderTop": "1px solid rgba(255,255,255,0.05)",
                        "paddingTop": "20px"}),
    ]),

    dcc.Interval(id="tick", interval=5_000, n_intervals=0),
])

# ── Callbacks ─────────────────────────────────────────────────
@app.callback(
    Output("stat-cards",    "children"),
    Output("chart-kills",   "figure"),
    Output("chart-bubble",  "figure"),
    Output("source-dropdown", "options"),
    Input("tick", "n_intervals"),
)
def update_charts(n):
    df = query_latest()

    empty = go.Figure()
    empty.update_layout(**CHART_LAYOUT, title_text="Esperando datos...")

    if df.empty:
        cards = [
            stat_card("Jugadores activos", "—"),
            stat_card("Kills totales",     "—"),
            stat_card("Daño promedio",     "—"),
            stat_card("Alertas activas",   "—", RED),
        ]
        return cards, empty, empty, []

    # ── Stat cards ──────────────────────────────────────────
    top_player  = df.loc[df["kills"].idxmax(), "source_id"] if not df.empty else "—"
    total_kills = int(df["kills"].sum())
    avg_damage  = int(df["damage_dealt"].mean())
    alerts      = int((df["level"] == "rampage").sum())

    cards = [
        stat_card("Jugadores activos", len(df), GOLD),
        stat_card("Kills totales",     total_kills, GREEN),
        stat_card("Daño promedio",     avg_damage, ORANGE),
        stat_card("Alertas rampage",   alerts, RED),
    ]

    # ── Chart 1: Bar ────────────────────────────────────────
    fig1 = px.bar(
        df, x="source_id", y="kills",
        color="level", color_discrete_map=COLOR_MAP,
        labels={"source_id": "Jugador", "kills": "Kills"},
        title=f"Top killer: {top_player}",
    )
    fig1.update_layout(**CHART_LAYOUT, uirevision="chart1")
    fig1.update_traces(marker_line_width=0)

    # ── Chart 2: Bubble ─────────────────────────────────────
    fig2 = px.scatter(
        df, x="kd_ratio", y="damage_dealt",
        size="kills", color="level",
        color_discrete_map=COLOR_MAP,
        hover_name="source_id",
        labels={"kd_ratio": "K/D Ratio", "damage_dealt": "Daño causado", "kills": "Kills"},
        size_max=45,
        title="Tamaño = kills acumulados",
    )
    fig2.update_layout(**CHART_LAYOUT, uirevision="chart2")

    options = [{"label": r["source_id"], "value": r["source_id"]}
               for _, r in df.iterrows()]
    return cards, fig1, fig2, options

@app.callback(
    Output("chart-timeseries", "figure"),
    Input("source-dropdown", "value"),
    Input("tick", "n_intervals"),
)
def update_timeseries(source_id, n):
    empty = go.Figure()
    empty.update_layout(**CHART_LAYOUT, title_text="Selecciona un jugador...")
    if not source_id:
        return empty
    df = query_timeseries(source_id)
    if df.empty:
        return empty
    fig = px.line(
        df, x="time", y="damage_dealt",
        labels={"time": "Tiempo", "damage_dealt": "Daño causado"},
        title=f"Serie de tiempo — {source_id}",
        color_discrete_sequence=[ACCENT],
    )
    fig.update_traces(line=dict(width=2.5))
    fig.update_layout(**CHART_LAYOUT, uirevision=source_id)
    return fig

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=DASH_PORT, debug=False)