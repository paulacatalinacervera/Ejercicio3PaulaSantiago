from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st
from cassandra.cluster import Cluster
from neo4j import GraphDatabase

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import (
    CASSANDRA_HOST,
    CASSANDRA_PORT,
    DATA_DIR,
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USER,
    PROCESSED_DIR,
    RESULTS_DIR,
    SIM_DAY,
)
from src.warehouse.queries import QUERIES

st.set_page_config(
    page_title="Centro de control de movilidad NYC",
    page_icon=":material/directions_car:",
    layout="wide",
)


def simulation_day() -> str:
    if SIM_DAY:
        return SIM_DAY
    metadata = DATA_DIR / "synthetic" / "generator_metadata.json"
    if metadata.exists():
        try:
            start = json.loads(metadata.read_text(encoding="utf-8"))["start_date"]
            return start.replace("-", "")
        except Exception:
            pass
    return datetime.utcnow().strftime("%Y%m%d")


@st.cache_resource
def cassandra_session():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    return cluster.connect()


@st.cache_resource
def neo4j_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


@st.cache_data(ttl=5)
def load_zone_dimension() -> pd.DataFrame:
    zones = pd.read_csv(DATA_DIR / "taxi_zone_lookup.csv")
    zones.columns = [c.lower() for c in zones.columns]
    zones = zones.rename(columns={"locationid": "zone_id"})
    if "location_id" in zones.columns:
        zones = zones.rename(columns={"location_id": "zone_id"})
    centroids = DATA_DIR / "zone_centroids.csv"
    if centroids.exists():
        coords = pd.read_csv(centroids)
        coords.columns = [c.lower() for c in coords.columns]
        zones = zones.merge(
            coords[["location_id", "lat", "lon"]],
            left_on="zone_id",
            right_on="location_id",
            how="left",
        )
    return zones[["zone_id", "zone", "borough", "lat", "lon"]]


@st.cache_data(ttl=5)
def load_congestion(day: str) -> pd.DataFrame:
    session = cassandra_session()
    rows = session.execute(
        "SELECT day_bucket, window_start, zone_id, readings, avg_speed_kmh, congestion_index "
        "FROM mobility.city_congestion_by_window"
    )
    data = [dict(row._asdict()) for row in rows]
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df = df[df["day_bucket"] == day].copy()
    if df.empty:
        return df
    df["window_start"] = pd.to_datetime(df["window_start"])
    df["congestion_index"] = pd.to_numeric(df["congestion_index"], errors="coerce")
    df["avg_speed_kmh"] = pd.to_numeric(df["avg_speed_kmh"], errors="coerce")
    return df.sort_values("window_start").groupby("zone_id", as_index=False).tail(1)


@st.cache_data(ttl="1h")
def load_od_links() -> pd.DataFrame:
    path = PROCESSED_DIR / "od_links.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if "path" in df.columns:
        df["path"] = [list(item) for item in df["path"]]
    return df


@st.cache_data
def load_metadata() -> dict:
    sources = {
        "generator": DATA_DIR / "synthetic" / "generator_metadata.json",
        "batch": RESULTS_DIR / "batch_summary.json",
        "bigquery": RESULTS_DIR / "bigquery_summary.json",
    }
    result: dict[str, dict] = {}
    for name, path in sources.items():
        if path.exists():
            try:
                result[name] = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                result[name] = {}
        else:
            result[name] = {}
    return result


def neo4j_query(query: str, **params) -> list[dict]:
    with neo4j_driver().session() as session:
        return session.run(query, **params).data()


@st.cache_data(ttl=30)
def graph_rankings() -> tuple[pd.DataFrame, pd.DataFrame]:
    pagerank = neo4j_query(
        "MATCH (z:Zone) WHERE z.pagerank IS NOT NULL "
        "RETURN z.location_id AS zone_id, z.zone AS zone, z.borough AS borough, z.pagerank AS pagerank "
        "ORDER BY pagerank DESC LIMIT 15"
    )
    betweenness = neo4j_query(
        "MATCH (z:Zone) WHERE z.betweenness IS NOT NULL "
        "RETURN z.location_id AS zone_id, z.zone AS zone, z.borough AS borough, z.betweenness AS betweenness "
        "ORDER BY betweenness DESC LIMIT 15"
    )
    return pd.DataFrame(pagerank), pd.DataFrame(betweenness)


@st.cache_data(ttl=30)
def graph_node_metrics() -> pd.DataFrame:
    rows = neo4j_query(
        "MATCH (z:Zone) "
        "RETURN z.location_id AS zone_id, z.zone AS zone_name, z.borough AS graph_borough, "
        "z.pagerank AS pagerank, z.betweenness AS betweenness "
        "ORDER BY z.location_id"
    )
    return pd.DataFrame(rows)


def shortest_path(origin: int, destination: int) -> dict:
    try:
        rows = neo4j_query(
            """
            MATCH (s:Zone {location_id: $origin}), (t:Zone {location_id: $destination})
            CALL gds.shortestPath.dijkstra.stream('zoneGraph', {
              sourceNode: id(s), targetNode: id(t),
              relationshipWeightProperty: 'avg_duration_min'
            })
            YIELD path, totalCost
            RETURN [n IN nodes(path) | n.zone] AS route,
                   [n IN nodes(path) | n.location_id] AS ids, totalCost
            """,
            origin=origin,
            destination=destination,
        )
        return rows[0] if rows else {}
    except Exception:
        rows = neo4j_query(
            """
            MATCH (s:Zone {location_id: $origin}), (t:Zone {location_id: $destination})
            MATCH p = shortestPath((s)-[:TRIP*]-(t))
            RETURN [n IN nodes(p) | n.zone] AS route,
                   [n IN nodes(p) | n.location_id] AS ids, length(p) AS hops
            LIMIT 1
            """,
            origin=origin,
            destination=destination,
        )
        return rows[0] if rows else {}


@st.cache_resource
def duckdb_connection():
    from src.warehouse.local_duckdb import get_connection

    return get_connection(PROCESSED_DIR)


def congestion_color(index: float) -> list[int]:
    if pd.isna(index):
        return [120, 120, 120, 160]
    red = int(255 * min(max(index, 0.0), 1.0))
    green = int(255 * (1.0 - min(max(index, 0.0), 1.0)))
    return [red, green, 40, 190]


def duration_color(value: float, max_value: float) -> list[int]:
    if not max_value or pd.isna(value) or pd.isna(max_value):
        return [56, 189, 248, 140]
    t = min(max(float(value) / float(max_value), 0.0), 1.0)
    return [int(56 + 170 * t), int(189 - 115 * t), int(248 - 150 * t), 150]


def build_node_table(
    zones: pd.DataFrame,
    latest: pd.DataFrame,
    metrics: pd.DataFrame,
    links: pd.DataFrame,
) -> pd.DataFrame:
    if links.empty:
        outgoing = pd.DataFrame(columns=["origin_zone", "out_trips", "out_distance_km", "out_duration_min"])
        incoming = pd.DataFrame(columns=["destination_zone", "in_trips", "in_distance_km", "in_duration_min"])
    else:
        outgoing = (
            links.groupby("origin_zone", as_index=False)
            .agg(
                out_trips=("trips", "sum"),
                out_distance_km=("avg_distance_km", "mean"),
                out_duration_min=("avg_duration_min", "mean"),
            )
            .rename(columns={"origin_zone": "zone_id"})
        )
        incoming = (
            links.groupby("destination_zone", as_index=False)
            .agg(
                in_trips=("trips", "sum"),
                in_distance_km=("avg_distance_km", "mean"),
                in_duration_min=("avg_duration_min", "mean"),
            )
            .rename(columns={"destination_zone": "zone_id"})
        )

    if latest.empty:
        live = pd.DataFrame(columns=["zone_id", "live_window", "live_readings", "live_speed", "live_congestion"])
    else:
        live = latest[["zone_id", "window_start", "readings", "avg_speed_kmh", "congestion_index"]].rename(
            columns={
                "window_start": "live_window",
                "readings": "live_readings",
                "avg_speed_kmh": "live_speed",
                "congestion_index": "live_congestion",
            }
        )

    if metrics.empty:
        metrics = pd.DataFrame(columns=["zone_id", "pagerank", "betweenness"])
    else:
        metrics = metrics[["zone_id", "pagerank", "betweenness"]]

    df = zones.merge(outgoing, on="zone_id", how="left")
    df = df.merge(incoming, on="zone_id", how="left")
    df = df.merge(live, on="zone_id", how="left")
    df = df.merge(metrics, on="zone_id", how="left")

    numeric_cols = [
        "out_trips",
        "in_trips",
        "out_distance_km",
        "in_distance_km",
        "out_duration_min",
        "in_duration_min",
        "live_readings",
        "live_speed",
        "live_congestion",
        "pagerank",
        "betweenness",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["importance"] = df["out_trips"] + df["in_trips"]
    df["node_radius"] = (df["importance"] / 100.0).clip(lower=250.0, upper=2600.0)
    max_rank = df["pagerank"].max() if not df.empty else 0.0
    fallback = df["pagerank"] / max_rank if max_rank else df["pagerank"] * 0.0
    df["map_score"] = df["live_congestion"].where(df["live_congestion"] > 0, fallback)
    df["color"] = df["map_score"].apply(congestion_color)
    df["label"] = df["zone"].astype(str)
    return df.sort_values("importance", ascending=False).reset_index(drop=True)


def render_summary(meta: dict) -> None:
    generator = meta.get("generator", {})
    batch = meta.get("batch", {})
    bigquery = meta.get("bigquery", {})
    tables = bigquery.get("tables", {})

    st.subheader("Resumen del dataset", icon=":material/dashboard:")
    with st.grid(4):
        st.metric("Viajes", f"{int(batch.get('trips', 0)):,}", border=True, height="stretch")
        st.metric("Lecturas GPS", f"{int(batch.get('gps_readings', 0)):,}", border=True, height="stretch")
        st.metric("Zonas", f"{int(batch.get('zones', 0)):,}", border=True, height="stretch")
        st.metric("Aristas OD", f"{int(batch.get('od_edges', 0)):,}", border=True, height="stretch")
        st.metric("Corredores mapeados", f"{int(batch.get('od_links', 0)):,}", border=True, height="stretch")
        st.metric("Filas horarias", f"{int(batch.get('hourly_zone_stats', 0)):,}", border=True, height="stretch")
        st.metric("Flota de taxis", f"{int(generator.get('fleet_size', 0)):,}", border=True, height="stretch")
        st.metric("Semilla", f"{generator.get('seed', '-')}", border=True, height="stretch")

    with st.container(horizontal=True):
        st.badge("AWS S3 real", icon=":material/check:", color="green")
        st.badge("GCS + BigQuery", icon=":material/check:", color="blue")
        st.badge("DuckDB fallback", icon=":material/check:", color="blue")
        if batch.get("volume_ok") or generator.get("volume_ok"):
            st.badge("Volumetría OK", icon=":material/check_circle:", color="green")

    st.space("medium")
    left, right = st.columns([1, 1])
    with left:
        with st.container(border=True):
            st.markdown("**Distribución de viajes por borough**")
            counts = generator.get("borough_trip_counts", {})
            borough_df = pd.DataFrame(
                list(counts.items()), columns=["borough", "trips"]
            ).sort_values("trips", ascending=False)
            if borough_df.empty:
                st.caption("Sin metadatos de borough.")
            else:
                st.echarts_chart(
                    {
                        "tooltip": {"trigger": "axis"},
                        "dataset": {"source": borough_df},
                        "xAxis": {"type": "category"},
                        "yAxis": {"type": "value"},
                        "series": [{"type": "bar", "encode": {"x": "borough", "y": "trips"}}],
                    },
                    alt="Viajes por borough, Manhattan lidera la demanda",
                )
    with right:
        with st.container(border=True):
            st.markdown("**Ficha técnica**")
            st.table(
                {
                    ":material/calendar_today: Inicio": str(batch.get("start_date", "-")),
                    ":material/calendar_today: Fin": str(batch.get("end_date", "-")),
                    ":material/route: Tablas BigQuery": str(len(tables)),
                    ":material/query_stats: Consultas BigQuery": str(bigquery.get("queries", "-")),
                    ":material/database: Fuente cruda": "S3 `pausanti-ejercicio3`",
                    ":material/cloud: staging": "GCS `proyecto-data-2026-mobility`",
                    ":material/analytics: Analítica": "BigQuery `pausan-proyecto.mobility`",
                },
                border="horizontal",
                width="content",
                alt="Ficha técnica del dataset de movilidad",
            )


@st.fragment(run_every="5s")
def render_live(day: str) -> None:
    if not st.session_state.get("auto_refresh", True):
        st.caption("Auto-refresh en pausa. Actívalo en la barra lateral.")
        return
    st.subheader("Congestión en vivo", icon=":material/monitoring:")
    try:
        latest = load_congestion(day)
        if latest.empty:
            st.caption("No hay ventanas para ese día. Lanza el streaming y el productor o revisa `SIM_DAY`.")
            return
        zones = load_zone_dimension()
        merged = latest.merge(zones, on="zone_id", how="left")
        merged["congestion_pct"] = (merged["congestion_index"] * 100).round(1)
        window = pd.to_datetime(merged["window_start"]).max()
        st.caption(f"Última ventana: {window:%Y-%m-%d %H:%M:%S UTC}")

        with st.grid(4):
            st.metric("Zonas activas", f"{merged['zone_id'].nunique():,}", border=True, height="stretch")
            st.metric("Congestión media", f"{merged['congestion_pct'].mean():.1f}%", border=True, height="stretch")
            st.metric("Velocidad media", f"{merged['avg_speed_kmh'].mean():.1f} km/h", border=True, height="stretch")
            st.metric("Lecturas última ventana", f"{int(merged['readings'].sum()):,}", border=True, height="stretch")

        map_df = merged.dropna(subset=["lat", "lon"]).copy()
        map_df["color"] = map_df["congestion_index"].apply(congestion_color)
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position="[lon, lat]",
            get_fill_color="color",
            get_radius="500 + readings * 2",
            pickable=True,
            stroked=True,
            get_line_color=[226, 232, 240, 120],
            get_line_width=1,
        )
        st.pydeck_chart(
            pdk.Deck(
                map_style="dark",
                layers=[layer],
                initial_view_state=pdk.ViewState(latitude=40.75, longitude=-73.97, zoom=9.6, pitch=25),
                tooltip={
                    "html": "<b>{zone}</b><br/>{borough}<br/>Congestión: {congestion_pct}%"
                },
            ),
            alt="Mapa de congestión en vivo por zona de taxi",
        )

        top = merged.sort_values("congestion_index", ascending=False).head(15)
        st.echarts_chart(
            {
                "tooltip": {"trigger": "axis"},
                "dataset": {"source": top},
                "xAxis": {"type": "category"},
                "yAxis": {"type": "value"},
                "series": [{"type": "bar", "encode": {"x": "zone", "y": "congestion_pct"}}],
            },
            alt="Top 15 de zonas más congestionadas en la última ventana",
        )
    except Exception as exc:
        st.error(f"No se pudo consultar Cassandra: {exc}")
        st.caption("Verifica que Cassandra esté arriba y que el esquema se haya aplicado.")


def render_network(day: str) -> None:
    st.subheader("Mapa de red de taxis", icon=":material/map:")
    st.caption(
        "Cada círculo es una zona de taxi; el tamaño representa el volumen de viajes. "
        "Las líneas son corredores origen-destino: grosor = viajes, color = duración media. "
        "El trazado sigue una aproximación tipo cuadrícula (avenida + calle) sobre los centroides, "
        "no ruteo calle por calle."
    )
    try:
        links = load_od_links()
        if links.empty:
            st.caption("Ejecuta `python -m src.batch` para generar `od_links.parquet`.")
            return
        zones = load_zone_dimension()
        latest = load_congestion(day)
        metrics = graph_node_metrics()
        nodes = build_node_table(zones, latest, metrics, links)

        borough_options = sorted(
            set(links["origin_borough"].dropna().astype(str)) | set(links["destination_borough"].dropna().astype(str))
        )
        selected_boroughs = st.multiselect(
            "Boroughs",
            options=borough_options,
            default=borough_options,
            key="network_boroughs",
        )
        top_n = st.slider("Corredores visibles", min_value=25, max_value=500, value=180, step=25)

        display_cols = [
            "zone_id",
            "zone",
            "borough",
            "lat",
            "lon",
            "out_trips",
            "in_trips",
            "out_distance_km",
            "in_distance_km",
            "out_duration_min",
            "in_duration_min",
            "live_congestion",
            "pagerank",
            "betweenness",
        ]
        selection = st.dataframe(
            nodes[display_cols],
            hide_index=True,
            alt="Tabla numérica de nodos, volúmenes, distancias y centralidad",
            column_config={
                "zone_id": st.column_config.NumberColumn("ID", format="%.0f"),
                "zone": st.column_config.TextColumn("Zona"),
                "borough": st.column_config.TextColumn("Borough"),
                "lat": st.column_config.NumberColumn("Latitud", format="%.4f"),
                "lon": st.column_config.NumberColumn("Longitud", format="%.4f"),
                "out_trips": st.column_config.NumberColumn("Viajes salida", format="%.0f"),
                "in_trips": st.column_config.NumberColumn("Viajes llegada", format="%.0f"),
                "out_distance_km": st.column_config.NumberColumn("Dist. salida km", format="%.2f"),
                "in_distance_km": st.column_config.NumberColumn("Dist. llegada km", format="%.2f"),
                "out_duration_min": st.column_config.NumberColumn("Dur. salida min", format="%.2f"),
                "in_duration_min": st.column_config.NumberColumn("Dur. llegada min", format="%.2f"),
                "live_congestion": st.column_config.ProgressColumn(
                    "Congestión", min_value=0.0, max_value=1.0
                ),
                "pagerank": st.column_config.NumberColumn("PageRank", format="%.4f"),
                "betweenness": st.column_config.NumberColumn("Intermediación", format="%.4f"),
            },
            on_select="rerun",
            selection_mode="single-row",
            key="node_table",
        )

        selected_zone = None
        if selection.selection.rows:
            selected_zone = int(nodes.iloc[selection.selection.rows[0]]["zone_id"])
            st.caption(f"Nodo seleccionado: **{nodes.iloc[selection.selection.rows[0]]['zone']}** ({selected_zone})")

        mask = links["origin_borough"].astype(str).isin(selected_boroughs) & links[
            "destination_borough"
        ].astype(str).isin(selected_boroughs)
        if selected_zone is not None:
            mask &= (links["origin_zone"] == selected_zone) | (links["destination_zone"] == selected_zone)
        view = links[mask].nlargest(top_n, "trips").copy()
        if view.empty:
            st.caption("No hay corredores para los filtros actuales.")
            return

        max_trips = view["trips"].max()
        max_duration = view["avg_duration_min"].max()
        view["path_width"] = (2.0 + (view["trips"] / max_trips) * 9.0).clip(upper=14.0)
        view["path_color"] = view["avg_duration_min"].apply(
            lambda value: duration_color(value, max_duration)
        )

        path_layer = pdk.Layer(
            "PathLayer",
            data=view,
            get_path="path",
            get_color="path_color",
            get_width="path_width",
            width_units="meters",
            width_min_pixels=1,
            pickable=True,
        )
        node_view = nodes[nodes["zone_id"].isin(set(view["origin_zone"]) | set(view["destination_zone"]))]
        node_layer = pdk.Layer(
            "ScatterplotLayer",
            data=node_view,
            get_position="[lon, lat]",
            get_fill_color="color",
            get_radius="node_radius",
            radius_units="meters",
            pickable=True,
            stroked=True,
            get_line_color=[226, 232, 240, 160],
            get_line_width=1,
        )
        label_view = node_view.nlargest(12, "importance")[["lon", "lat", "label"]]
        text_layer = pdk.Layer(
            "TextLayer",
            data=label_view,
            get_position="[lon, lat]",
            get_text="label",
            get_size=12,
            get_color=[226, 232, 240, 240],
            size_units="pixels",
            get_background_color=[15, 23, 42, 210],
            background=True,
        )
        deck = pdk.Deck(
            map_style="dark",
            layers=[path_layer, node_layer, text_layer],
            initial_view_state=pdk.ViewState(latitude=40.75, longitude=-73.97, zoom=9.8, pitch=30),
            tooltip={
                "html": (
                    "<b>{origin_zone_name} → {destination_zone_name}</b><br/>"
                    "Viajes: {trips}<br/>Duración media: {avg_duration_min} min<br/>"
                    "Distancia media: {avg_distance_km} km<br/>Dist. cuadrícula: {grid_km} km"
                )
            },
        )
        st.pydeck_chart(deck, alt="Mapa NYC con nodos de taxi y corredores origen-destino")
    except Exception as exc:
        st.error(f"No se pudo construir el mapa de red: {exc}")


def render_graph() -> None:
    st.subheader("Centralidad y rutas", icon=":material/hub:")
    try:
        pagerank, betweenness = graph_rankings()
        left, right = st.columns(2)
        with left:
            with st.container(border=True):
                st.markdown("**PageRank**")
                if pagerank.empty:
                    st.caption("Ejecuta `python -m src.graph.run_gds`.")
                else:
                    st.echarts_chart(
                        {
                            "tooltip": {"trigger": "axis"},
                            "dataset": {"source": pagerank},
                            "xAxis": {"type": "value"},
                            "yAxis": {"type": "category"},
                            "series": [{"type": "bar", "encode": {"x": "pagerank", "y": "zone"}}],
                        },
                        alt="Zonas con mayor PageRank en la red de viajes",
                    )
        with right:
            with st.container(border=True):
                st.markdown("**Intermediación**")
                if betweenness.empty:
                    st.caption("Ejecuta `python -m src.graph.run_gds`.")
                else:
                    st.echarts_chart(
                        {
                            "tooltip": {"trigger": "axis"},
                            "dataset": {"source": betweenness},
                            "xAxis": {"type": "value"},
                            "yAxis": {"type": "category"},
                            "series": [{"type": "bar", "encode": {"x": "betweenness", "y": "zone"}}],
                        },
                        alt="Zonas con mayor intermediación en la red de viajes",
                    )

        st.space("medium")
        with st.container(border=True):
            st.markdown("**Ruta más corta (Dijkstra ponderado por duración)**")
            c1, c2, c3 = st.columns([1, 1, 1], vertical_alignment="bottom")
            origin = c1.number_input("Zona origen", min_value=1, max_value=265, value=161)
            destination = c2.number_input("Zona destino", min_value=1, max_value=265, value=132)
            if c3.button("Calcular ruta", type="primary", width="stretch"):
                result = shortest_path(int(origin), int(destination))
                if result:
                    st.success(f"Costo total: {result.get('totalCost', result.get('hops'))}")
                    st.write(" → ".join(str(item) for item in result.get("route", [])))
                else:
                    st.warning("No se encontró ruta. Verifica que el grafo esté cargado.")
    except Exception as exc:
        st.error(f"No se pudo consultar Neo4j: {exc}")


def render_history() -> None:
    st.subheader("Analítica histórica", icon=":material/query_stats:")
    try:
        con = duckdb_connection()
        name = st.selectbox(
            "Consulta",
            options=list(QUERIES.keys()),
            format_func=lambda key: f"{key} - {QUERIES[key]['description']}",
        )
        df = con.execute(QUERIES[name]["sql"]).df()
        st.dataframe(df, hide_index=True, alt=f"Resultados de la consulta {name}")
        if name in {"peak_hours", "hourly_zone_trend"} and not df.empty:
            plot_df = df.copy()
            plot_df["hour"] = plot_df["hour"].astype(str)
            y = "trips" if "trips" in plot_df.columns else plot_df.columns[-1]
            st.echarts_chart(
                {
                    "tooltip": {"trigger": "axis"},
                    "dataset": {"source": plot_df},
                    "xAxis": {"type": "category"},
                    "yAxis": {"type": "value"},
                    "series": [{"type": "line", "showSymbol": True, "encode": {"x": "hour", "y": y}}],
                },
                alt=f"Evolución horaria de {name}",
            )
        elif name in {"critical_zones", "longest_routes", "top_od_routes"} and not df.empty:
            x_col = "zone_name" if "zone_name" in df.columns else df.columns[0]
            y_col = "trips" if "trips" in df.columns else df.columns[-1]
            plot_df = df.head(15).copy()
            plot_df[x_col] = plot_df[x_col].astype(str)
            st.echarts_chart(
                {
                    "tooltip": {"trigger": "axis"},
                    "dataset": {"source": plot_df},
                    "xAxis": {"type": "value"},
                    "yAxis": {"type": "category"},
                    "series": [{"type": "bar", "encode": {"x": y_col, "y": x_col}}],
                },
                alt=f"Ranking de la consulta {name}",
            )
    except Exception as exc:
        st.error(f"No se pudieron ejecutar las consultas locales: {exc}")
        st.caption("Ejecuta `python -m src.batch` para generar los Parquet procesados.")


def main() -> None:
    st.title("Centro de control de movilidad urbana - NYC")
    st.caption("Reto 3: Kafka + Spark + Cassandra + Neo4j + S3 + GCS + BigQuery/DuckDB")

    day = simulation_day()
    with st.sidebar:
        st.header("Controles")
        day = st.text_input("Día simulado (YYYYMMDD)", value=day)
        st.toggle("Auto-refresh cada 5 s", value=True, key="auto_refresh")
        st.caption("Neo4j Browser: `bolt://localhost:7687` · usuario `neo4j`")
        st.code(f"Cassandra: {CASSANDRA_HOST}:{CASSANDRA_PORT}\nNeo4j: {NEO4J_URI}")

    tabs = st.tabs(
        [
            ":material/analytics: Resumen",
            ":material/map: Mapa de red",
            ":material/monitoring: Congestión en vivo",
            ":material/hub: Grafo y rutas",
            ":material/table_chart: Analítica histórica",
        ],
        on_change="rerun",
    )
    tab_summary, tab_network, tab_live, tab_graph, tab_history = tabs

    if tab_summary.open:
        with tab_summary:
            render_summary(load_metadata())
    if tab_network.open:
        with tab_network:
            render_network(day)
    if tab_live.open:
        with tab_live:
            render_live(day)
    if tab_graph.open:
        with tab_graph:
            render_graph()
    if tab_history.open:
        with tab_history:
            render_history()


if __name__ == "__main__":
    main()
