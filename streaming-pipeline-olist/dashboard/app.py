"""Streamlit live dashboard for the Olist real-time streaming pipeline.

Reads the Gold Delta tables every few seconds and renders headline KPIs,
revenue/category bar charts, an orders-per-minute time series and a table of the
most recent windows. The sidebar surfaces pipeline health, Kafka consumer lag
and uptime. Designed for a continuously looping producer so the numbers keep
climbing during a screen recording.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import streamlit as st

import queries

REFRESH_SECONDS = 5

GOLD_PATH = os.environ.get("GOLD_PATH", "data/gold/orders_agg")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ORDERS_TOPIC = os.environ.get("TOPIC_ORDERS_RAW", "orders-raw")
INGESTION_GROUP = "spark-ingestion-job"


@st.cache_resource(show_spinner=False)
def get_spark():
    """Build (once) a local Delta-enabled SparkSession for the dashboard.

    Returns:
        A cached :class:`pyspark.sql.SparkSession`.
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("olist-dashboard")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _kafka_lag() -> int | None:
    """Best-effort total consumer lag for the ingestion group.

    Returns:
        Total lag, or ``None`` if Kafka is unreachable / client unavailable.
    """
    try:
        from confluent_kafka import Consumer, TopicPartition

        consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": INGESTION_GROUP,
                "enable.auto.commit": False,
            }
        )
        try:
            meta = consumer.list_topics(ORDERS_TOPIC, timeout=5).topics.get(
                ORDERS_TOPIC
            )
            if meta is None or meta.error is not None:
                return None
            parts = [TopicPartition(ORDERS_TOPIC, p) for p in meta.partitions]
            committed = {
                tp.partition: tp.offset
                for tp in consumer.committed(parts, timeout=5)
            }
            lag = 0
            for pid in meta.partitions:
                _, high = consumer.get_watermark_offsets(
                    TopicPartition(ORDERS_TOPIC, pid), timeout=5
                )
                offset = committed.get(pid, 0)
                lag += max(0, high - (offset if offset and offset > 0 else 0))
            return lag
        finally:
            consumer.close()
    except Exception:  # noqa: BLE001
        return None


def render_sidebar(spark, pipeline_ok: bool) -> None:
    """Render the sidebar with job status, Kafka lag and uptime.

    Args:
        spark: Active SparkSession.
        pipeline_ok: Whether the Gold table currently has data.
    """
    st.sidebar.header("🛰️ Pipeline status")
    status_color = "🟢" if pipeline_ok else "🔴"
    st.sidebar.markdown(f"**Jobs:** {status_color} {'active' if pipeline_ok else 'idle'}")

    lag = _kafka_lag()
    lag_label = "n/a" if lag is None else f"{lag:,}"
    st.sidebar.metric("Kafka consumer lag", lag_label)

    started = st.session_state.get("started_at")
    if started:
        uptime = int(time.time() - started)
        h, rem = divmod(uptime, 3600)
        m, s = divmod(rem, 60)
        st.sidebar.metric("Pipeline uptime", f"{h:02d}:{m:02d}:{s:02d}")

    st.sidebar.caption(f"Gold path: `{GOLD_PATH}`")


def render_dashboard(spark) -> None:
    """Render one full frame of the dashboard.

    Args:
        spark: Active SparkSession.
    """
    kpis = queries.get_kpi_summary(spark, GOLD_PATH)
    pipeline_ok = kpis["total_orders"] > 0
    render_sidebar(spark, pipeline_ok)

    st.title("⚡ Real-time Orders Pipeline — Olist")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    st.caption(f"Última atualização: {now} · auto-refresh {REFRESH_SECONDS}s")

    if not pipeline_ok:
        with st.spinner("Aguardando pipeline... (nenhum dado no Gold ainda)"):
            time.sleep(REFRESH_SECONDS)
        return

    # ---- Row 1: KPI cards ----
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Pedidos", f"{kpis['total_orders']:,}")
    c2.metric("Receita Total", f"R$ {kpis['total_revenue']:,.2f}")
    c3.metric("Ticket Médio", f"R$ {kpis['avg_ticket']:,.2f}")
    c4.metric("Clientes Únicos", f"{kpis['unique_customers']:,}")

    # ---- Row 2: revenue by state + top categories ----
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Receita por Estado (Top 10)")
        rev = queries.get_revenue_by_state(spark, GOLD_PATH, limit=10)
        if not rev.empty:
            st.bar_chart(rev.set_index("customer_state")["total_revenue"])
        else:
            st.info("Sem dados de receita ainda.")
    with col_right:
        st.subheader("Top Categorias (Top 10)")
        cats = queries.get_top_categories(spark, GOLD_PATH, limit=10)
        if not cats.empty:
            st.bar_chart(cats.set_index("product_category")["total_orders"])
        else:
            st.info("Sem dados de categorias ainda.")

    # ---- Row 3: orders per minute ----
    st.subheader("Orders per Minute (janela 5min)")
    ts = queries.get_orders_timeseries(spark, GOLD_PATH, minutes=30)
    if not ts.empty:
        st.line_chart(ts.set_index("window_start")["orders_per_minute"])
    else:
        st.info("Série temporal ainda não disponível.")

    # ---- Row 4: recent windows table ----
    st.subheader("Últimos 20 eventos agregados")
    recent = queries.get_recent_windows(spark, GOLD_PATH, limit=20)
    if not recent.empty:
        st.dataframe(recent, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhuma janela recente.")


def main() -> None:
    """Configure the page and drive the auto-refresh render loop."""
    st.set_page_config(
        page_title="Olist Real-time Pipeline",
        page_icon="⚡",
        layout="wide",
    )
    if "started_at" not in st.session_state:
        st.session_state["started_at"] = time.time()

    spark = get_spark()
    placeholder = st.empty()
    while True:
        with placeholder.container():
            try:
                render_dashboard(spark)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Erro ao renderizar o dashboard: {exc}")
        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    main()
