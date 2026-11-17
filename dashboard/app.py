"""Live analytics dashboard over the PostgreSQL tables written by the stream processor."""
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from alerts import evaluate_alerts
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

DSN = os.getenv("POSTGRES_DSN", "postgresql://pipeline:pipeline@localhost:5432/analytics")
ACCENT = "#4F46E5"
WINDOW = "event_time >= now() - make_interval(mins => :w)"

KPI_SQL = f"""
    SELECT count(*)                                                          AS orders,
           coalesce(sum(total_amount) FILTER (WHERE status <> 'cancelled'), 0) AS revenue,
           coalesce(avg(total_amount), 0)                                    AS aov,
           coalesce(avg(extract(epoch FROM (ingested_at - event_time))), 0)  AS latency_s,
           count(*) FILTER (WHERE event_time >= now() - interval '1 minute') AS last_minute
    FROM orders WHERE {WINDOW}
"""
INVALID_SQL = "SELECT count(*) AS invalid FROM dead_letter WHERE received_at >= now() - make_interval(mins => :w)"
FRESHNESS_SQL = "SELECT extract(epoch FROM (now() - max(ingested_at))) AS seconds_since FROM orders"
REVENUE_COMPARE_SQL = """
    SELECT coalesce(sum(total_amount) FILTER (WHERE status <> 'cancelled' AND event_time >= now() - interval '5 minutes'), 0) AS cur,
           coalesce(sum(total_amount) FILTER (WHERE status <> 'cancelled' AND event_time <  now() - interval '5 minutes'), 0) AS prev
    FROM orders WHERE event_time >= now() - interval '10 minutes'
"""
TREND_SQL = f"""
    SELECT date_trunc('minute', event_time) AS minute,
           count(*) AS orders,
           coalesce(sum(total_amount) FILTER (WHERE status <> 'cancelled'), 0) AS revenue
    FROM orders WHERE {WINDOW} GROUP BY 1 ORDER BY 1
"""
CATEGORY_SQL = f"""
    SELECT category, sum(total_amount) AS revenue FROM orders
    WHERE {WINDOW} AND status <> 'cancelled' GROUP BY 1 ORDER BY 2
"""
PRODUCT_SQL = f"""
    SELECT product, sum(total_amount) AS revenue FROM orders
    WHERE {WINDOW} AND status <> 'cancelled' GROUP BY 1 ORDER BY 2 DESC LIMIT 5
"""
COUNTRY_SQL = f"""
    SELECT country, count(*) AS orders FROM orders
    WHERE {WINDOW} GROUP BY 1 ORDER BY 2 DESC
"""
RECENT_SQL = """
    SELECT event_time, order_id, product, category, quantity, total_amount, country, status
    FROM orders ORDER BY event_time DESC LIMIT 15
"""
DEAD_LETTER_SQL = """
    SELECT received_at, error_reason, left(raw_payload, 80) AS payload_preview
    FROM dead_letter ORDER BY received_at DESC LIMIT 10
"""


@st.cache_resource
def get_engine():
    return create_engine(DSN, pool_pre_ping=True)


def query(sql: str, **params) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def style(fig, height: int = 300):
    fig.update_layout(
        template="plotly_white",
        height=height,
        margin=dict(l=10, r=10, t=40, b=10),
        title_font_size=15,
        showlegend=False,
    )
    return fig


def render(window: int) -> None:
    try:
        kpi = query(KPI_SQL, w=window).iloc[0]
        invalid = int(query(INVALID_SQL, w=window).iloc[0]["invalid"])
        seconds_since = query(FRESHNESS_SQL).iloc[0]["seconds_since"]
        rev = query(REVENUE_COMPARE_SQL).iloc[0]
        trend = query(TREND_SQL, w=window)
        category = query(CATEGORY_SQL, w=window)
        products = query(PRODUCT_SQL, w=window)
        countries = query(COUNTRY_SQL, w=window)
        recent = query(RECENT_SQL)
        dead = query(DEAD_LETTER_SQL)
    except SQLAlchemyError as exc:
        st.error(f"Database unavailable: {exc.__class__.__name__}. Retrying on the next refresh.")
        return

    # --- Alerts -----------------------------------------------------------
    alerts = evaluate_alerts(
        orders_in_window=int(kpi["orders"]),
        invalid_in_window=invalid,
        seconds_since_last_ingest=None if pd.isna(seconds_since) else float(seconds_since),
        avg_latency_seconds=float(kpi["latency_s"]),
        revenue_last_5m=float(rev["cur"]),
        revenue_prev_5m=float(rev["prev"]),
    )
    for alert in alerts:
        getattr(st, alert.level)(alert.message)
    if not alerts:
        st.success("All systems normal: stream is flowing and data quality is within limits.")

    # --- KPIs -------------------------------------------------------------
    total_msgs = int(kpi["orders"]) + invalid
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Orders", f"{int(kpi['orders']):,}")
    c2.metric("Revenue (USD)", f"${float(kpi['revenue']):,.0f}")
    c3.metric("Avg order value", f"${float(kpi['aov']):,.2f}")
    c4.metric("Orders last minute", f"{int(kpi['last_minute']):,}")
    c5.metric("Invalid messages", f"{invalid:,}", f"{invalid / total_msgs:.1%}" if total_msgs else None,
              delta_color="inverse")
    st.caption(f"Average pipeline latency (event time to stored): {float(kpi['latency_s']):.2f}s")

    if trend.empty:
        st.info("No orders in this time window yet.")
        return

    # --- Trends -----------------------------------------------------------
    left, right = st.columns(2)
    fig = px.area(trend, x="minute", y="revenue", title="Revenue per minute", color_discrete_sequence=[ACCENT])
    fig.update_layout(xaxis_title=None, yaxis_title="USD")
    left.plotly_chart(style(fig), use_container_width=True)

    fig = px.bar(trend, x="minute", y="orders", title="Orders per minute", color_discrete_sequence=[ACCENT])
    fig.update_layout(xaxis_title=None, yaxis_title="Orders")
    right.plotly_chart(style(fig), use_container_width=True)

    # --- Breakdowns -------------------------------------------------------
    a, b, c = st.columns(3)
    fig = px.bar(category, x="revenue", y="category", orientation="h", title="Revenue by category",
                 color_discrete_sequence=[ACCENT])
    fig.update_layout(xaxis_title="USD", yaxis_title=None)
    a.plotly_chart(style(fig), use_container_width=True)

    fig = px.bar(products.iloc[::-1], x="revenue", y="product", orientation="h", title="Top 5 products",
                 color_discrete_sequence=[ACCENT])
    fig.update_layout(xaxis_title="USD", yaxis_title=None)
    b.plotly_chart(style(fig), use_container_width=True)

    fig = px.bar(countries, x="country", y="orders", title="Orders by country", color_discrete_sequence=[ACCENT])
    fig.update_layout(xaxis_title=None, yaxis_title="Orders")
    c.plotly_chart(style(fig), use_container_width=True)

    # --- Tables -----------------------------------------------------------
    st.subheader("Latest orders")
    st.dataframe(recent, use_container_width=True, hide_index=True)

    with st.expander(f"Dead-letter queue: latest invalid messages ({invalid:,} in window)"):
        if dead.empty:
            st.write("No invalid messages recorded.")
        else:
            st.dataframe(dead, use_container_width=True, hide_index=True)


st.set_page_config(page_title="Real-Time Order Analytics", page_icon=":material/monitoring:", layout="wide")
st.title("Real-Time Order Analytics")
st.caption("Kafka → Python stream processor → PostgreSQL → Streamlit")

with st.sidebar:
    st.header("Controls")
    window_minutes = st.select_slider("Time window (minutes)", options=[5, 15, 30, 60], value=15)
    refresh_seconds = st.slider("Auto-refresh (seconds)", min_value=2, max_value=30, value=5)

st.fragment(run_every=refresh_seconds)(render)(window_minutes)
