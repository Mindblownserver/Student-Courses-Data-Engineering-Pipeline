from __future__ import annotations

import json
import os
from urllib.request import urlopen

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


DB_PATH = os.getenv("DUCKDB_PATH", "/data/gold/gold.duckdb")
TUNISIA_ADM1_GEOJSON_URL = os.getenv(
    "TUNISIA_ADM1_GEOJSON_URL",
    "https://github.com/wmgeolab/geoBoundaries/raw/9469f09/releaseData/gbOpen/TUN/ADM1/geoBoundaries-TUN-ADM1_simplified.geojson",
)

st.set_page_config(
    page_title="Gold KPI Dashboard",
    page_icon="📊",
    layout="wide",
)


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH, read_only=True)


def _normalize_region_name(value: str) -> str:
    cleaned = (value or "").upper().strip()
    for token in ["-", "_", "'", " "]:
        cleaned = cleaned.replace(token, "")
    return cleaned


@st.cache_data(ttl=3600)
def _load_tunisia_geojson() -> dict:
    with urlopen(TUNISIA_ADM1_GEOJSON_URL, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))

    for feature in payload.get("features", []):
        region_name = feature.get("properties", {}).get("shapeName", "")
        feature["id"] = _normalize_region_name(region_name)

    return payload


@st.cache_data(ttl=60)
def _load_daily_paid() -> pd.DataFrame:
    conn = _connect()
    try:
        return conn.execute(
            """
            SELECT
                d.date_value,
                SUM(f.student_total_count) AS total_students,
                SUM(f.student_paid_count) AS paid_students,
                SUM(f.student_total_count) - SUM(f.student_paid_count) AS unpaid_students,
                CASE
                    WHEN SUM(f.student_total_count) = 0 THEN 0.0
                    ELSE 100.0 * SUM(f.student_paid_count)::DOUBLE / SUM(f.student_total_count)::DOUBLE
                END AS paid_rate_pct
            FROM fact_student_daily f
            JOIN dim_date d ON d.date_key = f.date_key
            GROUP BY d.date_value
            ORDER BY d.date_value
            """
        ).df()
    finally:
        conn.close()


@st.cache_data(ttl=60)
def _load_region_paid() -> pd.DataFrame:
    conn = _connect()
    try:
        return conn.execute(
            """
            SELECT
                d.date_value,
                c.region_name,
                SUM(f.student_total_count) AS total_students,
                SUM(f.student_paid_count) AS paid_students,
                CASE
                    WHEN SUM(f.student_total_count) = 0 THEN 0.0
                    ELSE 100.0 * SUM(f.student_paid_count)::DOUBLE / SUM(f.student_total_count)::DOUBLE
                END AS paid_rate_pct
            FROM fact_student_daily f
            JOIN dim_date d ON d.date_key = f.date_key
            JOIN dim_city c ON c.city_key = f.city_key
            GROUP BY d.date_value, c.region_name
            ORDER BY d.date_value, c.region_name
            """
        ).df()
    finally:
        conn.close()


@st.cache_data(ttl=60)
def _load_course_enrollment() -> pd.DataFrame:
    conn = _connect()
    try:
        return conn.execute(
            """
            SELECT
                d.date_value,
                m.region_name,
                m.course_name,
                m.enrolled_students_count
            FROM mv_course_enrollment_region_daily m
            JOIN dim_date d ON d.date_key = m.date_key
            ORDER BY d.date_value, m.enrolled_students_count DESC
            """
        ).df()
    finally:
        conn.close()


@st.cache_data(ttl=60)
def _load_access_type_mix() -> pd.DataFrame:
    conn = _connect()
    try:
        return conn.execute(
            """
            SELECT
                COALESCE(access_type, 'UNKNOWN') AS access_type,
                COUNT(*) AS events_count,
                COUNT(DISTINCT student_id) AS students_count
            FROM fact_course_enrollment_event
            GROUP BY COALESCE(access_type, 'UNKNOWN')
            ORDER BY events_count DESC
            """
        ).df()
    finally:
        conn.close()


st.title("Gold Layer KPI Dashboard")
st.caption(f"DuckDB source: `{DB_PATH}`")

daily_paid = _load_daily_paid()
region_paid = _load_region_paid()
course_enrollment = _load_course_enrollment()
access_mix = _load_access_type_mix()

if daily_paid.empty:
    st.warning("No Gold KPI data found yet. Run Silver + Gold DAGs, then refresh.")
    st.stop()

latest = daily_paid.iloc[-1]
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Students", int(latest["total_students"]))
col2.metric("Paid Students", int(latest["paid_students"]))
col3.metric("Unpaid Students", int(latest["unpaid_students"]))
col4.metric("Paid Rate", f"{latest['paid_rate_pct']:.2f}%")

left, right = st.columns((2, 1))
with left:
    fig_trend = go.Figure()
    fig_trend.add_trace(
        go.Scatter(
            x=daily_paid["date_value"],
            y=daily_paid["paid_rate_pct"],
            mode="lines+markers",
            name="Paid Rate %",
            line={"width": 3, "color": "#0E7490"},
        )
    )
    fig_trend.update_layout(
        title="Paid Rate Trend",
        xaxis_title="Date",
        yaxis_title="Paid Rate (%)",
        template="plotly_white",
        height=380,
    )
    st.plotly_chart(fig_trend, use_container_width=True)

with right:
    latest_region = region_paid[
        region_paid["date_value"] == region_paid["date_value"].max()
    ]
    if not latest_region.empty:
        fig_region = px.bar(
            latest_region.sort_values(by="paid_rate_pct", ascending=False),
            x="region_name",
            y="paid_rate_pct",
            color="paid_rate_pct",
            color_continuous_scale="Teal",
            title="Paid Rate by Region (Latest Day)",
        )
        fig_region.update_layout(
            xaxis_title="Region",
            yaxis_title="Paid Rate (%)",
            template="plotly_white",
            height=380,
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig_region, use_container_width=True)
    else:
        st.info("No regional paid-rate data yet.")

st.subheader("Tunisia Regional KPI Map")
try:
    tunisia_geojson = _load_tunisia_geojson()
    latest_region = region_paid[
        region_paid["date_value"] == region_paid["date_value"].max()
    ].copy()
    latest_region["region_key"] = (
        latest_region["region_name"].astype("string").apply(_normalize_region_name)
    )

    if latest_region.empty:
        st.info("No regional rows available for map rendering yet.")
    else:
        fig_map = px.choropleth_mapbox(
            latest_region,
            geojson=tunisia_geojson,
            locations="region_key",
            featureidkey="id",
            color="paid_rate_pct",
            color_continuous_scale="YlGnBu",
            hover_name="region_name",
            hover_data={
                "paid_rate_pct": ":.2f",
                "paid_students": True,
                "total_students": True,
                "region_key": False,
            },
            center={"lat": 34.0, "lon": 9.5},
            mapbox_style="carto-positron",
            zoom=5.2,
            opacity=0.85,
            title="Paid Rate by Region (hover for KPIs)",
        )
        fig_map.update_layout(
            margin={"l": 0, "r": 0, "t": 50, "b": 0},
            height=520,
            coloraxis_colorbar={"title": "Paid rate %"},
        )
        st.plotly_chart(fig_map, use_container_width=True)
except Exception as exc:
    st.warning(f"Map could not be loaded: {exc}")

bottom_left, bottom_right = st.columns((2, 1))
with bottom_left:
    if not course_enrollment.empty:
        latest_course = course_enrollment[
            course_enrollment["date_value"] == course_enrollment["date_value"].max()
        ].copy()
        latest_course = latest_course.sort_values(
            by="enrolled_students_count", ascending=False
        ).head(12)
        fig_courses = px.bar(
            latest_course,
            x="course_name",
            y="enrolled_students_count",
            color="region_name",
            title="Top Courses by Enrollment (Latest Day)",
        )
        fig_courses.update_layout(
            xaxis_title="Course",
            yaxis_title="Enrolled Students",
            xaxis_tickangle=-25,
            template="plotly_white",
            height=420,
        )
        st.plotly_chart(fig_courses, use_container_width=True)
    else:
        st.info("No course enrollment mart rows yet.")

with bottom_right:
    if not access_mix.empty:
        fig_access = px.pie(
            access_mix,
            values="events_count",
            names="access_type",
            title="Enrollment Access Type Mix",
            hole=0.45,
        )
        fig_access.update_layout(template="plotly_white", height=420)
        st.plotly_chart(fig_access, use_container_width=True)
    else:
        st.info("No enrollment event rows yet.")

st.subheader("Regional KPI Table")
st.dataframe(
    region_paid.sort_values(["date_value", "paid_rate_pct"], ascending=[False, False]),
    use_container_width=True,
)
