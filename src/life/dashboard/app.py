from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from life.config import load_settings
from life.dashboard.data import (
    aggregate_period,
    load_daily_frame,
    metric_columns,
)


def _theme_css() -> str:
    bg = "#0f1117"
    card = "#1a1f2b"
    text = "#e6e8ec"

    return f"""
    <style>
    .stApp {{ background: {bg}; color: {text}; }}
    .life-card {{
        background: {card};
        border-radius: 14px;
        padding: 14px 16px;
        border: 1px solid rgba(127,127,127,0.22);
    }}
    </style>
    """


def _safe_delta(today: float | int | None, prev: float | int | None) -> str:
    if today is None or prev is None or pd.isna(today) or pd.isna(prev):
        return "n/a"
    delta = float(today) - float(prev)
    sign = "+" if delta >= 0 else ""
    return f"{sign}{delta:.1f}"


def _home_tab(df: pd.DataFrame, metrics: list[str]) -> None:
    st.subheader("Today + last 7 days")
    if df.empty:
        st.info("No data in daily_features yet.")
        return

    today_row = df.iloc[-1]
    prev_row = df.iloc[-2] if len(df) > 1 else None
    recent = df.tail(7)

    kpi_cols = [
        ("sleep_score", "Sleep score"),
        ("readiness_score", "Readiness"),
        ("daytime_stress_avg", "Stress"),
        ("anxiety_status_score", "Anxiety"),
        ("steps", "Steps"),
        ("sleep_total_hours", "Sleep hrs"),
    ]

    cols = st.columns(3)
    for idx, (field, label) in enumerate(kpi_cols):
        if field not in df.columns:
            continue
        val = today_row.get(field)
        prev = prev_row.get(field) if prev_row is not None else None
        with cols[idx % 3]:
            st.metric(label, "n/a" if pd.isna(val) else f"{val:.1f}", _safe_delta(val, prev))

    st.markdown("#### 7-day mini trends")
    spark_metrics = [m for m, _ in kpi_cols if m in df.columns]
    for metric in spark_metrics:
        fig = px.line(recent, x="date_local", y=metric, markers=True)
        fig.update_layout(height=180, margin=dict(l=10, r=10, t=20, b=10), title=metric)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Weekly rollup")
    steps_sum = recent["steps"].sum(min_count=1) if "steps" in recent.columns else np.nan
    active_sum = (
        recent["active_calories"].sum(min_count=1)
        if "active_calories" in recent.columns
        else np.nan
    )
    anx_avg = (
        recent["anxiety_status_score"].mean()
        if "anxiety_status_score" in recent.columns
        else np.nan
    )
    read_avg = recent["readiness_score"].mean() if "readiness_score" in recent.columns else np.nan

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Steps (7d)", "n/a" if pd.isna(steps_sum) else f"{steps_sum:,.0f}")
    c2.metric("Active kcal (7d)", "n/a" if pd.isna(active_sum) else f"{active_sum:,.0f}")
    c3.metric("Anxiety avg (7d)", "n/a" if pd.isna(anx_avg) else f"{anx_avg:.2f}")
    c4.metric("Readiness avg (7d)", "n/a" if pd.isna(read_avg) else f"{read_avg:.2f}")

    st.markdown("#### Movers in last 7 days")
    move_table: list[dict[str, float | str]] = []
    for metric in metrics:
        series = recent[metric].dropna()
        if len(series) < 2:
            continue
        if pd.api.types.is_bool_dtype(series):
            continue
        move_table.append(
            {
                "metric": metric,
                "delta_7d": float(series.iloc[-1] - series.iloc[0]),
            }
        )
    if move_table:
        movers = pd.DataFrame(move_table)
        movers["abs_delta"] = movers["delta_7d"].abs()
        movers = movers.sort_values("abs_delta", ascending=False).head(8)
        st.dataframe(movers[["metric", "delta_7d"]], use_container_width=True, hide_index=True)


def _bubble_tab(df: pd.DataFrame, metrics: list[str]) -> None:
    st.subheader("Bubble explorer")
    if not metrics:
        st.info("No numeric metrics available.")
        return
    c1, c2, c3, c4 = st.columns(4)
    x = c1.selectbox(
        "X",
        options=metrics,
        index=metrics.index("sleep_score") if "sleep_score" in metrics else 0,
    )
    y = c2.selectbox(
        "Y",
        options=metrics,
        index=metrics.index("anxiety_status_score") if "anxiety_status_score" in metrics else 0,
    )
    size = c3.selectbox(
        "Size",
        options=metrics,
        index=metrics.index("steps") if "steps" in metrics else 0,
    )
    color = c4.selectbox(
        "Color",
        options=metrics,
        index=metrics.index("readiness_score") if "readiness_score" in metrics else 0,
    )

    size_series = pd.to_numeric(df[size], errors="coerce")
    size_adjusted = size_series.copy()
    size_min = size_adjusted.min(skipna=True)
    size_shifted = False
    if pd.notna(size_min) and float(size_min) <= 0:
        size_adjusted = size_adjusted - float(size_min) + 1e-6
        size_shifted = True

    plot_df = pd.DataFrame(
        {
            "date_local": df["date_local"],
            "x_value": df[x],
            "y_value": df[y],
            "size_value": size_adjusted,
            "color_value": df[color],
        }
    ).dropna()
    fig = px.scatter(
        plot_df,
        x="x_value",
        y="y_value",
        size="size_value",
        color="color_value",
        hover_data=["date_local"],
        labels={
            "x_value": x,
            "y_value": y,
            "size_value": size,
            "color_value": color,
        },
    )
    if size_shifted:
        st.caption(
            f"Size metric `{size}` had non-positive values; bubble sizes were shifted to stay > 0."
        )
    fig.update_layout(height=560)
    st.plotly_chart(fig, use_container_width=True)


def _trends_tab(df: pd.DataFrame, metrics: list[str]) -> None:
    st.subheader("Trend explorer")
    if not metrics:
        st.info("No numeric metrics available.")
        return

    c1, c2, c3 = st.columns(3)
    period = c1.selectbox("Granularity", options=["day", "week", "month", "year"], index=0)
    x_metric = c2.selectbox("Primary metric", options=metrics, index=0)
    y_metric = c3.selectbox("Secondary metric", options=metrics, index=1 if len(metrics) > 1 else 0)
    smooth = st.checkbox("7-point smoothing", value=False)

    if period == "day":
        plot_df = df[["date_local", x_metric, y_metric]].copy()
    else:
        agg_df = aggregate_period(df, period)
        missing = [m for m in (x_metric, y_metric) if m not in agg_df.columns]
        if missing:
            st.warning(
                "Selected metrics are unavailable for aggregated view: "
                + ", ".join(missing)
                + ". Switch to daily or choose other metrics."
            )
            return
        plot_df = agg_df[["period_start", x_metric, y_metric]].rename(
            columns={"period_start": "date_local"}
        )

    if smooth:
        plot_df[x_metric] = plot_df[x_metric].rolling(7, min_periods=1).mean()
        plot_df[y_metric] = plot_df[y_metric].rolling(7, min_periods=1).mean()

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=plot_df["date_local"],
            y=plot_df[x_metric],
            mode="lines+markers",
            name=x_metric,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=plot_df["date_local"],
            y=plot_df[y_metric],
            mode="lines+markers",
            name=y_metric,
            yaxis="y2",
        )
    )
    fig.update_layout(
        height=540,
        yaxis=dict(title=x_metric),
        yaxis2=dict(title=y_metric, overlaying="y", side="right"),
    )
    st.plotly_chart(fig, use_container_width=True)


def _period_summary_tab(df: pd.DataFrame, metrics: list[str]) -> None:
    st.subheader("Week / month / year summaries")
    period = st.selectbox("Period", options=["week", "month", "year"], index=0)
    agg_df = aggregate_period(df, period)
    if agg_df.empty:
        st.info("No data.")
        return

    latest = agg_df.iloc[-1]
    c1, c2, c3 = st.columns(3)
    if "steps" in agg_df.columns:
        c1.metric("Total steps", f"{latest['steps']:,.0f}")
    if "anxiety_status_score" in agg_df.columns:
        c2.metric("Avg anxiety", f"{latest['anxiety_status_score']:.2f}")
    c3.metric("Coverage", f"{latest['coverage_pct']:.1f}%")

    defaults = [
        "steps",
        "active_calories",
        "sleep_total_hours",
        "readiness_score",
        "anxiety_status_score",
        "sleep_efficiency_pct",
    ]
    selected_defaults = [m for m in defaults if m in agg_df.columns]
    selected = st.multiselect(
        "Columns",
        options=["period_start", "days_present", "coverage_pct", *metrics],
        default=["period_start", "days_present", "coverage_pct", *selected_defaults],
    )
    st.dataframe(agg_df[selected], use_container_width=True, hide_index=True)


def _corr_tab(df: pd.DataFrame, metrics: list[str]) -> None:
    st.subheader("Correlation + lag")
    if not metrics:
        st.info("No numeric metrics available.")
        return

    chosen = st.multiselect(
        "Metrics for matrix",
        options=metrics,
        default=metrics[: min(12, len(metrics))],
    )
    if chosen:
        corr = df[chosen].corr(numeric_only=True)
        fig = px.imshow(corr, color_continuous_scale="RdBu_r", zmin=-1, zmax=1)
        fig.update_layout(height=620)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Lag explorer")
    c1, c2, c3 = st.columns(3)
    x = c1.selectbox("X metric", options=metrics, index=0)
    y = c2.selectbox("Y metric", options=metrics, index=1 if len(metrics) > 1 else 0)
    lag = c3.slider("Lag days", min_value=-14, max_value=14, value=1)
    lagged = df[["date_local", x, y]].copy()
    lagged["x_lagged"] = lagged[x].shift(lag)
    scatter_df = lagged.dropna(subset=["x_lagged", y])

    fig = px.scatter(scatter_df, x="x_lagged", y=y, hover_data=["date_local"])
    fig.update_layout(height=460, xaxis_title=f"{x} lagged by {lag}d")
    st.plotly_chart(fig, use_container_width=True)


def _sleep_tab(df: pd.DataFrame) -> None:
    st.subheader("Sleep + recovery")
    required = [
        "sleep_total_hours",
        "sleep_deep_hours",
        "sleep_rem_hours",
        "sleep_light_hours",
        "sleep_efficiency_pct",
        "sleep_avg_hr",
        "sleep_avg_hrv",
        "sleep_temperature_deviation",
        "readiness_score",
    ]
    present = [c for c in required if c in df.columns]
    if not present:
        st.info("Sleep features not available yet.")
        return

    recent = df.tail(30)
    c1, c2, c3 = st.columns(3)
    if "sleep_total_hours" in df.columns:
        c1.metric("Avg sleep (30d)", f"{recent['sleep_total_hours'].mean():.2f}h")
    if "sleep_efficiency_pct" in df.columns:
        c2.metric("Efficiency (30d)", f"{recent['sleep_efficiency_pct'].mean():.1f}%")
    if "sleep_avg_hrv" in df.columns:
        c3.metric("HRV avg (30d)", f"{recent['sleep_avg_hrv'].mean():.1f}")

    st.markdown("#### Sleep stage composition")
    stage_cols = [
        c for c in ["sleep_deep_hours", "sleep_rem_hours", "sleep_light_hours"] if c in df.columns
    ]
    if stage_cols:
        stage_df = recent[["date_local", *stage_cols]].melt(
            id_vars="date_local", value_vars=stage_cols, var_name="stage", value_name="hours"
        )
        fig = px.bar(stage_df, x="date_local", y="hours", color="stage")
        fig.update_layout(height=430)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Sleep physiology")
    phys_cols = [
        c
        for c in [
            "sleep_avg_hr",
            "sleep_avg_hrv",
            "sleep_temperature_deviation",
            "readiness_score",
        ]
        if c in recent.columns
    ]
    if phys_cols:
        fig = go.Figure()
        for col in phys_cols:
            y_axis = "y2" if "temperature" in col else "y"
            fig.add_trace(
                go.Scatter(
                    x=recent["date_local"],
                    y=recent[col],
                    mode="lines",
                    name=col,
                    yaxis=y_axis,
                )
            )
        fig.update_layout(
            height=430,
            yaxis=dict(title="Sleep physiology"),
            yaxis2=dict(title="Temperature deviation", overlaying="y", side="right"),
        )
        st.plotly_chart(fig, use_container_width=True)


def _quality_tab(df: pd.DataFrame, metrics: list[str]) -> None:
    st.subheader("Data quality")
    if df.empty:
        st.info("No data.")
        return

    st.markdown("#### Hard validators")
    validators = [
        ("readiness_score", (0, 100)),
        ("sleep_score", (0, 100)),
        ("activity_score", (0, 100)),
        ("anxiety_status_score", (1, 5)),
        ("physical_status_score", (1, 5)),
        ("productivity_score", (1, 5)),
        ("sleep_efficiency_pct", (0, 100)),
        ("sleep_deep_share_pct", (0, 100)),
        ("sleep_rem_share_pct", (0, 100)),
        ("sleep_light_share_pct", (0, 100)),
        ("spo2_average", (70, 100)),
    ]
    issues: list[dict[str, str | float | int]] = []
    for metric, (low, high) in validators:
        if metric not in df.columns:
            continue
        series = df[metric].dropna()
        if series.empty:
            continue
        bad = series[(series < low) | (series > high)]
        if bad.empty:
            continue
        bad_rows = df.loc[bad.index, ["date_local", metric]]
        for _, row in bad_rows.iterrows():
            issues.append(
                {
                    "metric": metric,
                    "date_local": row["date_local"],
                    "value": float(row[metric]),
                    "expected_min": float(low),
                    "expected_max": float(high),
                }
            )

    if issues:
        st.error(f"{len(issues)} hard validation issues found.")
        issues_df = pd.DataFrame(issues).sort_values(["metric", "date_local"])
        st.dataframe(issues_df, use_container_width=True, hide_index=True)
    else:
        st.success("All hard validators passed.")

    miss = pd.DataFrame(
        {
            "metric": metrics,
            "missing_pct": [100.0 * df[m].isna().mean() for m in metrics],
            "non_null_days": [int(df[m].notna().sum()) for m in metrics],
        }
    ).sort_values("missing_pct")
    st.dataframe(miss, use_container_width=True, hide_index=True)


def main() -> None:
    settings = load_settings()
    st.set_page_config(page_title="Life dashboard", page_icon=":bar_chart:", layout="wide")

    with st.sidebar:
        st.title("Life dashboard")
        db_path_str = st.text_input("DuckDB path", value=str(settings.duckdb_path))
        st.caption("Home is focused on today + last 7 days.")

    st.markdown(_theme_css(), unsafe_allow_html=True)

    df = load_daily_frame(Path(db_path_str))
    metrics = metric_columns(df)

    tabs = st.tabs(
        [
            "Home",
            "Explore",
            "Trends",
            "Period Summary",
            "Correlations & Lags",
            "Sleep + Recovery",
            "Data Quality",
        ]
    )

    with tabs[0]:
        _home_tab(df, metrics)
    with tabs[1]:
        _bubble_tab(df, metrics)
    with tabs[2]:
        _trends_tab(df, metrics)
    with tabs[3]:
        _period_summary_tab(df, metrics)
    with tabs[4]:
        _corr_tab(df, metrics)
    with tabs[5]:
        _sleep_tab(df)
    with tabs[6]:
        _quality_tab(df, metrics)


if __name__ == "__main__":
    main()
