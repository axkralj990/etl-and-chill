from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st
from plotly.subplots import make_subplots

from life.config import load_settings
from life.connectors.notion import NotionConnector
from life.connectors.oura import OuraConnector
from life.dashboard.data import (
    ANXIETY_STATUS_COLORS,
    aggregate_period,
    build_anxiety_status_counts,
    build_goals_progress,
    build_leaderboard,
    format_metric_value,
    load_daily_frame,
    metric_columns,
)
from life.enums import SourceName
from life.inference.cmdstan_adapter import CmdStanInferenceAdapter
from life.oauth import OuraOAuthClient, OuraTokenStore
from life.pipeline.runtime_config import load_runtime_config
from life.pipeline.shared import finalize_features, run_notion_sync, run_oura_sync
from life.storage.duckdb import DuckDBStorage

THEME_PRESETS: dict[str, dict[str, str]] = {
    "Deep Ocean": {
        "bg": "#08121d",
        "card": "#102638",
        "text": "#e9f2fb",
        "plot_bg": "#08121d",
        "paper_bg": "#08121d",
        "grid": "#26435a",
        "axis": "#b8c9da",
    }
}

PLOT_BG_HEX = "#08121d"


def _theme_css(theme: dict[str, str]) -> str:
    bg = theme["bg"]
    card = theme["card"]
    text = theme["text"]

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


def _apply_plotly_theme(theme_name: str, theme: dict[str, str]) -> None:
    template_name = f"life_{theme_name.lower().replace(' ', '_').replace('(', '').replace(')', '')}"
    base = pio.templates["plotly_dark"]
    custom = go.layout.Template(base)
    custom.layout.paper_bgcolor = theme["paper_bg"]
    custom.layout.plot_bgcolor = theme["plot_bg"]
    custom.layout.font = dict(color=theme["text"])
    custom.layout.xaxis = dict(
        gridcolor=theme["grid"],
        zerolinecolor=theme["grid"],
        linecolor=theme["axis"],
        tickcolor=theme["axis"],
    )
    custom.layout.yaxis = dict(
        gridcolor=theme["grid"],
        zerolinecolor=theme["grid"],
        linecolor=theme["axis"],
        tickcolor=theme["axis"],
    )
    pio.templates[template_name] = custom
    pio.templates.default = template_name
    px.defaults.template = template_name


def _plotly(fig: go.Figure | go.FigureWidget) -> None:
    fig.update_layout(
        paper_bgcolor=PLOT_BG_HEX,
        plot_bgcolor=PLOT_BG_HEX,
    )
    st.plotly_chart(fig, use_container_width=True)


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
        _plotly(fig)

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
    _plotly(fig)


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
    _plotly(fig)

    st.markdown("#### Correlation summary")
    lag_scan_days = 5

    corr_df = pd.DataFrame(
        {
            "x": pd.to_numeric(plot_df[x_metric], errors="coerce"),
            "y": pd.to_numeric(plot_df[y_metric], errors="coerce"),
        }
    )
    corr_df = corr_df.dropna(subset=["x", "y"])
    if len(corr_df) < 3:
        st.info("Not enough overlapping points to compute correlations.")
        return

    same_time_corr = float(corr_df["x"].corr(corr_df["y"]))

    lag_rows: list[dict[str, float | int]] = []
    for lag in range(-lag_scan_days, lag_scan_days + 1):
        shifted = pd.DataFrame({"x_shifted": corr_df["x"].shift(lag), "y": corr_df["y"]}).dropna()
        if len(shifted) < 3:
            continue
        lag_rows.append(
            {
                "lag_days": lag,
                "corr": float(shifted["x_shifted"].corr(shifted["y"])),
                "n": len(shifted),
            }
        )

    if not lag_rows:
        st.info("Lagged correlation could not be computed for selected metrics.")
        return

    lag_corr = pd.DataFrame(lag_rows)
    lag_corr["abs_corr"] = lag_corr["corr"].abs()
    best = lag_corr.sort_values("abs_corr", ascending=False).iloc[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Corr (lag 0)", f"{same_time_corr:.3f}")
    c2.metric("Best lag", f"{int(best['lag_days'])}d")
    c3.metric("Best |corr|", f"{float(best['corr']):.3f}")

    fig_lag = px.bar(
        lag_corr.sort_values("lag_days"),
        x="lag_days",
        y="corr",
        color="corr",
        color_continuous_scale="RdBu",
        range_color=[-1, 1],
        labels={"lag_days": "Lag (days)", "corr": "Correlation"},
        hover_data={"n": True, "corr": ":.3f", "abs_corr": ":.3f"},
    )
    fig_lag.add_hline(y=0, line_dash="dash", line_color="#cbd5e1")
    fig_lag.add_vline(x=0, line_dash="dash", line_color="#cbd5e1")
    fig_lag.update_layout(height=300, coloraxis_showscale=False)
    _plotly(fig_lag)


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
        _plotly(fig)

    st.markdown("#### Lag explorer")
    c1, c2 = st.columns(2)
    x = c1.selectbox("X metric", options=metrics, index=0)
    y = c2.selectbox("Y metric", options=metrics, index=1 if len(metrics) > 1 else 0)
    st.caption(
        "Lag direction: positive lag uses past values (t-lag); negative lag uses future values."
    )

    l1, l2 = st.columns(2)
    x_lag = int(
        l1.number_input(
            "X lag (days)",
            min_value=-14,
            max_value=14,
            value=1,
            step=1,
            key="explore_x_lag",
        )
    )
    y_lag = int(
        l2.number_input(
            "Y lag (days)",
            min_value=-14,
            max_value=14,
            value=0,
            step=1,
            key="explore_y_lag",
        )
    )

    lagged = pd.DataFrame(
        {
            "date_local": df["date_local"],
            "x_base": pd.to_numeric(df[x], errors="coerce"),
            "y_base": pd.to_numeric(df[y], errors="coerce"),
        }
    )
    lagged["x_lagged"] = lagged["x_base"].shift(x_lag)
    lagged["y_lagged"] = lagged["y_base"].shift(y_lag)
    scatter_df = lagged.dropna(subset=["x_lagged", "y_lagged"])

    fig = px.scatter(scatter_df, x="x_lagged", y="y_lagged", hover_data=["date_local"])
    fig.update_layout(
        height=460,
        xaxis_title=f"{x} (lag {x_lag}d)",
        yaxis_title=f"{y} (lag {y_lag}d)",
    )
    _plotly(fig)


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
        stage_labels = {
            "sleep_deep_hours": "Deep",
            "sleep_rem_hours": "REM",
            "sleep_light_hours": "Light",
        }
        stage_df["stage"] = stage_df["stage"].map(stage_labels).fillna(stage_df["stage"])
        fig = px.bar(
            stage_df,
            x="date_local",
            y="hours",
            color="stage",
            category_orders={"stage": ["Deep", "REM", "Light"]},
            color_discrete_map={
                "Deep": "#0b3d91",
                "REM": "#4ea8de",
                "Light": "#9bd0ff",
            },
        )
        fig.update_layout(height=430)
        _plotly(fig)

    st.markdown("#### Sleep physiology")
    metric_specs = [
        ("sleep_avg_hr", "Average HR", "bpm", "#f4d35e"),
        ("sleep_avg_hrv", "Average HRV", "ms", "#4ea8de"),
        ("sleep_temperature_deviation", "Temperature deviation", "deg", "#5e60ce"),
        ("readiness_score", "Readiness", "pts", "#3da35d"),
    ]
    metric_specs = [spec for spec in metric_specs if spec[0] in recent.columns]

    if metric_specs:
        fig = make_subplots(
            rows=len(metric_specs),
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.04,
        )

        for row_idx, (metric, label, unit, line_color) in enumerate(metric_specs, start=1):
            plot_df = recent[["date_local", metric]].copy()
            plot_df[metric] = pd.to_numeric(plot_df[metric], errors="coerce")
            plot_df = plot_df.dropna(subset=[metric])
            if plot_df.empty:
                continue

            fig.add_trace(
                go.Scatter(
                    x=plot_df["date_local"],
                    y=plot_df[metric],
                    mode="lines+markers",
                    line=dict(color=line_color, width=3),
                    marker=dict(
                        size=7,
                        color=line_color,
                        line=dict(color=line_color, width=1),
                    ),
                    showlegend=False,
                    hovertemplate=(
                        "Date: %{x|%Y-%m-%d}<br>" + f"{label}: %{{y:.2f}} {unit}<extra></extra>"
                    ),
                ),
                row=row_idx,
                col=1,
            )
            fig.update_yaxes(title_text=f"{label} ({unit})", row=row_idx, col=1)

        fig.update_xaxes(title_text="Date", row=len(metric_specs), col=1)
        fig.update_layout(height=240 * len(metric_specs), margin=dict(l=10, r=10, t=50, b=10))
        _plotly(fig)


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


def _leaderboard_tab(df: pd.DataFrame) -> None:
    st.subheader("All-time leaderboard")
    leaderboard = build_leaderboard(df)
    if leaderboard.empty:
        st.info("Not enough data for leaderboard records yet.")
        return

    display = leaderboard.copy()
    display["date_local"] = display["date_local"].dt.date
    display["general_notes"] = display["general_notes"].fillna("").astype(str).str.strip()
    display.loc[display["general_notes"] == "", "general_notes"] = "(empty)"
    st.dataframe(
        display[
            ["metric_label", "record_label", "value_display", "date_local", "general_notes"]
        ].rename(columns={"value_display": "value", "general_notes": "General Notes"}),
        use_container_width=True,
        height=560,
        hide_index=True,
    )


def _goals_tab(df: pd.DataFrame, goals_cfg: dict[str, float]) -> None:
    st.subheader("Goals")
    if df.empty:
        st.info("No data for goals yet.")
        return

    goals_df = df.copy()
    goals_df["date_local"] = pd.to_datetime(goals_df["date_local"], errors="coerce")
    goals_df = goals_df.dropna(subset=["date_local"])
    if goals_df.empty:
        st.info("No dated rows available for goals yet.")
        return

    period = st.selectbox("Period", options=["week", "month"], index=0)
    today = pd.Timestamp.now().normalize()
    selected_ref_date = today

    if period == "week":
        week_starts = (
            goals_df["date_local"].dt.to_period("W").dt.start_time.drop_duplicates().sort_values()
        )
        if not week_starts.empty:
            week_labels = [
                f"{week_start.isocalendar().year}-W{week_start.isocalendar().week:02d}"
                for week_start in week_starts
            ]
            default_week_start = today - pd.Timedelta(days=today.weekday())
            default_idx = 0
            for idx, week_start in enumerate(week_starts):
                if week_start == default_week_start:
                    default_idx = idx
                    break
            selected_week_label = st.selectbox(
                "Week",
                options=week_labels,
                index=default_idx,
                key="goals_week_select",
            )
            selected_week_idx = week_labels.index(selected_week_label)
            selected_ref_date = pd.Timestamp(week_starts.iloc[selected_week_idx])
    else:
        month_starts = (
            goals_df["date_local"].dt.to_period("M").dt.start_time.drop_duplicates().sort_values()
        )
        if not month_starts.empty:
            month_labels = [month_start.strftime("%Y-%m") for month_start in month_starts]
            default_month_start = today.to_period("M").start_time
            default_idx = 0
            for idx, month_start in enumerate(month_starts):
                if month_start == default_month_start:
                    default_idx = idx
                    break
            selected_month_label = st.selectbox(
                "Month",
                options=month_labels,
                index=default_idx,
                key="goals_month_select",
            )
            selected_month_idx = month_labels.index(selected_month_label)
            selected_ref_date = pd.Timestamp(month_starts.iloc[selected_month_idx])

    progress = build_goals_progress(
        df,
        period=period,
        goals=goals_cfg,
        ref_date=selected_ref_date,
    )
    if progress.empty:
        st.info("No goal-compatible metrics available for selected period.")
    else:
        period_label = progress.iloc[0]["period_label"]
        period_start = pd.to_datetime(progress.iloc[0]["period_start"])
        period_end = pd.to_datetime(progress.iloc[0]["period_end"])
        elapsed_days = int((min(today, period_end) - period_start).days + 1)
        total_days = int((period_end - period_start).days + 1)
        st.caption(f"Selected period: {period_label}. Missing days are ignored.")
        st.caption(f"Progress in period: {elapsed_days}/{total_days} days elapsed.")

        cards = st.columns(len(progress))
        for idx, row in progress.reset_index(drop=True).iterrows():
            avg_display = format_metric_value(float(row["avg_value"]), row["metric"])
            goal_display = format_metric_value(float(row["goal"]), row["metric"])
            status = "On track" if bool(row["on_track"]) else "Off track"
            delta_suffix = f"{row['delta_pct']:+.1f}% vs goal"
            with cards[idx]:
                st.metric(
                    label=f"{row['metric_label']} ({status})",
                    value=avg_display,
                    delta=delta_suffix,
                    help=(
                        f"Goal: {goal_display}. "
                        f"Days with data: {int(row['days_with_data'])}/{elapsed_days}."
                    ),
                )
                st.caption(f"Data days: {int(row['days_with_data'])}/{elapsed_days}")

        st.markdown("#### Goal attainment")
        attainment = progress.copy()
        attainment["attainment_pct"] = np.where(
            attainment["goal"] > 0,
            100.0 * attainment["avg_value"] / attainment["goal"],
            0.0,
        )
        attainment["track_label"] = attainment["on_track"].map(
            lambda ok: "On track" if ok else "Off track"
        )

        fig_attain = px.bar(
            attainment.sort_values("attainment_pct"),
            x="attainment_pct",
            y="metric_label",
            orientation="h",
            color="track_label",
            color_discrete_map={"On track": "#3da35d", "Off track": "#d1495b"},
            labels={
                "attainment_pct": "Goal attainment (%)",
                "metric_label": "Metric",
                "track_label": "Status",
            },
            hover_data={
                "avg_value": ":.2f",
                "goal": ":.2f",
                "delta_pct": ":+.1f",
                "track_label": True,
            },
        )
        max_attainment = float(attainment["attainment_pct"].max()) if not attainment.empty else 0.0
        fig_attain.add_vline(
            x=100,
            line_dash="dash",
            line_width=2,
            line_color="#f1f3f5",
        )
        fig_attain.update_layout(
            height=360,
            xaxis=dict(range=[0, max(120.0, max_attainment * 1.1)]),
        )
        _plotly(fig_attain)

        st.markdown("#### Progress vs on-course line")
        period_dates = pd.date_range(start=period_start, end=period_end, freq="D")
        today_cap = min(today, period_end)

        period_df = df.copy()
        period_df["date_local"] = pd.to_datetime(period_df["date_local"], errors="coerce")
        period_df = period_df.dropna(subset=["date_local"])
        period_df = period_df[
            (period_df["date_local"] >= period_start) & (period_df["date_local"] <= period_end)
        ].copy()

        def _on_course_line(target_total: float) -> np.ndarray:
            elapsed_days_idx = np.arange(1, len(period_dates) + 1, dtype=float)
            return target_total * (elapsed_days_idx / float(len(period_dates)))

        chart_cols = st.columns(4)

        with chart_cols[0]:
            step_row = progress[progress["metric"] == "steps"]
            if step_row.empty:
                st.info("Steps goal not configured.")
            else:
                step_goal = float(step_row.iloc[0]["goal"])
                step_target_total = float(len(period_dates)) * step_goal
                step_daily = (
                    period_df.groupby("date_local", dropna=False)["steps"]
                    .sum()
                    .reindex(period_dates)
                    if "steps" in period_df.columns
                    else pd.Series(index=period_dates, dtype=float)
                )
                step_daily = pd.to_numeric(step_daily, errors="coerce").fillna(0.0)
                step_actual = step_daily.cumsum()
                step_actual[step_actual.index > today_cap] = np.nan

                fig_steps = go.Figure()
                fig_steps.add_trace(
                    go.Scatter(
                        x=period_dates,
                        y=_on_course_line(step_target_total),
                        mode="lines",
                        line=dict(color="#f4d35e", width=2, dash="dash"),
                        name="On-course",
                    )
                )
                fig_steps.add_trace(
                    go.Scatter(
                        x=period_dates,
                        y=step_actual,
                        mode="lines+markers",
                        line=dict(color="#4ea8de", width=3),
                        marker=dict(size=5),
                        name="Actual",
                    )
                )
                fig_steps.update_layout(
                    title="Steps",
                    height=320,
                    xaxis_title=f"Time in {period}",
                    yaxis_title="Cumulative steps",
                    yaxis=dict(range=[0, step_target_total]),
                    margin=dict(l=8, r=8, t=40, b=8),
                    showlegend=False,
                )
                _plotly(fig_steps)

        with chart_cols[1]:
            sleep_row = progress[progress["metric"] == "sleep_total_hours"]
            if sleep_row.empty:
                st.info("Sleep goal not configured.")
            else:
                sleep_goal = float(sleep_row.iloc[0]["goal"])
                sleep_target_total = float(len(period_dates)) * sleep_goal
                sleep_daily = (
                    period_df.groupby("date_local", dropna=False)["sleep_total_hours"]
                    .mean()
                    .reindex(period_dates)
                    if "sleep_total_hours" in period_df.columns
                    else pd.Series(index=period_dates, dtype=float)
                )
                sleep_daily = pd.to_numeric(sleep_daily, errors="coerce").fillna(0.0)
                sleep_actual = sleep_daily.cumsum()
                sleep_actual[sleep_actual.index > today_cap] = np.nan

                fig_sleep = go.Figure()
                fig_sleep.add_trace(
                    go.Scatter(
                        x=period_dates,
                        y=_on_course_line(sleep_target_total),
                        mode="lines",
                        line=dict(color="#f4d35e", width=2, dash="dash"),
                        name="On-course",
                    )
                )
                fig_sleep.add_trace(
                    go.Scatter(
                        x=period_dates,
                        y=sleep_actual,
                        mode="lines+markers",
                        line=dict(color="#5e60ce", width=3),
                        marker=dict(size=5),
                        name="Actual",
                    )
                )
                fig_sleep.update_layout(
                    title="Sleep",
                    height=320,
                    xaxis_title=f"Time in {period}",
                    yaxis_title="Cumulative sleep hours",
                    yaxis=dict(range=[0, sleep_target_total]),
                    margin=dict(l=8, r=8, t=40, b=8),
                    showlegend=False,
                )
                _plotly(fig_sleep)

        with chart_cols[2]:
            strength_row = progress[progress["metric"] == "strength_elements"]
            if strength_row.empty:
                st.info("Workout goal not configured.")
            else:
                workout_target_total = float(strength_row.iloc[0]["goal"])
                strength_series = pd.Series(index=period_dates, dtype=float)
                if "workout_elements_json" in period_df.columns:
                    expanded_strength: list[dict[str, object]] = []
                    for row in period_df.itertuples():
                        raw_json = getattr(row, "workout_elements_json", None)
                        if not isinstance(raw_json, str) or not raw_json.strip():
                            continue
                        try:
                            parsed = json.loads(raw_json)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(parsed, list):
                            continue
                        for item in parsed:
                            if not isinstance(item, dict):
                                continue
                            if item.get("type") != "strength":
                                continue
                            elements = item.get("elements")
                            value = elements.get("elements") if isinstance(elements, dict) else None
                            expanded_strength.append(
                                {
                                    "date_local": row.date_local,
                                    "elements": value,
                                }
                            )
                    if expanded_strength:
                        strength_df = pd.DataFrame(expanded_strength)
                        strength_df["date_local"] = pd.to_datetime(
                            strength_df["date_local"], errors="coerce"
                        )
                        strength_df["elements"] = pd.to_numeric(
                            strength_df["elements"], errors="coerce"
                        )
                        strength_df = strength_df.dropna(subset=["date_local", "elements"])
                        strength_series = (
                            strength_df.groupby("date_local", dropna=False)["elements"]
                            .sum()
                            .reindex(period_dates)
                        )

                strength_series = pd.to_numeric(strength_series, errors="coerce").fillna(0.0)
                workout_actual = strength_series.cumsum()
                workout_actual[workout_actual.index > today_cap] = np.nan

                fig_workouts = go.Figure()
                fig_workouts.add_trace(
                    go.Scatter(
                        x=period_dates,
                        y=_on_course_line(workout_target_total),
                        mode="lines",
                        line=dict(color="#f4d35e", width=2, dash="dash"),
                        name="On-course",
                    )
                )
                fig_workouts.add_trace(
                    go.Scatter(
                        x=period_dates,
                        y=workout_actual,
                        mode="lines+markers",
                        line=dict(color="#3da35d", width=3),
                        marker=dict(size=5),
                        name="Actual",
                    )
                )
                fig_workouts.update_layout(
                    title="Workouts",
                    height=320,
                    xaxis_title=f"Time in {period}",
                    yaxis_title="Cumulative workout elements",
                    yaxis=dict(range=[0, workout_target_total]),
                    margin=dict(l=8, r=8, t=40, b=8),
                    showlegend=False,
                )
                _plotly(fig_workouts)

        with chart_cols[3]:
            cardio_row = progress[progress["metric"] == "cardio_events"]
            if cardio_row.empty:
                st.info("Cardio events goal not configured.")
            else:
                cardio_target_total = float(cardio_row.iloc[0]["goal"])
                cardio_daily_events = pd.Series(index=period_dates, dtype=float)
                if "workout_elements_json" in period_df.columns:
                    cardio_rows: list[dict[str, object]] = []
                    for row in period_df.itertuples():
                        raw_json = getattr(row, "workout_elements_json", None)
                        if not isinstance(raw_json, str) or not raw_json.strip():
                            continue
                        try:
                            parsed = json.loads(raw_json)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(parsed, list):
                            continue

                        day_events = 0.0
                        for item in parsed:
                            if not isinstance(item, dict):
                                continue
                            workout_type = item.get("type")
                            elements = item.get("elements")
                            value = elements.get("elements") if isinstance(elements, dict) else None
                            if workout_type in {
                                "running",
                                "cycling",
                                "swimming",
                                "hiking",
                            } and not isinstance(value, int | float):
                                day_events += 1.0
                                continue

                            if not isinstance(value, int | float):
                                continue

                            if workout_type == "running":
                                if value > 30:
                                    day_events += 4.0
                                elif value > 20:
                                    day_events += 3.0
                                elif value > 10:
                                    day_events += 2.0
                                else:
                                    day_events += 1.0
                            elif workout_type == "cycling":
                                if value > 80:
                                    day_events += 4.0
                                elif value > 60:
                                    day_events += 3.0
                                elif value > 30:
                                    day_events += 2.0
                                else:
                                    day_events += 1.0
                            elif workout_type == "swimming":
                                if value > 4:
                                    day_events += 3.0
                                elif value > 2:
                                    day_events += 2.0
                                else:
                                    day_events += 1.0
                            elif workout_type == "hiking":
                                if value > 2000:
                                    day_events += 4.0
                                elif value > 1200:
                                    day_events += 3.0
                                elif value > 700:
                                    day_events += 2.0
                                else:
                                    day_events += 1.0

                        cardio_rows.append({"date_local": row.date_local, "events": day_events})

                    if cardio_rows:
                        cardio_df = pd.DataFrame(cardio_rows)
                        cardio_df["date_local"] = pd.to_datetime(
                            cardio_df["date_local"], errors="coerce"
                        )
                        cardio_df["events"] = pd.to_numeric(cardio_df["events"], errors="coerce")
                        cardio_df = cardio_df.dropna(subset=["date_local", "events"])
                        cardio_daily_events = (
                            cardio_df.groupby("date_local", dropna=False)["events"]
                            .sum()
                            .reindex(period_dates)
                        )

                cardio_daily_events = pd.to_numeric(cardio_daily_events, errors="coerce")
                cardio_daily_events = cardio_daily_events.fillna(0.0)
                cardio_actual = cardio_daily_events.cumsum()
                cardio_actual[cardio_actual.index > today_cap] = np.nan

                fig_cardio = go.Figure()
                fig_cardio.add_trace(
                    go.Scatter(
                        x=period_dates,
                        y=_on_course_line(cardio_target_total),
                        mode="lines",
                        line=dict(color="#f4d35e", width=2, dash="dash"),
                        name="On-course",
                    )
                )
                fig_cardio.add_trace(
                    go.Scatter(
                        x=period_dates,
                        y=cardio_actual,
                        mode="lines+markers",
                        line=dict(color="#2a9d8f", width=3),
                        marker=dict(size=5),
                        name="Actual",
                    )
                )
                fig_cardio.update_layout(
                    title="Cardio events",
                    height=320,
                    xaxis_title=f"Time in {period}",
                    yaxis_title="Cumulative cardio events",
                    yaxis=dict(range=[0, cardio_target_total]),
                    margin=dict(l=8, r=8, t=40, b=8),
                    showlegend=False,
                )
                _plotly(fig_cardio)

        strength_rows = progress[progress["metric"] == "strength_elements"]
        if not strength_rows.empty:
            st.markdown("#### Strength progress")
            strength_row = strength_rows.iloc[0]
            st.progress(
                min(max(float(strength_row["avg_value"]) / float(strength_row["goal"]), 0.0), 1.0),
                text=(
                    f"{int(strength_row['avg_value'])} / {int(strength_row['goal'])} elements"
                    f" ({float(strength_row['delta_pct']):+.1f}%)"
                ),
            )

        latest_frame = progress.copy()
        latest_frame["avg_display"] = latest_frame.apply(
            lambda row: format_metric_value(float(row["avg_value"]), row["metric"]), axis=1
        )
        latest_frame["goal_display"] = latest_frame.apply(
            lambda row: format_metric_value(float(row["goal"]), row["metric"]), axis=1
        )
        latest_frame["delta_display"] = latest_frame["delta_pct"].map(lambda v: f"{v:+.1f}%")
        latest_frame["track"] = latest_frame["on_track"].map(lambda ok: "✅" if ok else "❌")
        st.markdown("#### Current period snapshot")
        st.dataframe(
            latest_frame[
                ["metric_label", "avg_display", "goal_display", "delta_display", "track"]
            ].rename(
                columns={
                    "metric_label": "metric",
                    "avg_display": "average",
                    "goal_display": "goal",
                    "delta_display": "above/below",
                    "track": "status",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )


def _anxiety_stress_tab(df: pd.DataFrame) -> None:
    st.subheader("Anxiety status timeline")
    period = st.selectbox(
        "Period",
        options=["week", "month"],
        index=0,
        key="anxiety_status_period",
    )
    status_counts = build_anxiety_status_counts(df, period=period)
    if status_counts.empty:
        st.info("No anxiety status data available.")
        return

    date_source = pd.DataFrame(
        {
            "date_local": pd.to_datetime(df["date_local"]),
            "status": pd.to_numeric(df.get("anxiety_status_score"), errors="coerce").round(),
        }
    ).dropna(subset=["date_local", "status"])
    date_source["status"] = date_source["status"].astype(int)
    date_source = date_source[date_source["status"].between(1, 5)]
    if period == "week":
        date_source["period_start"] = date_source["date_local"].dt.to_period("W").dt.start_time
    else:
        date_source["period_start"] = date_source["date_local"].dt.to_period("M").dt.start_time

    def _date_hover(series: pd.Series) -> str:
        values = sorted({pd.to_datetime(value).date().isoformat() for value in series})
        if not values:
            return "(none)"
        limited = values[:5]
        text = ", ".join(limited)
        if len(values) > 5:
            text += ", ..."
        return text

    dates_grouped = (
        date_source.groupby(["period_start", "status"], dropna=False)["date_local"]
        .agg(_date_hover)
        .reset_index(name="dates_hover")
    )

    all_statuses = pd.DataFrame({"status": [1, 2, 3, 4, 5]})
    periods = status_counts[["period_start"]].drop_duplicates().assign(_join_key=1)
    statuses = all_statuses.assign(_join_key=1)
    complete = periods.merge(statuses, on="_join_key").drop(columns=["_join_key"])
    status_counts = complete.merge(status_counts, on=["period_start", "status"], how="left")
    status_counts["count"] = status_counts["count"].fillna(0)
    status_counts = status_counts.merge(
        dates_grouped,
        on=["period_start", "status"],
        how="left",
    )
    status_counts["dates_hover"] = status_counts["dates_hover"].fillna("(none)")

    totals = status_counts.groupby("period_start", dropna=False)["count"].transform("sum")
    status_counts["pct"] = np.where(totals > 0, 100.0 * status_counts["count"] / totals, 0.0)

    fig_status = go.Figure()
    for status in [1, 2, 3, 4, 5]:
        subset = status_counts[status_counts["status"] == status].sort_values("period_start")
        fig_status.add_trace(
            go.Bar(
                x=subset["period_start"],
                y=subset["pct"],
                name=f"Status {status}",
                marker_color=ANXIETY_STATUS_COLORS[status],
                customdata=subset[["count", "dates_hover"]].to_numpy(),
                hovertemplate=(
                    "Period: %{x|%Y-%m-%d}<br>"
                    "Status: " + str(status) + "<br>"
                    "Days: %{customdata[0]:.0f}<br>"
                    "Share: %{y:.1f}%<br>"
                    "Dates: %{customdata[1]}<extra></extra>"
                ),
            )
        )

    period_label = "Week" if period == "week" else "Month"
    fig_status.update_layout(
        barmode="stack",
        bargap=0,
        xaxis_title=period_label,
        yaxis_title="Percentage of days (%)",
        yaxis=dict(range=[0, 100]),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        height=560,
    )
    _plotly(fig_status)

    st.markdown("#### Average anxiety over time")
    anxiety_avg = (
        date_source.groupby("period_start", dropna=False)["status"]
        .mean()
        .reset_index(name="avg_anxiety")
    )
    anxiety_avg = anxiety_avg.sort_values("period_start")
    fig_avg = px.line(
        anxiety_avg,
        x="period_start",
        y="avg_anxiety",
        markers=True,
        labels={"period_start": period_label, "avg_anxiety": "Average anxiety"},
    )
    fig_avg.update_layout(height=320, yaxis=dict(range=[1, 5]))
    _plotly(fig_avg)

    st.markdown("#### General notes by date")
    notes_df = pd.DataFrame(
        {
            "date_local": pd.to_datetime(df["date_local"]),
            "general_notes": df.get("general_notes"),
        }
    ).dropna(subset=["date_local"])
    notes_df = notes_df.sort_values("date_local", ascending=False)
    if notes_df.empty:
        st.info("No dates available.")
        return

    options = [d.date() for d in notes_df["date_local"]]
    selected_date = st.selectbox(
        "Pick a date",
        options=options,
        key="anxiety_notes_date",
    )
    selected_row = notes_df[notes_df["date_local"].dt.date == selected_date].iloc[0]
    notes_text = selected_row.get("general_notes")
    if not isinstance(notes_text, str) or not notes_text.strip():
        notes_text = "(empty)"
    st.text_area("General Notes", value=notes_text, height=180, disabled=True)


def _workouts_tab(df: pd.DataFrame) -> None:
    st.subheader("Workouts")
    if df.empty or "workout_count" not in df.columns:
        st.info("Workout data is not available yet.")
        return

    workout_df = df.copy()
    workout_df["date_local"] = pd.to_datetime(workout_df["date_local"])
    workout_df["workout_count"] = pd.to_numeric(
        workout_df["workout_count"], errors="coerce"
    ).fillna(0)
    workout_df = workout_df[workout_df["workout_count"] > 0].copy()
    if workout_df.empty:
        st.info("No workouts logged yet.")
        return

    period = st.selectbox(
        "Granularity",
        options=["day", "week", "month"],
        index=1,
        key="workouts_granularity",
    )

    expanded_rows: list[dict[str, object]] = []
    for row in workout_df.itertuples():
        raw_json = getattr(row, "workout_elements_json", None)
        if isinstance(raw_json, str) and raw_json.strip():
            try:
                parsed = json.loads(raw_json)
            except json.JSONDecodeError:
                parsed = []
        else:
            parsed = []

        if isinstance(parsed, list) and parsed:
            for item in parsed:
                name = item.get("name") if isinstance(item, dict) else None
                w_type = item.get("type") if isinstance(item, dict) else None
                elements = item.get("elements") if isinstance(item, dict) else None
                element_value = None
                if isinstance(elements, dict):
                    element_value = elements.get("elements")
                expanded_rows.append(
                    {
                        "date_local": row.date_local,
                        "workout_name": name,
                        "workout_type": w_type,
                        "elements": element_value,
                    }
                )
        else:
            expanded_rows.append(
                {
                    "date_local": row.date_local,
                    "workout_name": getattr(row, "workout_raw", None),
                    "workout_type": getattr(row, "workout_type", None),
                    "elements": None,
                }
            )

    expanded_df = pd.DataFrame(expanded_rows)
    if expanded_df.empty:
        st.info("No parsed workout entries to visualize.")
        return

    expanded_df["date_local"] = pd.to_datetime(expanded_df["date_local"], errors="coerce")
    expanded_df = expanded_df.dropna(subset=["date_local"])
    expanded_df["workout_type"] = expanded_df["workout_type"].fillna("other")

    global_type_counts = (
        expanded_df["workout_type"]
        .value_counts()
        .rename_axis("workout_type")
        .reset_index(name="count")
    )
    type_order = global_type_counts["workout_type"].tolist()

    st.markdown("#### Workout type mix")
    preferred_colors = {
        "swimming": "#4ea8de",
        "bjj": "#d1495b",
        "strength": "#f77f00",
        "running": "#2a9d8f",
        "cycling": "#f4d35e",
        "hiking": "#90be6d",
        "climbing": "#9b5de5",
        "walk": "#43aa8b",
        "other": "#adb5bd",
        "mixed": "#577590",
    }
    palette_fallback = [
        "#4ea8de",
        "#2a9d8f",
        "#f4d35e",
        "#f77f00",
        "#d1495b",
        "#90be6d",
        "#43aa8b",
        "#577590",
        "#9b5de5",
        "#adb5bd",
    ]
    color_map: dict[str, str] = {}
    fallback_idx = 0
    for workout_type in type_order:
        if workout_type in preferred_colors:
            color_map[workout_type] = preferred_colors[workout_type]
        else:
            color_map[workout_type] = palette_fallback[fallback_idx % len(palette_fallback)]
            fallback_idx += 1

    if period == "day":
        expanded_df["period_start"] = expanded_df["date_local"].dt.normalize()
    elif period == "week":
        expanded_df["period_start"] = expanded_df["date_local"].dt.to_period("W").dt.start_time
    else:
        expanded_df["period_start"] = expanded_df["date_local"].dt.to_period("M").dt.to_timestamp()

    volume_by_type = (
        expanded_df.groupby(["period_start", "workout_type"], dropna=False)
        .size()
        .reset_index(name="workout_count")
        .sort_values("period_start")
    )
    fig_volume = px.bar(
        volume_by_type,
        x="period_start",
        y="workout_count",
        color="workout_type",
        barmode="stack",
        color_discrete_map=color_map,
        category_orders={"workout_type": type_order},
        labels={
            "period_start": "Period",
            "workout_count": "Workout entries",
            "workout_type": "Workout type",
        },
    )
    fig_volume.update_layout(height=320)
    _plotly(fig_volume)

    donut_years = sorted(expanded_df["date_local"].dt.year.dropna().astype(int).unique().tolist())
    years_to_show = donut_years[-5:]
    if not years_to_show:
        st.info("No workouts available for yearly donut charts.")
    else:
        donut_cols = st.columns(len(years_to_show))
        for idx, year in enumerate(years_to_show):
            year_counts = (
                expanded_df[expanded_df["date_local"].dt.year == year]["workout_type"]
                .value_counts()
                .rename_axis("workout_type")
                .reset_index(name="count")
            )
            if year_counts.empty:
                with donut_cols[idx]:
                    st.info(f"{year}: no data")
                continue

            fig_types = px.pie(
                year_counts,
                names="workout_type",
                values="count",
                hole=0.4,
                color="workout_type",
                color_discrete_map=color_map,
                category_orders={"workout_type": type_order},
                title=str(year),
            )
            fig_types.update_layout(
                height=320,
                margin=dict(l=8, r=8, t=40, b=8),
                showlegend=False,
            )
            with donut_cols[idx]:
                _plotly(fig_types)

    st.markdown("#### Workout log")
    log = expanded_df.sort_values("date_local", ascending=False).copy()
    log["date_local"] = pd.to_datetime(log["date_local"]).dt.date
    st.dataframe(log, use_container_width=True, hide_index=True)


def _bayes_regression_tab(df: pd.DataFrame, metrics: list[str]) -> None:
    st.subheader("Bayesian multiple regression")
    if df.empty or not metrics:
        st.info("No numeric data available for Bayesian regression.")
        return

    if len(metrics) < 2:
        st.info("Need at least two numeric metrics (target + one explanatory variable).")
        return

    target_default = (
        metrics.index("anxiety_status_score") if "anxiety_status_score" in metrics else 0
    )
    target = st.selectbox(
        "Target variable",
        options=metrics,
        index=target_default,
        key="bayes_target",
    )

    dated_df = df.copy()
    dated_df["date_local"] = pd.to_datetime(dated_df["date_local"], errors="coerce")
    dated_df = dated_df.dropna(subset=["date_local"])
    if dated_df.empty:
        st.info("No dated rows available for regression.")
        return

    min_date = dated_df["date_local"].min().date()
    max_date = dated_df["date_local"].max().date()
    d1, d2 = st.columns(2)
    start_date = d1.date_input(
        "Start date",
        value=min_date,
        min_value=min_date,
        max_value=max_date,
        key="bayes_start_date",
    )
    end_date = d2.date_input(
        "End date",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
        key="bayes_end_date",
    )
    if start_date > end_date:
        st.warning("Start date must be on or before end date.")
        return

    filtered = dated_df[
        (dated_df["date_local"].dt.date >= start_date)
        & (dated_df["date_local"].dt.date <= end_date)
    ].copy()

    st.markdown("#### Explanatory variables (6 slots)")
    st.caption(
        "Lag direction: positive lag uses past predictor values (t-lag) to explain today's target; "
        "negative lag uses future values."
    )

    explanatory_options = [m for m in metrics if m != target]
    slot_defaults: list[str] = []
    for candidate in [
        "sleep_total_hours",
        "steps",
        "readiness_score",
        "sleep_score",
        "activity_score",
        "sleep_efficiency_pct",
    ]:
        if candidate in explanatory_options and candidate not in slot_defaults:
            slot_defaults.append(candidate)
    slot_defaults = slot_defaults[:6]

    selected_features: list[str] = []
    lag_by_feature: dict[str, int] = {}
    duplicate_features: set[str] = set()
    for slot in range(6):
        options = ["(none)", *explanatory_options]
        default_feature = slot_defaults[slot] if slot < len(slot_defaults) else "(none)"
        default_index = options.index(default_feature) if default_feature in options else 0
        c1, c2 = st.columns([4, 2])
        feature = c1.selectbox(
            f"Variable {slot + 1}",
            options=options,
            index=default_index,
            key=f"bayes_feature_slot_{slot}",
        )
        lag = int(
            c2.number_input(
                f"Lag {slot + 1} (days)",
                min_value=-14,
                max_value=14,
                value=0,
                step=1,
                key=f"bayes_lag_slot_{slot}",
            )
        )
        if feature != "(none)":
            if feature in selected_features:
                duplicate_features.add(feature)
            else:
                selected_features.append(feature)
            lag_by_feature[feature] = lag

    if duplicate_features:
        dup_text = ", ".join(sorted(duplicate_features))
        st.warning(f"Duplicate variables ignored: {dup_text}")

    c1, c2, c3 = st.columns(3)
    normalize = c1.checkbox("Normalize variables", value=True, key="bayes_normalize")
    chains = c2.slider("Chains", min_value=2, max_value=4, value=4, key="bayes_chains")
    samples = c3.slider(
        "Samples / chain",
        min_value=500,
        max_value=2000,
        value=1000,
        step=250,
        key="bayes_samples",
    )

    rows_in_range = len(filtered)
    rows_used_text = "n/a"
    preview_error: str | None = None
    if selected_features:
        try:
            adapter = CmdStanInferenceAdapter()
            preview_frame, _, _, _ = adapter.build_design_matrix(
                df=filtered,
                target=target,
                features=selected_features,
                lag_by_feature=lag_by_feature,
                normalize=normalize,
            )
            rows_used_text = f"{len(preview_frame):,}"
        except Exception as exc:
            preview_error = str(exc)

    st.markdown("#### Input sample overview")
    s1, s2, s3 = st.columns(3)
    s1.metric("Rows in date range", f"{rows_in_range:,}")
    s2.metric("Rows used after lag/NA", rows_used_text)
    s3.metric("Predictors selected", str(len(selected_features)))
    if preview_error is not None:
        st.caption(f"Design-matrix preview warning: {preview_error}")

    run_clicked = st.button("Run cmdstan regression", key="bayes_run")
    if run_clicked:
        if not selected_features:
            st.warning("Choose at least one explanatory variable.")
        else:
            progress = st.progress(5, text="Preparing regression inputs...")
            try:
                adapter = CmdStanInferenceAdapter()
                progress.progress(20, text="Building design matrix...")
                progress.progress(35, text="Running CmdStan sampling...")
                result = adapter.run(
                    df=filtered,
                    target=target,
                    features=selected_features,
                    lag_by_feature=lag_by_feature,
                    normalize=normalize,
                    chains=chains,
                    iter_sampling=samples,
                    iter_warmup=samples,
                )
            except ModuleNotFoundError:
                progress.empty()
                st.error(
                    "cmdstanpy is not installed. "
                    "Run `uv sync --extra inference` and ensure CmdStan is installed."
                )
            except Exception as exc:
                progress.empty()
                st.error(f"Regression failed: {exc}")
            else:
                progress.progress(100, text="Regression complete")
                st.session_state["bayes_result"] = result

    result = st.session_state.get("bayes_result")
    if result is None:
        st.caption("Configure variables and click `Run cmdstan regression`.")
        return

    st.markdown("#### MCMC diagnostics")
    diag = result.diagnostics
    d1, d2, d3, d4 = st.columns(4)
    max_rhat = diag.get("max_r_hat")
    min_ess = diag.get("min_ess_bulk")
    d1.metric("Max R-hat", "n/a" if pd.isna(max_rhat) else f"{float(max_rhat):.3f}")
    d2.metric("Min ESS bulk", "n/a" if pd.isna(min_ess) else f"{float(min_ess):.0f}")
    d3.metric("Divergences", f"{int(diag.get('divergent_transitions', 0))}")
    d4.metric("MCMC quality", str(diag.get("mcmc_quality", "n/a")))

    st.markdown("#### CmdStan run output (tail)")
    st.text_area("Sampling logs", value=result.stdout_tail, height=240, disabled=True)

    st.markdown("#### Coefficients")
    coef_df = result.coefficients.copy()
    draws_df = result.coefficient_draws.copy()
    if coef_df.empty or draws_df.empty:
        st.info("No coefficients available.")
    else:
        fig_coef = go.Figure()
        term_order = list(coef_df["term"])
        for term in term_order:
            term_draws = draws_df[draws_df["term"] == term]["value"]
            if term_draws.empty:
                continue
            fig_coef.add_trace(
                go.Violin(
                    x=term_draws,
                    y=[term] * len(term_draws),
                    orientation="h",
                    side="positive",
                    line_color="#4ea8de",
                    fillcolor="rgba(78, 168, 222, 0.45)",
                    width=0.75,
                    points=False,
                    showlegend=False,
                    meanline_visible=False,
                    hovertemplate="term: %{y}<br>draw: %{x:.3f}<extra></extra>",
                )
            )

        for row in coef_df.itertuples():
            fig_coef.add_trace(
                go.Scatter(
                    x=[row.q5, row.q95],
                    y=[row.term, row.term],
                    mode="lines",
                    line=dict(color="#f4d35e", width=3),
                    showlegend=False,
                    hoverinfo="skip",
                )
            )
            fig_coef.add_trace(
                go.Scatter(
                    x=[row.mean],
                    y=[row.term],
                    mode="markers",
                    marker=dict(color="#f4d35e", size=9),
                    name="Mean / 5-95%",
                    showlegend=False,
                    hovertemplate="term: %{y}<br>mean: %{x:.3f}<extra></extra>",
                )
            )

        fig_coef.add_vline(x=0, line_dash="dash", line_width=1, line_color="#f1f3f5")
        fig_coef.update_layout(
            height=max(360, 90 + 55 * max(len(term_order), 1)),
            xaxis_title="Coefficient",
            yaxis_title="Term",
            violinmode="overlay",
        )
        _plotly(fig_coef)

    st.markdown("#### Posterior predictive check")
    ppc = result.ppc.copy().sort_values("date_local")
    fig_ppc = go.Figure()
    fig_ppc.add_trace(
        go.Scatter(
            x=ppc["date_local"],
            y=ppc["y_rep_q5"],
            mode="lines",
            line=dict(width=0),
            showlegend=False,
            hoverinfo="skip",
        )
    )
    fig_ppc.add_trace(
        go.Scatter(
            x=ppc["date_local"],
            y=ppc["y_rep_q95"],
            mode="lines",
            fill="tonexty",
            fillcolor="rgba(78, 168, 222, 0.2)",
            line=dict(width=0),
            name="Posterior 5-95%",
        )
    )
    fig_ppc.add_trace(
        go.Scatter(
            x=ppc["date_local"],
            y=ppc["y_rep_q50"],
            mode="lines",
            line=dict(color="#4ea8de", width=2),
            name="Posterior median",
        )
    )
    fig_ppc.add_trace(
        go.Scatter(
            x=ppc["date_local"],
            y=ppc["y_actual"],
            mode="markers",
            marker=dict(color="#f4d35e", size=7),
            name="Observed",
        )
    )
    fig_ppc.update_layout(height=420, xaxis_title="Date", yaxis_title=target)
    _plotly(fig_ppc)

    st.markdown("#### Draw summary")
    st.dataframe(
        coef_df[["term", "mean", "q5", "q50", "q95", "r_hat", "ess_bulk"]],
        use_container_width=True,
        hide_index=True,
    )


def _feature_clusters_tab(df: pd.DataFrame, metrics: list[str]) -> None:
    st.subheader("Feature clusters")
    if df.empty or not metrics:
        st.info("No numeric data available for clustering.")
        return

    default_features = [
        "sleep_avg_hr",
        "sleep_avg_hrv",
        "sleep_temperature_deviation",
        "sleep_total_hours",
        "sleep_efficiency_pct",
        "sleep_deep_share_pct",
        "sleep_rem_share_pct",
        "steps",
        "readiness_score",
        "daytime_stress_avg",
    ]
    selected_defaults = [feature for feature in default_features if feature in metrics]
    features = st.multiselect(
        "Features",
        options=metrics,
        default=selected_defaults,
        key="cluster_features",
    )

    c1, c2 = st.columns(2)
    k = c1.slider("Number of clusters", min_value=2, max_value=8, value=4, key="cluster_k")
    granularity = c2.selectbox(
        "Timeline granularity",
        options=["day", "week"],
        index=0,
        key="cluster_granularity",
    )

    run_cluster = st.button("Run clustering", key="run_feature_clustering")
    if run_cluster:
        if len(features) < 2:
            st.warning("Select at least two features for clustering.")
            return

        try:
            from sklearn.cluster import KMeans
            from sklearn.decomposition import PCA
            from sklearn.preprocessing import StandardScaler
        except ModuleNotFoundError:
            st.error("scikit-learn is missing. Install dependencies with `uv sync`.")
            return

        work = pd.DataFrame({"date_local": pd.to_datetime(df["date_local"], errors="coerce")})
        for feature in features:
            work[feature] = pd.to_numeric(df[feature], errors="coerce")
        work = work.dropna(subset=["date_local", *features]).sort_values("date_local")
        if len(work) < k:
            st.warning(
                f"Need at least {k} complete rows for {k} clusters. "
                f"Current complete rows: {len(work)}."
            )
            return

        scaler = StandardScaler()
        x_scaled = scaler.fit_transform(work[features])

        model = KMeans(n_clusters=k, random_state=42, n_init=20)
        labels = model.fit_predict(x_scaled)

        pca_components = 2 if x_scaled.shape[1] >= 2 else 1
        pca = PCA(n_components=pca_components)
        pcs = pca.fit_transform(x_scaled)

        clustered = work[["date_local"]].copy()
        clustered["cluster_id"] = labels.astype(int)
        clustered["cluster"] = clustered["cluster_id"].map(lambda val: f"Cluster {val}")
        clustered["pc1"] = pcs[:, 0]
        clustered["pc2"] = pcs[:, 1] if pca_components > 1 else 0.0

        profile = pd.DataFrame(x_scaled, columns=features)
        profile["cluster"] = clustered["cluster"].to_numpy()
        profile = profile.groupby("cluster", dropna=False)[features].mean().reset_index()

        st.session_state["cluster_result"] = {
            "clustered": clustered,
            "profile": profile,
            "features": features,
            "explained_var": pca.explained_variance_ratio_.tolist(),
            "k": k,
        }

    result = st.session_state.get("cluster_result")
    if not result:
        st.caption("Choose features and run clustering.")
        return

    clustered = result["clustered"]
    profile = result["profile"]
    explained_var = result["explained_var"]

    cluster_ids = sorted(pd.to_numeric(clustered["cluster_id"], errors="coerce").dropna().unique())
    cluster_order = [f"Cluster {int(cluster_id)}" for cluster_id in cluster_ids]
    cluster_palette = px.colors.qualitative.Safe + px.colors.qualitative.Set2
    cluster_color_map = {
        cluster_name: cluster_palette[idx % len(cluster_palette)]
        for idx, cluster_name in enumerate(cluster_order)
    }

    st.caption(
        "Features are standardized (z-scores) before clustering; "
        "profile values are cluster mean z-scores."
    )
    c1, c2, c3 = st.columns(3)
    c1.metric("Rows clustered", f"{len(clustered):,}")
    c2.metric("Clusters", str(result["k"]))
    c3.metric("PCA variance (2D)", f"{100.0 * sum(explained_var[:2]):.1f}%")

    if granularity == "day":
        daily_mix = (
            clustered.groupby(["date_local", "cluster"], dropna=False)
            .size()
            .reset_index(name="count")
        )
        totals = daily_mix.groupby("date_local", dropna=False)["count"].transform("sum")
        daily_mix["pct"] = np.where(totals > 0, 100.0 * daily_mix["count"] / totals, 0.0)
        fig_timeline = px.bar(
            daily_mix,
            x="date_local",
            y="pct",
            color="cluster",
            barmode="stack",
            color_discrete_map=cluster_color_map,
            category_orders={"cluster": cluster_order},
            labels={"date_local": "Date", "pct": "Share of rows (%)"},
        )
        fig_timeline.update_layout(height=340, yaxis=dict(range=[0, 100]), bargap=0)
        _plotly(fig_timeline)
    else:
        weekly = clustered.copy()
        weekly["period_start"] = weekly["date_local"].dt.to_period("W").dt.start_time
        weekly_mix = (
            weekly.groupby(["period_start", "cluster"], dropna=False)
            .size()
            .reset_index(name="count")
        )
        totals = weekly_mix.groupby("period_start", dropna=False)["count"].transform("sum")
        weekly_mix["pct"] = np.where(totals > 0, 100.0 * weekly_mix["count"] / totals, 0.0)
        fig_weekly = px.bar(
            weekly_mix,
            x="period_start",
            y="pct",
            color="cluster",
            color_discrete_map=cluster_color_map,
            category_orders={"cluster": cluster_order},
            barmode="stack",
            labels={"period_start": "Week", "pct": "Share of days (%)"},
        )
        fig_weekly.update_layout(height=340, yaxis=dict(range=[0, 100]))
        _plotly(fig_weekly)

    fig_pca = px.scatter(
        clustered,
        x="pc1",
        y="pc2",
        color="cluster",
        color_discrete_map=cluster_color_map,
        category_orders={"cluster": cluster_order},
        hover_data={"date_local": True, "cluster_id": True},
        labels={"pc1": "PC1", "pc2": "PC2"},
    )
    fig_pca.update_layout(height=360)
    _plotly(fig_pca)

    profile_long = profile.melt(id_vars="cluster", var_name="feature", value_name="z_score")
    fig_profile = px.bar(
        profile_long,
        x="feature",
        y="z_score",
        color="cluster",
        color_discrete_map=cluster_color_map,
        category_orders={"cluster": cluster_order},
        barmode="group",
        labels={"feature": "Feature", "z_score": "Mean z-score"},
    )
    fig_profile.add_hline(y=0, line_dash="dash", line_color="#cbd5e1")
    fig_profile.update_layout(height=360)
    _plotly(fig_profile)


def _dynamic_bayes_example_tab(df: pd.DataFrame, metrics: list[str]) -> None:
    st.subheader("Dynamic Bayes (example)")
    st.caption(
        "Rolling-window Bayesian regression example: coefficients are re-estimated over time "
        "to show changing relationships."
    )

    if df.empty or len(metrics) < 2:
        st.info("Need at least two numeric metrics.")
        return

    target_default = (
        metrics.index("anxiety_status_score") if "anxiety_status_score" in metrics else 0
    )
    target = st.selectbox(
        "Target",
        options=metrics,
        index=target_default,
        key="dynamic_bayes_target",
    )

    predictor_options = [m for m in metrics if m != target]
    defaults = [
        m for m in ["sleep_total_hours", "steps", "readiness_score"] if m in predictor_options
    ]
    predictors = st.multiselect(
        "Predictors",
        options=predictor_options,
        default=defaults,
        key="dynamic_bayes_predictors",
    )

    c1, c2 = st.columns(2)
    window = c1.slider("Rolling window (days)", min_value=21, max_value=120, value=45, step=3)
    min_rows = c2.slider("Minimum rows per fit", min_value=15, max_value=80, value=25, step=1)

    run = st.button("Run dynamic example", key="dynamic_bayes_run")
    if not run:
        st.caption("Select variables and click `Run dynamic example`.")
        return

    if len(predictors) == 0:
        st.warning("Pick at least one predictor.")
        return

    try:
        from sklearn.linear_model import BayesianRidge
        from sklearn.preprocessing import StandardScaler
    except ModuleNotFoundError:
        st.error("scikit-learn is missing. Run `uv sync`.")
        return

    work = pd.DataFrame({"date_local": pd.to_datetime(df["date_local"], errors="coerce")})
    work[target] = pd.to_numeric(df[target], errors="coerce")
    for pred in predictors:
        work[pred] = pd.to_numeric(df[pred], errors="coerce")
    work = work.dropna(subset=["date_local", target, *predictors]).sort_values("date_local")
    if len(work) < max(min_rows, window):
        st.info("Not enough complete rows for selected window/minimum settings.")
        return

    rows: list[dict[str, object]] = []
    for idx in range(window - 1, len(work)):
        slice_df = work.iloc[idx - window + 1 : idx + 1].copy()
        if len(slice_df) < min_rows:
            continue

        x = slice_df[predictors].to_numpy(dtype=float)
        y = slice_df[target].to_numpy(dtype=float)

        scaler = StandardScaler()
        x_scaled = scaler.fit_transform(x)

        model = BayesianRidge()
        model.fit(x_scaled, y)

        date_point = slice_df.iloc[-1]["date_local"]
        for pred_idx, pred in enumerate(predictors):
            rows.append(
                {
                    "date_local": date_point,
                    "term": pred,
                    "coef": float(model.coef_[pred_idx]),
                }
            )

    coef_df = pd.DataFrame(rows)
    if coef_df.empty:
        st.info("No rolling fits were produced.")
        return

    fig_coef = px.line(
        coef_df,
        x="date_local",
        y="coef",
        color="term",
        labels={"date_local": "Date", "coef": "Coefficient", "term": "Predictor"},
    )
    fig_coef.add_hline(y=0, line_dash="dash", line_color="#cbd5e1")
    fig_coef.update_layout(height=400)
    _plotly(fig_coef)

    latest = (
        coef_df.sort_values("date_local")
        .groupby("term", as_index=False)
        .tail(1)
        .sort_values("coef", key=lambda s: s.abs(), ascending=False)
    )
    st.markdown("#### Latest coefficients")
    st.dataframe(latest[["term", "coef", "date_local"]], use_container_width=True, hide_index=True)


def _sync_and_source_stats_tab(db_path: Path, settings, runtime) -> None:
    st.subheader("Sync + source statistics")
    st.caption("Refresh Notion/Oura data and inspect descriptive stats for source tables.")

    refresh_message = st.session_state.pop("sync_refresh_message", None)
    if isinstance(refresh_message, str) and refresh_message:
        st.success(refresh_message)

    action_cols = st.columns(2)
    run_refresh_notion = action_cols[0].button("Refresh from Notion", key="refresh_notion_button")
    run_refresh_oura = action_cols[1].button("Refresh from Oura", key="refresh_oura_button")

    if run_refresh_notion:
        if not settings.notion_token or not settings.notion_database_id:
            st.error("Missing NOTION_TOKEN or NOTION_DATABASE_ID in environment.")
        else:
            storage: DuckDBStorage | None = None
            try:
                storage = DuckDBStorage(db_path)
                notion_connector = NotionConnector(
                    settings.notion_token,
                    settings.notion_database_id,
                )
                today = date.today()
                lookback_days = runtime.incremental.lookback_days
                fallback_days = runtime.incremental.fallback_days
                notion_max = storage.get_sync_state(SourceName.NOTION.value, "daily_logs")
                notion_start = (
                    notion_max - timedelta(days=lookback_days)
                    if notion_max
                    else (today - timedelta(days=fallback_days))
                )

                with st.spinner("Syncing Notion and rebuilding daily features..."):
                    notion_count = run_notion_sync(storage, notion_connector, notion_start, today)
                    finalize_features(storage)

                st.session_state["sync_refresh_message"] = (
                    "Notion refresh complete. "
                    f"Rows synced: {notion_count}. Window: {notion_start} to {today}."
                )
                st.rerun()
            except Exception as exc:
                st.error(f"Notion refresh failed: {exc}")
            finally:
                if storage is not None:
                    storage.conn.close()

    if run_refresh_oura:
        token_store = OuraTokenStore(settings.oura_token_store_path)
        access_token = settings.oura_access_token or token_store.get_access_token()

        if (
            not access_token
            and settings.oura_auto_refresh
            and (settings.oura_refresh_token or token_store.get_refresh_token())
            and settings.oura_client_id
            and settings.oura_client_secret
            and settings.oura_redirect_uri
        ):
            try:
                oauth = OuraOAuthClient(
                    authorize_url=settings.oura_oauth_authorize_url,
                    token_url=settings.oura_oauth_token_url,
                    revoke_url=settings.oura_oauth_revoke_url,
                    client_id=settings.oura_client_id,
                    client_secret=settings.oura_client_secret,
                    redirect_uri=settings.oura_redirect_uri,
                    scope=settings.oura_scopes,
                )
                refresh_token = settings.oura_refresh_token or token_store.get_refresh_token()
                if refresh_token:
                    token_data = oauth.refresh_token(refresh_token)
                    access_token = token_data.get("access_token")
                    if access_token:
                        token_store.set_tokens(
                            access_token=access_token,
                            refresh_token=token_data.get("refresh_token"),
                            expires_in=token_data.get("expires_in"),
                            token_type=token_data.get("token_type"),
                            scope=token_data.get("scope"),
                        )
            except Exception as exc:
                st.error(f"Oura token refresh failed: {exc}")
                access_token = None

        if not access_token:
            st.error(
                "Missing Oura access token. Set OURA_ACCESS_TOKEN, or configure token store/oauth "
                "refresh vars like CLI sync."
            )
        else:
            storage = None
            try:
                storage = DuckDBStorage(db_path)
                oura_connector = OuraConnector(access_token, settings.oura_base_url)
                today = date.today()
                lookback_days = runtime.incremental.lookback_days
                fallback_days = runtime.incremental.fallback_days

                oura_max_candidates: list[date] = []
                for endpoint in runtime.oura.endpoints:
                    current = storage.get_sync_state(SourceName.OURA.value, endpoint.value)
                    if current:
                        oura_max_candidates.append(current)
                max_oura = max(oura_max_candidates) if oura_max_candidates else None
                oura_start = (
                    max_oura - timedelta(days=lookback_days)
                    if max_oura
                    else (today - timedelta(days=fallback_days))
                )

                with st.spinner("Syncing Oura and rebuilding daily features..."):
                    oura_count = run_oura_sync(
                        storage,
                        oura_connector,
                        oura_start,
                        today,
                        runtime.oura.endpoints,
                    )
                    finalize_features(storage)

                st.session_state["sync_refresh_message"] = (
                    "Oura refresh complete. "
                    f"Rows synced: {oura_count}. Window: {oura_start} to {today}."
                )
                st.rerun()
            except Exception as exc:
                st.error(f"Oura refresh failed: {exc}")
            finally:
                if storage is not None:
                    storage.conn.close()

    storage: DuckDBStorage | None = None
    try:
        storage = DuckDBStorage(db_path)

        def _render_source_stats(
            table_name: str,
            title: str,
            preferred_numeric: list[str],
        ) -> None:
            st.markdown(f"#### {title}")
            frame = storage.conn.execute(
                f"select * from {table_name} order by date_local"
            ).fetchdf()
            if frame.empty:
                st.info("No rows available.")
                return

            deprecated_notion_attrs = {
                "points",
                "coffee_count",
                "cigarettes_count",
                "sleep_hours_self_reported",
                "productivity_score",
                "supplements",
                "weight_kg",
                "learned",
            }
            if table_name == "canonical_notion_daily":
                drop_cols = [c for c in deprecated_notion_attrs if c in frame.columns]
                if drop_cols:
                    frame = frame.drop(columns=drop_cols)

            frame["date_local"] = pd.to_datetime(frame["date_local"], errors="coerce")
            rows = len(frame)
            days = int(frame["date_local"].dropna().nunique())
            first_day = frame["date_local"].min()
            last_day = frame["date_local"].max()
            span_days = int((last_day - first_day).days + 1) if pd.notna(first_day) else 0
            coverage = (100.0 * days / span_days) if span_days > 0 else 0.0
            days_since_last = (
                int((pd.Timestamp.today().normalize() - last_day.normalize()).days)
                if pd.notna(last_day)
                else -1
            )

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Rows", f"{rows:,}")
            c2.metric("Distinct days", f"{days:,}")
            c3.metric("Coverage", f"{coverage:.1f}%")
            c4.metric("First day", "n/a" if pd.isna(first_day) else str(first_day.date()))
            c5.metric("Days since last", "n/a" if days_since_last < 0 else str(days_since_last))

            numeric = frame.select_dtypes(include=["number"]).copy()
            for column in preferred_numeric:
                if column in frame.columns and column not in numeric.columns:
                    series = pd.to_numeric(frame[column], errors="coerce")
                    if series.notna().any():
                        numeric[column] = series

            if not numeric.empty:
                describe = numeric.describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9]).transpose()
                describe = describe.reset_index().rename(columns={"index": "metric"})
                st.markdown("Descriptive statistics")
                st.dataframe(describe, use_container_width=True, hide_index=True)

            missing = pd.DataFrame(
                {
                    "column": frame.columns,
                    "missing_pct": [100.0 * frame[col].isna().mean() for col in frame.columns],
                    "non_null_rows": [int(frame[col].notna().sum()) for col in frame.columns],
                }
            ).sort_values("missing_pct", ascending=False)
            st.markdown("Missingness by column")
            st.dataframe(missing, use_container_width=True, hide_index=True)

        _render_source_stats(
            table_name="canonical_notion_daily",
            title="Notion",
            preferred_numeric=[
                "anxiety_status_score",
                "physical_status_score",
                "alcohol_units",
                "mindful_min",
                "workout_count",
            ],
        )
        _render_source_stats(
            table_name="canonical_oura_daily",
            title="Oura",
            preferred_numeric=[
                "readiness_score",
                "sleep_score",
                "activity_score",
                "steps",
                "active_calories",
                "sleep_avg_hrv",
                "daytime_stress_avg",
            ],
        )
    finally:
        if storage is not None:
            storage.conn.close()


def main() -> None:
    settings = load_settings()
    runtime = load_runtime_config(settings.pipeline_config_path)
    goals_cfg = runtime.goals.model_dump()
    st.set_page_config(page_title="Life dashboard", page_icon=":bar_chart:", layout="wide")

    with st.sidebar:
        st.title("Life dashboard")
        db_path_str = st.text_input("DuckDB path", value=str(settings.duckdb_path))
        theme_name = "Deep Ocean"
        st.caption("Theme: Deep Ocean")

    selected_theme = THEME_PRESETS[theme_name]
    _apply_plotly_theme(theme_name, selected_theme)
    st.markdown(_theme_css(selected_theme), unsafe_allow_html=True)

    df = load_daily_frame(Path(db_path_str))
    metrics = metric_columns(df)

    tabs = st.tabs(
        [
            "Goals",
            "Sleep + Recovery",
            "Workouts",
            "Anxiety & Stress",
            "Explore",
            "Trends",
            "Correlations & Lags",
            "Feature Clusters",
            "Dynamic Bayes Example",
            "Bayes Regression",
            "Leaderboard",
            "Period Summary",
            "Data Quality",
            "Sync & Source Stats",
        ]
    )

    with tabs[0]:
        _goals_tab(df, goals_cfg)
    with tabs[1]:
        _sleep_tab(df)
    with tabs[2]:
        _workouts_tab(df)
    with tabs[3]:
        _anxiety_stress_tab(df)
    with tabs[4]:
        _bubble_tab(df, metrics)
    with tabs[5]:
        _trends_tab(df, metrics)
    with tabs[6]:
        _corr_tab(df, metrics)
    with tabs[7]:
        _feature_clusters_tab(df, metrics)
    with tabs[8]:
        _dynamic_bayes_example_tab(df, metrics)
    with tabs[9]:
        _bayes_regression_tab(df, metrics)
    with tabs[10]:
        _leaderboard_tab(df)
    with tabs[11]:
        _period_summary_tab(df, metrics)
    with tabs[12]:
        _quality_tab(df, metrics)
    with tabs[13]:
        _sync_and_source_stats_tab(Path(db_path_str), settings, runtime)


if __name__ == "__main__":
    main()
