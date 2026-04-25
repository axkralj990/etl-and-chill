from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from life.config import load_settings
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
from life.pipeline.runtime_config import load_runtime_config


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


def _leaderboard_tab(df: pd.DataFrame) -> None:
    st.subheader("All-time leaderboard")
    leaderboard = build_leaderboard(df)
    if leaderboard.empty:
        st.info("Not enough data for leaderboard records yet.")
        return

    display = leaderboard.copy()
    display["date_local"] = display["date_local"].dt.date
    st.dataframe(
        display[["metric_label", "record_label", "value_display", "date_local"]].rename(
            columns={"value_display": "value"}
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("#### Record details")
    choices = [
        f"{row.metric_label} - {row.record_label} ({row.date_local.date()})"
        for row in leaderboard.itertuples()
    ]
    selected_label = st.selectbox("Pick a record", choices)
    selected = leaderboard.iloc[choices.index(selected_label)]

    st.write(f"Date: {selected['date_local'].date()}")
    notes = selected.get("general_notes")
    if isinstance(notes, str) and notes.strip():
        st.write(f"General Notes: {notes.strip()}")
    else:
        st.write("General Notes: (empty)")


def _goals_tab(df: pd.DataFrame, goals_cfg: dict[str, float]) -> None:
    st.subheader("Goals")
    if df.empty:
        st.info("No data for goals yet.")
        return

    period = st.selectbox("Period", options=["week", "month"], index=0)
    progress = build_goals_progress(df, period=period, goals=goals_cfg)
    if progress.empty:
        st.info("No goal-compatible metrics available for selected period.")
    else:
        period_label = progress.iloc[0]["period_label"]
        period_start = pd.to_datetime(progress.iloc[0]["period_start"])
        period_end = pd.to_datetime(progress.iloc[0]["period_end"])
        today = pd.Timestamp.now().normalize()
        elapsed_days = int((min(today, period_end) - period_start).days + 1)
        total_days = int((period_end - period_start).days + 1)
        st.caption(f"Current period: {period_label}. Missing days are ignored.")
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
        st.plotly_chart(fig_attain, use_container_width=True)

        st.markdown("#### Actual vs goal markers")
        marker_df = progress.sort_values("metric_label").copy()
        fig_markers = go.Figure()
        for row in marker_df.itertuples():
            fig_markers.add_trace(
                go.Scatter(
                    x=[row.goal, row.avg_value],
                    y=[row.metric_label, row.metric_label],
                    mode="lines",
                    line=dict(color="#aab2bf", width=3),
                    showlegend=False,
                    hoverinfo="skip",
                )
            )
        fig_markers.add_trace(
            go.Scatter(
                x=marker_df["goal"],
                y=marker_df["metric_label"],
                mode="markers",
                marker=dict(color="#f4d35e", size=11, symbol="diamond"),
                name="Goal",
            )
        )
        fig_markers.add_trace(
            go.Scatter(
                x=marker_df["avg_value"],
                y=marker_df["metric_label"],
                mode="markers",
                marker=dict(
                    color=np.where(marker_df["on_track"], "#3da35d", "#d1495b"),
                    size=12,
                    symbol="circle",
                ),
                name="Actual",
            )
        )
        fig_markers.update_layout(
            height=360,
            xaxis_title="Value",
            yaxis_title="Metric",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_markers, use_container_width=True)

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
        latest_frame["track"] = latest_frame["on_track"].map(
            lambda ok: "On track" if ok else "Off track"
        )
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
    st.plotly_chart(fig_status, use_container_width=True)

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

    if period == "day":
        volume = workout_df.groupby("date_local", dropna=False)["workout_count"].sum().reset_index()
        volume = volume.rename(columns={"date_local": "period_start"})
    elif period == "week":
        workout_df["period_start"] = pd.to_datetime(workout_df["week_start_monday"])
        volume = (
            workout_df.groupby("period_start", dropna=False)["workout_count"].sum().reset_index()
        )
    else:
        workout_df["period_start"] = workout_df["date_local"].dt.to_period("M").dt.to_timestamp()
        volume = (
            workout_df.groupby("period_start", dropna=False)["workout_count"].sum().reset_index()
        )

    fig_volume = px.bar(
        volume.sort_values("period_start"),
        x="period_start",
        y="workout_count",
        labels={"period_start": "Period", "workout_count": "Workout entries"},
        color_discrete_sequence=["#3da35d"],
    )
    fig_volume.update_layout(height=320)
    st.plotly_chart(fig_volume, use_container_width=True)

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

    st.markdown("#### Workout type mix")
    type_counts = (
        expanded_df["workout_type"]
        .fillna("other")
        .value_counts()
        .rename_axis("workout_type")
        .reset_index(name="count")
    )
    fig_types = px.pie(type_counts, names="workout_type", values="count", hole=0.4)
    fig_types.update_layout(height=360)
    st.plotly_chart(fig_types, use_container_width=True)

    st.markdown("#### Distance / elements trends")
    elements_df = expanded_df.dropna(subset=["elements"]).copy()
    if not elements_df.empty:
        elements_df["elements"] = pd.to_numeric(elements_df["elements"], errors="coerce")
        elements_df = elements_df.dropna(subset=["elements"])
        if not elements_df.empty:
            fig_elements = px.scatter(
                elements_df,
                x="date_local",
                y="elements",
                color="workout_type",
                hover_data=["workout_name"],
                labels={"elements": "Elements"},
            )
            fig_elements.update_layout(height=360)
            st.plotly_chart(fig_elements, use_container_width=True)

    st.markdown("#### Workout log")
    log = expanded_df.sort_values("date_local", ascending=False).copy()
    log["date_local"] = pd.to_datetime(log["date_local"]).dt.date
    st.dataframe(log, use_container_width=True, hide_index=True)


def main() -> None:
    settings = load_settings()
    runtime = load_runtime_config(settings.pipeline_config_path)
    goals_cfg = runtime.goals.model_dump()
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
            "Leaderboard",
            "Goals",
            "Anxiety & Stress",
            "Workouts",
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
    with tabs[7]:
        _leaderboard_tab(df)
    with tabs[8]:
        _goals_tab(df, goals_cfg)
    with tabs[9]:
        _anxiety_stress_tab(df)
    with tabs[10]:
        _workouts_tab(df)


if __name__ == "__main__":
    main()
