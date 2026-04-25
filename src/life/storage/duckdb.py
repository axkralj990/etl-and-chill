from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from life.enums import RunStatus


class DuckDBStorage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn = duckdb.connect(str(db_path))
        self.ensure_schema()

    def ensure_schema(self) -> None:
        self.conn.execute(
            """
            create table if not exists raw_records (
                source varchar not null,
                endpoint varchar not null,
                source_id varchar not null,
                day date,
                payload_json varchar not null,
                ingested_at timestamp not null
            );
            """
        )
        self.conn.execute(
            """
            create table if not exists canonical_notion_daily (
                source_id varchar,
                date_local date primary key,
                week_start_monday date not null,
                iso_week integer not null,
                year integer not null,
                month integer not null,
                name varchar,
                name_date_match boolean,
                anxiety_status_label varchar,
                anxiety_status_score integer,
                physical_status_label varchar,
                physical_status_score integer,
                productivity_label varchar,
                productivity_score integer,
                weight_kg double,
                alcohol_units double,
                mindful_min double,
                points double,
                coffee_count double,
                fasting_hours double,
                sleep_hours_self_reported double,
                cold_min double,
                cigarettes_count integer,
                substances_raw varchar,
                general_notes varchar,
                supplements varchar,
                weather varchar,
                learned varchar
            );
            """
        )
        self.conn.execute(
            """
            create table if not exists canonical_oura_daily (
                date_local date primary key,
                activity_score integer,
                steps integer,
                active_calories integer,
                total_calories integer,
                target_calories integer,
                inactivity_alerts integer,
                sleep_score integer,
                sleep_time_in_bed integer,
                sleep_total_duration integer,
                sleep_deep_duration integer,
                sleep_rem_duration integer,
                sleep_light_duration integer,
                sleep_lowest_hr integer,
                sleep_avg_hr double,
                sleep_avg_hrv double,
                readiness_score integer,
                sleep_temperature_deviation double,
                sleep_temperature_trend_deviation double,
                spo2_average double,
                daytime_stress_avg double,
                stress_summary varchar,
                stress_high integer,
                stress_recovery integer,
                resilience_level varchar,
                cardiovascular_age integer,
                source_payloads_json varchar not null
            );
            """
        )
        self.conn.execute(
            """
            create table if not exists daily_features (
                date_local date primary key,
                week_start_monday date not null,
                iso_week integer not null,
                year integer not null,
                month integer not null,
                weekday integer not null,
                activity_score integer,
                steps integer,
                active_calories integer,
                total_calories integer,
                sleep_score integer,
                sleep_time_in_bed integer,
                sleep_total_duration integer,
                sleep_deep_duration integer,
                sleep_rem_duration integer,
                sleep_light_duration integer,
                sleep_time_in_bed_hours double,
                sleep_total_hours double,
                sleep_deep_hours double,
                sleep_rem_hours double,
                sleep_light_hours double,
                sleep_efficiency_pct double,
                sleep_deep_share_pct double,
                sleep_rem_share_pct double,
                sleep_light_share_pct double,
                sleep_lowest_hr integer,
                sleep_avg_hr double,
                sleep_avg_hrv double,
                readiness_score integer,
                sleep_temperature_deviation double,
                sleep_temperature_trend_deviation double,
                spo2_average double,
                daytime_stress_avg double,
                anxiety_status_score integer,
                productivity_score integer,
                physical_status_score integer,
                sleep_hours_self_reported double,
                alcohol_units double,
                coffee_count double,
                mindful_min double,
                cold_min double,
                points double,
                cigarettes_count integer,
                sleep_score_7d_avg double,
                readiness_score_7d_avg double,
                daytime_stress_7d_avg double,
                hrv_7d_avg double,
                hr_7d_avg double,
                temp_dev_7d_avg double,
                sleep_minus_anxiety double,
                readiness_minus_productivity double,
                high_stress_flag boolean
            );
            """
        )
        self._ensure_optional_columns()
        self.conn.execute(
            """
            create table if not exists sync_state (
                source varchar not null,
                endpoint varchar not null,
                max_date date,
                primary key (source, endpoint)
            );
            """
        )
        self.conn.execute(
            """
            create table if not exists pipeline_runs (
                run_id varchar primary key,
                mode varchar not null,
                status varchar not null,
                started_at timestamp not null,
                completed_at timestamp,
                details_json varchar
            );
            """
        )

    def insert_raw_records(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        prepared = [
            (
                row["source"],
                row["endpoint"],
                row["source_id"],
                row.get("day"),
                json.dumps(row["payload"], ensure_ascii=True),
                row["ingested_at"],
            )
            for row in rows
        ]
        self.conn.executemany(
            """
            insert into raw_records (source, endpoint, source_id, day, payload_json, ingested_at)
            values (?, ?, ?, ?, ?, ?)
            """,
            prepared,
        )

    def _ensure_optional_columns(self) -> None:
        columns = {
            row[1]
            for row in self.conn.execute("pragma table_info('canonical_oura_daily')").fetchall()
        }
        desired = {
            "stress_summary": "varchar",
            "steps": "integer",
            "active_calories": "integer",
            "total_calories": "integer",
            "target_calories": "integer",
            "inactivity_alerts": "integer",
            "sleep_time_in_bed": "integer",
            "sleep_total_duration": "integer",
            "sleep_deep_duration": "integer",
            "sleep_rem_duration": "integer",
            "sleep_light_duration": "integer",
            "sleep_lowest_hr": "integer",
            "sleep_avg_hr": "double",
            "sleep_avg_hrv": "double",
            "sleep_temperature_deviation": "double",
            "sleep_temperature_trend_deviation": "double",
        }
        for column, dtype in desired.items():
            if column not in columns:
                self.conn.execute(f"alter table canonical_oura_daily add column {column} {dtype}")

        df_columns = {
            row[1] for row in self.conn.execute("pragma table_info('daily_features')").fetchall()
        }
        df_desired = {
            "steps": "integer",
            "active_calories": "integer",
            "total_calories": "integer",
            "sleep_time_in_bed": "integer",
            "sleep_total_duration": "integer",
            "sleep_deep_duration": "integer",
            "sleep_rem_duration": "integer",
            "sleep_light_duration": "integer",
            "sleep_time_in_bed_hours": "double",
            "sleep_total_hours": "double",
            "sleep_deep_hours": "double",
            "sleep_rem_hours": "double",
            "sleep_light_hours": "double",
            "sleep_efficiency_pct": "double",
            "sleep_deep_share_pct": "double",
            "sleep_rem_share_pct": "double",
            "sleep_light_share_pct": "double",
            "sleep_lowest_hr": "integer",
            "sleep_avg_hr": "double",
            "sleep_avg_hrv": "double",
            "sleep_temperature_deviation": "double",
            "sleep_temperature_trend_deviation": "double",
            "hrv_7d_avg": "double",
            "hr_7d_avg": "double",
            "temp_dev_7d_avg": "double",
            "cold_min": "double",
        }
        for column, dtype in df_desired.items():
            if column not in df_columns:
                self.conn.execute(f"alter table daily_features add column {column} {dtype}")

    def upsert_notion_daily(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            columns = list(row.keys())
            placeholders = ", ".join(["?"] * len(columns))
            update_clause = ", ".join([f"{c}=excluded.{c}" for c in columns if c != "date_local"])
            sql = f"""
                insert into canonical_notion_daily ({", ".join(columns)})
                values ({placeholders})
                on conflict(date_local) do update set {update_clause}
            """
            self.conn.execute(sql, [row[c] for c in columns])

    def upsert_oura_daily(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            row = dict(row)
            row["source_payloads_json"] = json.dumps(row.pop("source_payloads"), ensure_ascii=True)
            columns = list(row.keys())
            placeholders = ", ".join(["?"] * len(columns))
            update_clause = ", ".join([f"{c}=excluded.{c}" for c in columns if c != "date_local"])
            sql = f"""
                insert into canonical_oura_daily ({", ".join(columns)})
                values ({placeholders})
                on conflict(date_local) do update set {update_clause}
            """
            self.conn.execute(sql, [row[c] for c in columns])

    def build_daily_features(self) -> None:
        self.conn.execute(
            """
            insert into daily_features (
                date_local,
                week_start_monday,
                iso_week,
                year,
                month,
                weekday,
                activity_score,
                steps,
                active_calories,
                total_calories,
                sleep_score,
                sleep_time_in_bed,
                sleep_total_duration,
                sleep_deep_duration,
                sleep_rem_duration,
                sleep_light_duration,
                sleep_time_in_bed_hours,
                sleep_total_hours,
                sleep_deep_hours,
                sleep_rem_hours,
                sleep_light_hours,
                sleep_efficiency_pct,
                sleep_deep_share_pct,
                sleep_rem_share_pct,
                sleep_light_share_pct,
                sleep_lowest_hr,
                sleep_avg_hr,
                sleep_avg_hrv,
                readiness_score,
                sleep_temperature_deviation,
                sleep_temperature_trend_deviation,
                spo2_average,
                daytime_stress_avg,
                anxiety_status_score,
                productivity_score,
                physical_status_score,
                sleep_hours_self_reported,
                alcohol_units,
                coffee_count,
                mindful_min,
                cold_min,
                points,
                cigarettes_count,
                sleep_score_7d_avg,
                readiness_score_7d_avg,
                daytime_stress_7d_avg,
                hrv_7d_avg,
                hr_7d_avg,
                temp_dev_7d_avg,
                sleep_minus_anxiety,
                readiness_minus_productivity,
                high_stress_flag
            )
            select
                coalesce(o.date_local, n.date_local) as date_local,
                date_trunc('week', coalesce(o.date_local, n.date_local))::date as week_start_monday,
                weekofyear(coalesce(o.date_local, n.date_local)) as iso_week,
                year(coalesce(o.date_local, n.date_local)) as year,
                month(coalesce(o.date_local, n.date_local)) as month,
                cast(
                    date_part('isodow', coalesce(o.date_local, n.date_local))
                    as integer
                ) as weekday,
                o.activity_score,
                o.steps,
                o.active_calories,
                o.total_calories,
                o.sleep_score,
                o.sleep_time_in_bed,
                o.sleep_total_duration,
                o.sleep_deep_duration,
                o.sleep_rem_duration,
                o.sleep_light_duration,
                cast(o.sleep_time_in_bed as double) / 3600.0 as sleep_time_in_bed_hours,
                cast(o.sleep_total_duration as double) / 3600.0 as sleep_total_hours,
                cast(o.sleep_deep_duration as double) / 3600.0 as sleep_deep_hours,
                cast(o.sleep_rem_duration as double) / 3600.0 as sleep_rem_hours,
                cast(o.sleep_light_duration as double) / 3600.0 as sleep_light_hours,
                100.0 * cast(o.sleep_total_duration as double)
                    / nullif(cast(o.sleep_time_in_bed as double), 0) as sleep_efficiency_pct,
                100.0 * cast(o.sleep_deep_duration as double)
                    / nullif(cast(o.sleep_total_duration as double), 0) as sleep_deep_share_pct,
                100.0 * cast(o.sleep_rem_duration as double)
                    / nullif(cast(o.sleep_total_duration as double), 0) as sleep_rem_share_pct,
                100.0 * cast(o.sleep_light_duration as double)
                    / nullif(cast(o.sleep_total_duration as double), 0) as sleep_light_share_pct,
                o.sleep_lowest_hr,
                o.sleep_avg_hr,
                o.sleep_avg_hrv,
                o.readiness_score,
                o.sleep_temperature_deviation,
                o.sleep_temperature_trend_deviation,
                o.spo2_average,
                o.daytime_stress_avg,
                n.anxiety_status_score,
                n.productivity_score,
                n.physical_status_score,
                n.sleep_hours_self_reported,
                n.alcohol_units,
                n.coffee_count,
                n.mindful_min,
                n.cold_min,
                n.points,
                n.cigarettes_count,
                avg(o.sleep_score) over (
                    order by coalesce(o.date_local, n.date_local)
                    rows between 6 preceding and current row
                ) as sleep_score_7d_avg,
                avg(o.readiness_score) over (
                    order by coalesce(o.date_local, n.date_local)
                    rows between 6 preceding and current row
                ) as readiness_score_7d_avg,
                avg(o.daytime_stress_avg) over (
                    order by coalesce(o.date_local, n.date_local)
                    rows between 6 preceding and current row
                ) as daytime_stress_7d_avg,
                avg(o.sleep_avg_hrv) over (
                    order by coalesce(o.date_local, n.date_local)
                    rows between 6 preceding and current row
                ) as hrv_7d_avg,
                avg(o.sleep_avg_hr) over (
                    order by coalesce(o.date_local, n.date_local)
                    rows between 6 preceding and current row
                ) as hr_7d_avg,
                avg(o.sleep_temperature_deviation) over (
                    order by coalesce(o.date_local, n.date_local)
                    rows between 6 preceding and current row
                ) as temp_dev_7d_avg,
                cast(o.sleep_score as double)
                    - cast(n.anxiety_status_score as double) as sleep_minus_anxiety,
                cast(o.readiness_score as double)
                    - cast(n.productivity_score as double) as readiness_minus_productivity,
                coalesce(o.daytime_stress_avg, 0) > 70 as high_stress_flag
            from canonical_oura_daily o
            full outer join canonical_notion_daily n
              on o.date_local = n.date_local
            on conflict(date_local) do update set
                week_start_monday = excluded.week_start_monday,
                iso_week = excluded.iso_week,
                year = excluded.year,
                month = excluded.month,
                weekday = excluded.weekday,
                activity_score = excluded.activity_score,
                steps = excluded.steps,
                active_calories = excluded.active_calories,
                total_calories = excluded.total_calories,
                sleep_score = excluded.sleep_score,
                sleep_time_in_bed = excluded.sleep_time_in_bed,
                sleep_total_duration = excluded.sleep_total_duration,
                sleep_deep_duration = excluded.sleep_deep_duration,
                sleep_rem_duration = excluded.sleep_rem_duration,
                sleep_light_duration = excluded.sleep_light_duration,
                sleep_time_in_bed_hours = excluded.sleep_time_in_bed_hours,
                sleep_total_hours = excluded.sleep_total_hours,
                sleep_deep_hours = excluded.sleep_deep_hours,
                sleep_rem_hours = excluded.sleep_rem_hours,
                sleep_light_hours = excluded.sleep_light_hours,
                sleep_efficiency_pct = excluded.sleep_efficiency_pct,
                sleep_deep_share_pct = excluded.sleep_deep_share_pct,
                sleep_rem_share_pct = excluded.sleep_rem_share_pct,
                sleep_light_share_pct = excluded.sleep_light_share_pct,
                sleep_lowest_hr = excluded.sleep_lowest_hr,
                sleep_avg_hr = excluded.sleep_avg_hr,
                sleep_avg_hrv = excluded.sleep_avg_hrv,
                readiness_score = excluded.readiness_score,
                sleep_temperature_deviation = excluded.sleep_temperature_deviation,
                sleep_temperature_trend_deviation = excluded.sleep_temperature_trend_deviation,
                spo2_average = excluded.spo2_average,
                daytime_stress_avg = excluded.daytime_stress_avg,
                anxiety_status_score = excluded.anxiety_status_score,
                productivity_score = excluded.productivity_score,
                physical_status_score = excluded.physical_status_score,
                sleep_hours_self_reported = excluded.sleep_hours_self_reported,
                alcohol_units = excluded.alcohol_units,
                coffee_count = excluded.coffee_count,
                mindful_min = excluded.mindful_min,
                cold_min = excluded.cold_min,
                points = excluded.points,
                cigarettes_count = excluded.cigarettes_count,
                sleep_score_7d_avg = excluded.sleep_score_7d_avg,
                readiness_score_7d_avg = excluded.readiness_score_7d_avg,
                daytime_stress_7d_avg = excluded.daytime_stress_7d_avg,
                hrv_7d_avg = excluded.hrv_7d_avg,
                hr_7d_avg = excluded.hr_7d_avg,
                temp_dev_7d_avg = excluded.temp_dev_7d_avg,
                sleep_minus_anxiety = excluded.sleep_minus_anxiety,
                readiness_minus_productivity = excluded.readiness_minus_productivity,
                high_stress_flag = excluded.high_stress_flag
            """
        )

    def set_sync_state(self, source: str, endpoint: str, max_date: date | None) -> None:
        self.conn.execute(
            """
            insert into sync_state (source, endpoint, max_date)
            values (?, ?, ?)
            on conflict(source, endpoint) do update set max_date = excluded.max_date
            """,
            [source, endpoint, max_date],
        )

    def get_sync_state(self, source: str, endpoint: str) -> date | None:
        row = self.conn.execute(
            "select max_date from sync_state where source = ? and endpoint = ?",
            [source, endpoint],
        ).fetchone()
        if not row:
            return None
        return row[0]

    def get_max_canonical_date(self, table_name: str, date_col: str = "date_local") -> date | None:
        row = self.conn.execute(f"select max({date_col}) from {table_name}").fetchone()
        return row[0] if row else None

    def insert_run_start(self, run_id: str, mode: str) -> None:
        self.conn.execute(
            """
            insert into pipeline_runs (run_id, mode, status, started_at)
            values (?, ?, ?, ?)
            """,
            [run_id, mode, RunStatus.STARTED.value, datetime.utcnow()],
        )

    def finalize_run(self, run_id: str, status: RunStatus, details_json: str | None = None) -> None:
        self.conn.execute(
            """
            update pipeline_runs
            set status = ?, completed_at = ?, details_json = ?
            where run_id = ?
            """,
            [status.value, datetime.utcnow(), details_json, run_id],
        )
