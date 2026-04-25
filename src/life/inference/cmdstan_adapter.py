from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class BayesianRegressionResult:
    feature_names: list[str]
    frame: pd.DataFrame
    normalized: bool
    coefficients: pd.DataFrame
    coefficient_draws: pd.DataFrame
    ppc: pd.DataFrame
    diagnostics: dict[str, float | int | str]
    stdout_tail: str


class CmdStanInferenceAdapter:
    """Bayesian multiple regression helper built on cmdstanpy."""

    def __init__(self) -> None:
        self.model_path = Path(__file__).with_name("models") / "linear_regression_ppc.stan"

    @staticmethod
    def _standardize(series: pd.Series) -> pd.Series:
        std = float(series.std(ddof=0))
        if std == 0 or np.isnan(std):
            return pd.Series(np.zeros(len(series)), index=series.index)
        return (series - float(series.mean())) / std

    def build_design_matrix(
        self,
        *,
        df: pd.DataFrame,
        target: str,
        features: list[str],
        lag_by_feature: dict[str, int] | None,
        normalize: bool,
    ) -> tuple[pd.DataFrame, np.ndarray, np.ndarray, list[str]]:
        if "date_local" not in df.columns:
            raise ValueError("Data frame must contain date_local column")
        if target not in df.columns:
            raise ValueError(f"Unknown target column: {target}")
        if not features:
            raise ValueError("Select at least one explanatory variable")

        frame = df.copy().sort_values("date_local")
        model_columns: list[str] = []
        lag_map = lag_by_feature or {}
        for feature in features:
            if feature not in frame.columns:
                raise ValueError(f"Unknown explanatory variable: {feature}")
            lag_days = int(lag_map.get(feature, 0))
            if lag_days != 0:
                sign = "plus" if lag_days > 0 else "minus"
                name = f"{feature}_lag_{sign}_{abs(lag_days)}"
                frame[name] = pd.to_numeric(frame[feature], errors="coerce").shift(lag_days)
                model_columns.append(name)
            else:
                frame[feature] = pd.to_numeric(frame[feature], errors="coerce")
                model_columns.append(feature)

        frame[target] = pd.to_numeric(frame[target], errors="coerce")
        frame = frame.dropna(subset=[target, *model_columns]).copy()
        if frame.empty:
            raise ValueError("No rows left after applying lag/NA filtering")

        if normalize:
            frame[target] = self._standardize(frame[target])
            for col in model_columns:
                frame[col] = self._standardize(frame[col])

        y = frame[target].to_numpy(dtype=float)
        x = frame[model_columns].to_numpy(dtype=float)
        return frame, x, y, model_columns

    @staticmethod
    def _build_coefficients(summary: pd.DataFrame, feature_names: list[str]) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        alpha_row = summary.loc["alpha"] if "alpha" in summary.index else None
        if alpha_row is not None:
            rows.append(
                {
                    "term": "intercept",
                    "mean": float(alpha_row.get("Mean", np.nan)),
                    "q5": float(alpha_row.get("5%", np.nan)),
                    "q50": float(alpha_row.get("50%", np.nan)),
                    "q95": float(alpha_row.get("95%", np.nan)),
                    "r_hat": float(alpha_row.get("R_hat", np.nan)),
                    "ess_bulk": float(alpha_row.get("ESS_bulk", np.nan)),
                }
            )

        for i, feature in enumerate(feature_names, start=1):
            row_name = f"beta[{i}]"
            if row_name not in summary.index:
                continue
            row = summary.loc[row_name]
            rows.append(
                {
                    "term": feature,
                    "mean": float(row.get("Mean", np.nan)),
                    "q5": float(row.get("5%", np.nan)),
                    "q50": float(row.get("50%", np.nan)),
                    "q95": float(row.get("95%", np.nan)),
                    "r_hat": float(row.get("R_hat", np.nan)),
                    "ess_bulk": float(row.get("ESS_bulk", np.nan)),
                }
            )

        return pd.DataFrame(rows)

    @staticmethod
    def _build_coefficient_draws(draws: pd.DataFrame, feature_names: list[str]) -> pd.DataFrame:
        rows: list[pd.DataFrame] = []

        if "alpha" in draws.columns:
            rows.append(
                pd.DataFrame(
                    {
                        "term": "intercept",
                        "value": pd.to_numeric(draws["alpha"], errors="coerce"),
                    }
                )
            )

        for idx, feature in enumerate(feature_names, start=1):
            col = f"beta[{idx}]"
            if col not in draws.columns:
                continue
            rows.append(
                pd.DataFrame(
                    {
                        "term": feature,
                        "value": pd.to_numeric(draws[col], errors="coerce"),
                    }
                )
            )

        if not rows:
            return pd.DataFrame(columns=["term", "value"])
        return pd.concat(rows, ignore_index=True).dropna(subset=["value"])

    @staticmethod
    def _build_ppc(frame: pd.DataFrame, y_actual: np.ndarray, y_rep: np.ndarray) -> pd.DataFrame:
        p5, p50, p95 = np.percentile(y_rep, [5, 50, 95], axis=0)
        out = frame[["date_local"]].copy()
        out["y_actual"] = y_actual
        out["y_rep_q5"] = p5
        out["y_rep_q50"] = p50
        out["y_rep_q95"] = p95
        return out.sort_values("date_local")

    @staticmethod
    def _diagnostics(
        summary: pd.DataFrame,
        coefficients: pd.DataFrame,
        fit: Any,
    ) -> dict[str, float | int | str]:
        r_hat_col = summary["R_hat"] if "R_hat" in summary.columns else pd.Series(dtype=float)
        ess_col = summary["ESS_bulk"] if "ESS_bulk" in summary.columns else pd.Series(dtype=float)
        max_r_hat = (
            float(r_hat_col.replace([np.inf, -np.inf], np.nan).dropna().max())
            if not r_hat_col.empty
            else float("nan")
        )
        min_ess_bulk = (
            float(ess_col.replace([np.inf, -np.inf], np.nan).dropna().min())
            if not ess_col.empty
            else float("nan")
        )

        divergent = 0
        try:
            divergent_draws = fit.method_variables()["divergent__"]
            divergent = int(np.sum(divergent_draws))
        except Exception:
            divergent = 0

        coefficient_terms = len(coefficients[coefficients["term"] != "intercept"])
        diagnostics_text = "good"
        if (not np.isnan(max_r_hat) and max_r_hat > 1.01) or divergent > 0:
            diagnostics_text = "needs_attention"

        return {
            "max_r_hat": max_r_hat,
            "min_ess_bulk": min_ess_bulk,
            "divergent_transitions": divergent,
            "num_predictors": coefficient_terms,
            "mcmc_quality": diagnostics_text,
        }

    @staticmethod
    def _stdout_tail(fit: Any, lines_per_chain: int = 20) -> str:
        try:
            stdout_files = getattr(fit.runset, "stdout_files", None)
        except Exception:
            stdout_files = None
        if not stdout_files:
            return "No CmdStan stdout files available."

        chunks: list[str] = []
        for idx, path in enumerate(stdout_files, start=1):
            if not path:
                continue
            try:
                content = Path(path).read_text(encoding="utf-8", errors="ignore").splitlines()
            except OSError:
                continue
            tail = content[-lines_per_chain:]
            chunks.append(f"# Chain {idx}\n" + "\n".join(tail))

        if not chunks:
            return "No readable CmdStan stdout output found."
        return "\n\n".join(chunks)

    def run(
        self,
        *,
        df: pd.DataFrame,
        target: str,
        features: list[str],
        lag_by_feature: dict[str, int] | None = None,
        normalize: bool = True,
        seed: int = 42,
        chains: int = 4,
        iter_warmup: int = 1000,
        iter_sampling: int = 1000,
    ) -> BayesianRegressionResult:
        frame, x, y, feature_names = self.build_design_matrix(
            df=df,
            target=target,
            features=features,
            lag_by_feature=lag_by_feature,
            normalize=normalize,
        )

        try:
            from cmdstanpy import CmdStanModel
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise ModuleNotFoundError(
                "cmdstanpy is not installed. "
                "Install optional dependency group: `uv sync --extra inference`"
            ) from exc

        model = CmdStanModel(stan_file=str(self.model_path))
        fit = model.sample(
            data={"N": int(x.shape[0]), "K": int(x.shape[1]), "X": x, "y": y},
            seed=seed,
            chains=chains,
            parallel_chains=chains,
            iter_warmup=iter_warmup,
            iter_sampling=iter_sampling,
            show_progress=False,
        )

        summary = fit.summary()
        coefficients = self._build_coefficients(summary, feature_names)
        coefficient_draws = self._build_coefficient_draws(fit.draws_pd(), feature_names)
        y_rep = fit.stan_variable("y_rep")
        ppc = self._build_ppc(frame, y, y_rep)
        diagnostics = self._diagnostics(summary, coefficients, fit)
        stdout_tail = self._stdout_tail(fit)

        return BayesianRegressionResult(
            feature_names=feature_names,
            frame=frame,
            normalized=normalize,
            coefficients=coefficients,
            coefficient_draws=coefficient_draws,
            ppc=ppc,
            diagnostics=diagnostics,
            stdout_tail=stdout_tail,
        )
