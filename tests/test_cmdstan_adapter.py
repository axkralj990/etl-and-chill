from __future__ import annotations

import pandas as pd

from life.inference.cmdstan_adapter import CmdStanInferenceAdapter


def test_build_design_matrix_with_lag_and_normalization() -> None:
    df = pd.DataFrame(
        {
            "date_local": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]),
            "target": [10.0, 11.0, 13.0, 12.0],
            "steps": [1000, 2000, 3000, 4000],
            "sleep_total_hours": [6.5, 7.0, 7.5, 7.0],
        }
    )
    adapter = CmdStanInferenceAdapter()

    frame, x, y, feature_names = adapter.build_design_matrix(
        df=df,
        target="target",
        features=["steps", "sleep_total_hours"],
        lag_by_feature={"steps": 1, "sleep_total_hours": 1},
        normalize=True,
    )

    assert len(frame) == 3
    assert feature_names == ["steps_lag_plus_1", "sleep_total_hours_lag_plus_1"]
    assert x.shape == (3, 2)
    assert y.shape == (3,)
    assert round(float(y.mean()), 8) == 0.0


def test_build_design_matrix_raises_on_unknown_feature() -> None:
    df = pd.DataFrame(
        {
            "date_local": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "target": [1.0, 2.0],
            "steps": [1000, 1500],
        }
    )
    adapter = CmdStanInferenceAdapter()

    try:
        adapter.build_design_matrix(
            df=df,
            target="target",
            features=["missing_feature"],
            lag_by_feature=None,
            normalize=False,
        )
    except ValueError as exc:
        assert "Unknown explanatory variable" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected ValueError for unknown feature")
