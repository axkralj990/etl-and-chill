from __future__ import annotations


class CmdStanInferenceAdapter:
    """
    Extension point for future Bayesian inference workflows with cmdstanpy.
    """

    def run(self) -> None:
        return None
