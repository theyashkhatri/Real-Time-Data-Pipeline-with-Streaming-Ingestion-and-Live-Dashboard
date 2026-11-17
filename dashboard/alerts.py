"""Pure alert rules for the dashboard (no Streamlit dependency, so they are unit-testable)."""
from typing import NamedTuple

STALL_SECONDS = 30
INVALID_RATE_LIMIT = 0.10
LATENCY_LIMIT_SECONDS = 10
REVENUE_DROP_RATIO = 0.5


class Alert(NamedTuple):
    level: str  # "info" | "warning" | "error"
    message: str


def evaluate_alerts(
    orders_in_window: int,
    invalid_in_window: int,
    seconds_since_last_ingest: float | None,
    avg_latency_seconds: float,
    revenue_last_5m: float,
    revenue_prev_5m: float,
) -> list[Alert]:
    alerts: list[Alert] = []

    if seconds_since_last_ingest is None:
        return [Alert("info", "Waiting for the first events to arrive...")]

    if seconds_since_last_ingest > STALL_SECONDS:
        alerts.append(
            Alert("error", f"Stream stalled: no new orders ingested for {seconds_since_last_ingest:.0f}s.")
        )

    total = orders_in_window + invalid_in_window
    if total and invalid_in_window / total > INVALID_RATE_LIMIT:
        alerts.append(
            Alert("warning", f"High invalid-message rate: {invalid_in_window / total:.1%} of messages failed validation.")
        )

    if avg_latency_seconds > LATENCY_LIMIT_SECONDS:
        alerts.append(
            Alert("warning", f"Pipeline latency is {avg_latency_seconds:.1f}s on average (limit {LATENCY_LIMIT_SECONDS}s).")
        )

    if revenue_prev_5m > 0 and revenue_last_5m < REVENUE_DROP_RATIO * revenue_prev_5m:
        drop = 1 - revenue_last_5m / revenue_prev_5m
        alerts.append(Alert("warning", f"Revenue dropped {drop:.0%} versus the previous 5 minutes."))

    return alerts
