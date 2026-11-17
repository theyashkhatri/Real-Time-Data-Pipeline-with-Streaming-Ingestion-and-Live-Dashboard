from alerts import evaluate_alerts


def healthy(**overrides):
    args = dict(
        orders_in_window=1000,
        invalid_in_window=20,
        seconds_since_last_ingest=2.0,
        avg_latency_seconds=0.5,
        revenue_last_5m=1000.0,
        revenue_prev_5m=1000.0,
    )
    args.update(overrides)
    return evaluate_alerts(**args)


def levels(alerts):
    return [a.level for a in alerts]


def test_healthy_pipeline_raises_no_alerts():
    assert healthy() == []


def test_no_data_yet_is_informational_only():
    alerts = healthy(seconds_since_last_ingest=None, invalid_in_window=500)
    assert levels(alerts) == ["info"]


def test_stalled_stream_is_an_error():
    alerts = healthy(seconds_since_last_ingest=95)
    assert levels(alerts) == ["error"]
    assert "stalled" in alerts[0].message


def test_high_invalid_rate_warns():
    alerts = healthy(orders_in_window=800, invalid_in_window=200)
    assert levels(alerts) == ["warning"]


def test_invalid_rate_at_limit_does_not_warn():
    assert healthy(orders_in_window=900, invalid_in_window=100) == []


def test_high_latency_warns():
    assert levels(healthy(avg_latency_seconds=12)) == ["warning"]


def test_revenue_drop_warns():
    alerts = healthy(revenue_last_5m=300, revenue_prev_5m=1000)
    assert levels(alerts) == ["warning"]
    assert "70%" in alerts[0].message


def test_no_revenue_alert_without_a_baseline():
    assert healthy(revenue_last_5m=0, revenue_prev_5m=0) == []


def test_multiple_problems_are_all_reported():
    alerts = healthy(seconds_since_last_ingest=60, avg_latency_seconds=20, invalid_in_window=500)
    assert len(alerts) == 3
