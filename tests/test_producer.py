import json
import random

import pytest

from common.schema import ValidationError, parse_message, validate_event
from producer.producer import build_event, corrupt_event, current_rate


def test_generated_events_always_pass_validation():
    rng = random.Random(7)
    for _ in range(1000):
        event = build_event(rng)
        assert validate_event(event)["total_amount"] > 0


def test_generated_order_ids_are_unique():
    rng = random.Random(1)
    ids = {build_event(rng)["order_id"] for _ in range(1000)}
    assert len(ids) == 1000


def test_corrupted_events_always_fail_validation():
    rng = random.Random(11)
    for _ in range(1000):
        raw = corrupt_event(build_event(rng), rng)
        with pytest.raises(ValidationError):
            parse_message(raw)


def test_generated_events_round_trip_through_json():
    event = build_event(random.Random(3))
    assert parse_message(json.dumps(event).encode())["order_id"] == event["order_id"]


def test_traffic_rate_stays_positive_and_bounded():
    rates = [current_rate(5, t) for t in range(0, 600, 5)]
    assert min(rates) >= 0.1
    assert max(rates) <= 5 * 1.5 + 1e-9
