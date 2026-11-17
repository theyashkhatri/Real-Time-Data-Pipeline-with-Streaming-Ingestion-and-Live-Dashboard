from datetime import datetime, timedelta, timezone

import pytest

from common.schema import ValidationError, parse_message, validate_event

NOW = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)


def valid_event(**overrides):
    event = {
        "order_id": "ord-1",
        "user_id": 42,
        "product": "Wireless Earbuds",
        "category": "Electronics",
        "quantity": 2,
        "unit_price": 59.99,
        "country": "in",
        "payment_method": "upi",
        "status": "completed",
        "event_time": "2026-11-17T11:59:30Z",
    }
    event.update(overrides)
    return event


def test_valid_event_is_normalised():
    clean = validate_event(valid_event(), now=NOW)
    assert clean["total_amount"] == 119.98
    assert clean["country"] == "IN"
    assert clean["event_time"] == datetime(2026, 11, 17, 11, 59, 30, tzinfo=timezone.utc)


def test_naive_timestamp_is_treated_as_utc():
    clean = validate_event(valid_event(event_time="2026-11-17T11:00:00"), now=NOW)
    assert clean["event_time"].tzinfo == timezone.utc


def test_offset_timestamp_is_converted_to_utc():
    clean = validate_event(valid_event(event_time="2026-11-17T16:30:00+05:30"), now=NOW)
    assert clean["event_time"] == datetime(2026, 11, 17, 11, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "overrides",
    [
        {"quantity": 0},
        {"quantity": -3},
        {"quantity": 21},
        {"quantity": 2.5},
        {"quantity": True},
        {"unit_price": 0},
        {"unit_price": -5},
        {"unit_price": "12"},
        {"unit_price": 10_001},
        {"user_id": 0},
        {"user_id": "7"},
        {"category": "Groceries"},
        {"status": "shipped"},
        {"payment_method": "cash"},
        {"country": "USA"},
        {"country": "1X"},
        {"order_id": "  "},
        {"event_time": "yesterday"},
        {"event_time": 1700000000},
        {"event_time": "2026-11-17T13:00:00Z"},  # in the future
    ],
)
def test_invalid_values_are_rejected(overrides):
    with pytest.raises(ValidationError):
        validate_event(valid_event(**overrides), now=NOW)


def test_missing_field_is_rejected():
    event = valid_event()
    del event["category"]
    with pytest.raises(ValidationError, match="category"):
        validate_event(event, now=NOW)


def test_small_clock_skew_is_tolerated():
    soon = (NOW + timedelta(minutes=2)).isoformat()
    assert validate_event(valid_event(event_time=soon), now=NOW)


@pytest.mark.parametrize("payload", [None, b"", b"{{ nope", b"[1, 2]", b"\xff\xfe", "42"])
def test_parse_message_rejects_bad_payloads(payload):
    with pytest.raises(ValidationError):
        parse_message(payload, now=NOW)


def test_parse_message_accepts_bytes_and_str():
    import json

    body = json.dumps(valid_event())
    assert parse_message(body, now=NOW)["order_id"] == "ord-1"
    assert parse_message(body.encode(), now=NOW)["order_id"] == "ord-1"
