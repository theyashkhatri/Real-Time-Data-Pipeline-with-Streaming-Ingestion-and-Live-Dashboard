"""Event contract and validation for incoming order events.

Every message on the Kafka topic must pass `parse_message` before it is allowed
into the `orders` table. Anything that fails is routed to the dead-letter table.
"""
import json
from datetime import datetime, timedelta, timezone

CATEGORIES = {"Electronics", "Home", "Fashion", "Books", "Sports", "Beauty"}
PAYMENT_METHODS = {"card", "upi", "paypal", "bank_transfer", "wallet"}
STATUSES = {"completed", "pending", "cancelled"}

REQUIRED_FIELDS = (
    "order_id",
    "user_id",
    "product",
    "category",
    "quantity",
    "unit_price",
    "country",
    "payment_method",
    "status",
    "event_time",
)

MAX_FUTURE_SKEW = timedelta(minutes=5)


class ValidationError(ValueError):
    """Raised when an event does not satisfy the contract."""


def _is_number(value) -> bool:
    # bool is a subclass of int in Python; reject it explicitly.
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _string(raw: dict, key: str, max_len: int = 100) -> str:
    value = raw[key]
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{key} must be a non-empty string")
    value = value.strip()
    if len(value) > max_len:
        raise ValidationError(f"{key} is longer than {max_len} characters")
    return value


def _choice(raw: dict, key: str, allowed: set) -> str:
    value = _string(raw, key)
    if value not in allowed:
        raise ValidationError(f"{key} '{value}' is not one of {sorted(allowed)}")
    return value


def _parse_time(value, now: datetime) -> datetime:
    if not isinstance(value, str):
        raise ValidationError("event_time must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValidationError(f"event_time '{value}' is not valid ISO-8601") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    if parsed > now + MAX_FUTURE_SKEW:
        raise ValidationError("event_time is in the future")
    return parsed


def validate_event(raw: dict, now: datetime | None = None) -> dict:
    """Validate and normalise one event. Returns a clean dict or raises ValidationError."""
    now = now or datetime.now(timezone.utc)

    missing = [f for f in REQUIRED_FIELDS if f not in raw or raw[f] is None]
    if missing:
        raise ValidationError(f"missing required fields: {', '.join(missing)}")

    user_id = raw["user_id"]
    if not isinstance(user_id, int) or isinstance(user_id, bool) or user_id <= 0:
        raise ValidationError("user_id must be a positive integer")

    quantity = raw["quantity"]
    if not isinstance(quantity, int) or isinstance(quantity, bool) or not 1 <= quantity <= 20:
        raise ValidationError("quantity must be an integer between 1 and 20")

    unit_price = raw["unit_price"]
    if not _is_number(unit_price) or not 0 < unit_price <= 10_000:
        raise ValidationError("unit_price must be a number in (0, 10000]")

    country = _string(raw, "country")
    if len(country) != 2 or not country.isalpha():
        raise ValidationError("country must be a 2-letter code")

    return {
        "order_id": _string(raw, "order_id", max_len=64),
        "user_id": user_id,
        "product": _string(raw, "product"),
        "category": _choice(raw, "category", CATEGORIES),
        "quantity": quantity,
        "unit_price": round(float(unit_price), 2),
        "total_amount": round(quantity * float(unit_price), 2),
        "country": country.upper(),
        "payment_method": _choice(raw, "payment_method", PAYMENT_METHODS),
        "status": _choice(raw, "status", STATUSES),
        "event_time": _parse_time(raw["event_time"], now),
    }


def parse_message(payload: bytes | str | None, now: datetime | None = None) -> dict:
    """Decode a raw Kafka message value and validate it."""
    if payload is None:
        raise ValidationError("empty message")
    try:
        text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        data = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValidationError(f"malformed JSON: {exc}") from None
    if not isinstance(data, dict):
        raise ValidationError("payload must be a JSON object")
    return validate_event(data, now=now)
