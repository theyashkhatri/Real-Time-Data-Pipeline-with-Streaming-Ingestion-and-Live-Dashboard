"""Simulated e-commerce order stream -> Kafka.

Generates realistic order events at a Poisson-distributed rate that swells and
fades over time (so the live dashboard has visible traffic patterns). A small
share of messages is deliberately corrupted or duplicated to exercise the
consumer's validation, dead-letter and idempotency logic.
"""
import json
import logging
import math
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from common import config
from common.schema import CATEGORIES, PAYMENT_METHODS, REQUIRED_FIELDS

log = logging.getLogger("producer")

CATALOG = {
    "Electronics": [("Wireless Earbuds", 59.0), ("USB-C Hub", 34.0), ("Smart Watch", 149.0), ("Power Bank", 29.0)],
    "Home": [("Air Fryer", 89.0), ("LED Desk Lamp", 24.0), ("Coffee Maker", 64.0), ("Throw Blanket", 32.0)],
    "Fashion": [("Running Shoes", 79.0), ("Denim Jacket", 69.0), ("Backpack", 45.0), ("Sunglasses", 38.0)],
    "Books": [("Data Engineering Handbook", 42.0), ("SQL Cookbook", 36.0), ("Python Crash Course", 30.0)],
    "Sports": [("Yoga Mat", 26.0), ("Dumbbell Set", 95.0), ("Cycling Helmet", 55.0)],
    "Beauty": [("Face Serum", 28.0), ("Sunscreen SPF50", 18.0), ("Hair Dryer", 48.0)],
}
COUNTRIES = (["US"] * 30 + ["IN"] * 25 + ["GB"] * 12 + ["DE"] * 10 + ["CA"] * 8 + ["AU"] * 8 + ["SG"] * 7)
STATUS_WEIGHTS = (["completed"] * 80) + (["pending"] * 12) + (["cancelled"] * 8)

assert set(CATALOG) == CATEGORIES, "catalog categories must match the schema"


def build_event(rng: random.Random = random, now: datetime | None = None) -> dict:
    """Create one valid order event."""
    now = now or datetime.now(timezone.utc)
    category = rng.choice(list(CATALOG))
    product, base_price = rng.choice(CATALOG[category])
    return {
        "order_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "user_id": rng.randint(1, 5000),
        "product": product,
        "category": category,
        "quantity": rng.choices([1, 2, 3, 4], weights=[60, 25, 10, 5])[0],
        "unit_price": round(base_price * rng.uniform(0.9, 1.1), 2),
        "country": rng.choice(COUNTRIES),
        "payment_method": rng.choice(sorted(PAYMENT_METHODS)),
        "status": rng.choice(STATUS_WEIGHTS),
        "event_time": now.isoformat(),
    }


def corrupt_event(event: dict, rng: random.Random = random) -> bytes:
    """Return the raw bytes of a deliberately invalid message."""
    kind = rng.choice(
        ["not_json", "not_object", "missing_field", "negative_qty", "bad_category", "bad_time", "bad_price"]
    )
    if kind == "not_json":
        return b"{{ this is not json"
    if kind == "not_object":
        return b"[1, 2, 3]"

    bad = dict(event)
    if kind == "missing_field":
        bad.pop(rng.choice(REQUIRED_FIELDS))
    elif kind == "negative_qty":
        bad["quantity"] = -rng.randint(1, 5)
    elif kind == "bad_category":
        bad["category"] = "Unknown"
    elif kind == "bad_time":
        bad["event_time"] = "yesterday-ish"
    elif kind == "bad_price":
        bad["unit_price"] = -abs(bad["unit_price"])
    return json.dumps(bad).encode()


def current_rate(base_rate: float, elapsed_seconds: float) -> float:
    """Traffic rate (events/sec) that oscillates +/-50% around the base rate."""
    return max(0.1, base_rate * (1 + 0.5 * math.sin(elapsed_seconds / 45)))


def ensure_topic(bootstrap: str, topic: str, partitions: int, retries: int = 30) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    for attempt in range(1, retries + 1):
        try:
            if topic in admin.list_topics(timeout=5).topics:
                return
            futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
            futures[topic].result()
            log.info("created topic %s with %d partitions", topic, partitions)
            return
        except Exception as exc:  # broker still starting, or topic created concurrently
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                return
            log.warning("kafka not ready (%d/%d): %s", attempt, retries, exc)
            time.sleep(2)
    raise RuntimeError("could not reach Kafka")


def main() -> None:
    from confluent_kafka import Producer

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ensure_topic(config.KAFKA_BOOTSTRAP, config.KAFKA_TOPIC, config.TOPIC_PARTITIONS)

    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "linger.ms": 50, "acks": "all"})
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    def on_delivery(err, msg):
        if err is not None:
            log.error("delivery failed: %s", err)

    sent = 0
    started = time.monotonic()
    log.info("producing to %s at ~%.1f events/sec", config.KAFKA_TOPIC, config.EVENTS_PER_SECOND)

    while running:
        event = build_event()
        key = str(event["user_id"]).encode()

        if random.random() < config.INVALID_RATE:
            value = corrupt_event(event)
        else:
            value = json.dumps(event).encode()

        producer.produce(config.KAFKA_TOPIC, key=key, value=value, on_delivery=on_delivery)
        if random.random() < config.DUPLICATE_RATE:  # at-least-once delivery simulation
            producer.produce(config.KAFKA_TOPIC, key=key, value=value, on_delivery=on_delivery)
        producer.poll(0)

        sent += 1
        if sent % 100 == 0:
            log.info("sent %d events", sent)

        rate = current_rate(config.EVENTS_PER_SECOND, time.monotonic() - started)
        time.sleep(random.expovariate(rate))

    log.info("shutting down, flushing %d queued messages", len(producer))
    producer.flush(10)


if __name__ == "__main__":
    main()
