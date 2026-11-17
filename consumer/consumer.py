"""Stream processor: Kafka -> validate -> PostgreSQL.

Delivery semantics: offsets are committed only AFTER the database transaction
commits (at-least-once). Inserts are idempotent (`ON CONFLICT DO NOTHING`), so
redelivered or duplicated messages never create duplicate rows.

If the database fails, the process exits and Docker restarts it; the consumer
group then resumes from the last committed offset, so nothing is lost.
"""
import logging
import signal
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from psycopg2.extras import execute_values

from common import config
from common.schema import ValidationError, parse_message

log = logging.getLogger("consumer")

INSERT_ORDERS = """
    INSERT INTO orders (order_id, user_id, product, category, quantity, unit_price,
                        total_amount, country, payment_method, status, event_time)
    VALUES %s
    ON CONFLICT (order_id) DO NOTHING
"""

INSERT_DEAD_LETTER = """
    INSERT INTO dead_letter (kafka_topic, kafka_partition, kafka_offset, raw_payload, error_reason)
    VALUES %s
    ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
"""

running = True


def _stop(*_):
    global running
    running = False


def connect_db(dsn: str, retries: int = 30, delay: float = 2.0):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("postgres not ready (%d/%d): %s", attempt, retries, str(exc).strip())
            time.sleep(delay)
    raise RuntimeError("could not connect to PostgreSQL")


def split_batch(messages):
    """Validate messages; return (order_rows, dead_letter_rows)."""
    orders, dead = [], []
    for msg in messages:
        value = msg.value()
        try:
            e = parse_message(value)
        except ValidationError as exc:
            raw = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else None
            dead.append((msg.topic(), msg.partition(), msg.offset(), raw, str(exc)[:500]))
            continue
        orders.append(
            (
                e["order_id"], e["user_id"], e["product"], e["category"], e["quantity"],
                e["unit_price"], e["total_amount"], e["country"], e["payment_method"],
                e["status"], e["event_time"],
            )
        )
    return orders, dead


def write_batch(conn, orders, dead) -> None:
    with conn.cursor() as cur:
        if orders:
            execute_values(cur, INSERT_ORDERS, orders)
        if dead:
            execute_values(cur, INSERT_DEAD_LETTER, dead)
    conn.commit()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    conn = connect_db(config.POSTGRES_DSN)
    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": config.CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # we commit manually after the DB write
        }
    )
    consumer.subscribe([config.KAFKA_TOPIC])
    log.info("consuming %s as group %s", config.KAFKA_TOPIC, config.CONSUMER_GROUP)

    total_ok = total_bad = 0
    try:
        while running:
            batch = consumer.consume(num_messages=config.BATCH_SIZE, timeout=config.BATCH_TIMEOUT_SECONDS)
            messages = []
            for msg in batch:
                if msg.error():
                    # Topic may not exist yet on first start; the producer creates it.
                    if msg.error().code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                        continue
                    raise KafkaException(msg.error())
                messages.append(msg)
            if not messages:
                continue

            orders, dead = split_batch(messages)
            write_batch(conn, orders, dead)
            consumer.commit(asynchronous=False)  # only after the DB commit succeeded

            total_ok += len(orders)
            total_bad += len(dead)
            log.info("batch: %d valid, %d dead-lettered (totals: %d / %d)", len(orders), len(dead), total_ok, total_bad)
    finally:
        log.info("shutting down")
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
