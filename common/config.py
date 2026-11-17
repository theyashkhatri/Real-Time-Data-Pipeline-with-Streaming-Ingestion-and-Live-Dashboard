"""Central configuration, read from environment variables with local-dev defaults."""
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "orders-processor")

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN", "postgresql://pipeline:pipeline@localhost:5432/analytics"
)

# Producer behaviour
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5"))
INVALID_RATE = float(os.getenv("INVALID_RATE", "0.03"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.01"))
TOPIC_PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "3"))

# Consumer batching
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
BATCH_TIMEOUT_SECONDS = float(os.getenv("BATCH_TIMEOUT_SECONDS", "2"))
