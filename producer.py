import io
import random
import time
import uuid

import fastavro
from confluent_kafka import Producer

TOPIC = "orders"
BOOTSTRAP_SERVERS = "localhost:9092"
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]

# Load the Avro schema once at startup
with open("order.avsc", "r") as f:
    SCHEMA = fastavro.schema.parse_schema(fastavro.schema.load_schema("order.avsc"))


def encode_order(order: dict) -> bytes:
    """Serialize a Python dict into Avro binary bytes using the schema."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, SCHEMA, order)
    return buf.getvalue()


def make_random_order() -> dict:
    return {
        "orderId": str(uuid.uuid4())[:8],
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def delivery_report(err, msg):
    """Called by Kafka client for every message once it's acked or fails."""
    if err is not None:
        print(f"[PRODUCER] Delivery failed for {msg.key()}: {err}")
    else:
        print(f"[PRODUCER] Delivered to {msg.topic()} [partition {msg.partition()}]")


def send_with_retry(producer: Producer, order: dict):
    payload = encode_order(order)
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            producer.produce(
                TOPIC,
                key=order["orderId"],
                value=payload,
                callback=delivery_report,
            )
            producer.poll(0)  # trigger delivery callbacks
            return
        except BufferError:
            attempt += 1
            print(f"[PRODUCER] Queue full, retry {attempt}/{MAX_RETRIES}")
            time.sleep(RETRY_DELAY_SECONDS)
    print(f"[PRODUCER] Giving up on order {order['orderId']} after {MAX_RETRIES} retries")


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    print(f"[PRODUCER] Starting. Sending orders to topic '{TOPIC}'... (Ctrl+C to stop)")

    try:
        while True:
            order = make_random_order()
            print(f"[PRODUCER] Sending: {order}")
            send_with_retry(producer, order)
            time.sleep(1)  # one order per second, easy to watch during a live demo
    except KeyboardInterrupt:
        print("\n[PRODUCER] Stopping...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
