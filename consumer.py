import io
import random

import fastavro
from confluent_kafka import Consumer, Producer

TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "order-aggregator"
MAX_RETRIES = 3

SCHEMA = fastavro.schema.parse_schema(fastavro.schema.load_schema("order.avsc"))

# Running average state
total_price = 0.0
count = 0


def decode_order(payload: bytes) -> dict:
    buf = io.BytesIO(payload)
    return fastavro.schemaless_reader(buf, SCHEMA)


def process_order(order: dict):

    if random.random() < 0.15:
        raise RuntimeError("Simulated temporary processing failure")

    global total_price, count
    total_price += order["price"]
    count += 1
    running_avg = total_price / count
    print(
        f"[CONSUMER] Processed {order['orderId']} "
        f"(price={order['price']:.2f}) -> running average = {running_avg:.2f} "
        f"over {count} orders"
    )


def send_to_dlq(dlq_producer: Producer, raw_payload: bytes, order_id: str, reason: str):
    print(f"[CONSUMER] Sending order {order_id} to DLQ. Reason: {reason}")
    dlq_producer.produce(DLQ_TOPIC, key=order_id, value=raw_payload)
    dlq_producer.flush()


def handle_message(dlq_producer: Producer, raw_payload: bytes):
    order = decode_order(raw_payload)
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            process_order(order)
            return
        except Exception as e:
            attempt += 1
            print(f"[CONSUMER] Failed to process {order['orderId']} "
                  f"(attempt {attempt}/{MAX_RETRIES}): {e}")

    send_to_dlq(dlq_producer, raw_payload, order["orderId"], "max retries exceeded")


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print(f"[CONSUMER] Listening on topic '{TOPIC}'... (Ctrl+C to stop)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[CONSUMER] Kafka error: {msg.error()}")
                continue

            handle_message(dlq_producer, msg.value())
    except KeyboardInterrupt:
        print("\n[CONSUMER] Stopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
