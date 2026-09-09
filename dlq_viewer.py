import io
import fastavro
from confluent_kafka import Consumer

DLQ_TOPIC = "orders-dlq"
BOOTSTRAP_SERVERS = "localhost:9092"

SCHEMA = fastavro.schema.parse_schema(fastavro.schema.load_schema("order.avsc"))


def decode_order(payload: bytes) -> dict:
    buf = io.BytesIO(payload)
    return fastavro.schemaless_reader(buf, SCHEMA)


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "dlq-viewer",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([DLQ_TOPIC])

    print(f"[DLQ VIEWER] Watching '{DLQ_TOPIC}'... (Ctrl+C to stop)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[DLQ VIEWER] Error: {msg.error()}")
                continue
            order = decode_order(msg.value())
            print(f"[DLQ VIEWER] Dead-lettered order: {order}")
    except KeyboardInterrupt:
        print("\n[DLQ VIEWER] Stopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
