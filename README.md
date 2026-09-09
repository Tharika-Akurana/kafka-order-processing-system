# Kafka Order Processing System - Assignment

A Kafka-based system that produces and consumes Avro-encoded order
messages, with real-time average calculation, retry logic, and a
Dead Letter Queue (DLQ).

## What's in this project

| File | Purpose |
|---|---|
| `docker-compose.yml` | Runs Kafka + Zookeeper locally |
| `order.avsc` | Avro schema for the order message |
| `producer.py` | Sends random order messages (Avro-encoded), with retry on send failure |
| `consumer.py` | Reads orders, keeps a running average, retries failures, sends dead ones to DLQ |
| `dlq_viewer.py` | Lets you watch the DLQ topic live, for your demo |
| `requirements.txt` | Python packages needed |

## Step 1 : Install prerequisites

You need:
1. **Docker Desktop** : https://www.docker.com/products/docker-desktop/
2. **Python 3.9+** : check with `python3 --version`

## Step 2 : Start Kafka locally

In the project folder, run:

```bash
docker compose up -d
```

Wait about 20–30 seconds for Kafka to fully start. Check it's running:

```bash
docker ps
```

You should see `kafka` and `zookeeper` containers listed.

## Step 3 : Set up Python

Create a virtual environment and install dependencies:

```bash
python3 -m venv venv
source venv/bin/activate        # on Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Step 4 : Run the producer

In one terminal (with the venv activated):

```bash
python3 producer.py
```

You'll see it printing a new random order every second, e.g.:
```
[PRODUCER] Sending: {'orderId': 'a1b2c3d4', 'product': 'Item3', 'price': 231.5}
[PRODUCER] Delivered to orders [partition 0]
```

## Step 5 : Run the consumer

In a **second terminal**:

```bash
source venv/bin/activate
python3 consumer.py
```

You'll see it decoding orders and printing the running average:
```
[CONSUMER] Processed a1b2c3d4 (price=231.50) -> running average = 231.50 over 1 orders
[CONSUMER] Processed 9f8e7d6c (price=88.20) -> running average = 159.85 over 2 orders
```

Occasionally you'll see a simulated failure and retries:
```
[CONSUMER] Failed to process 55aa66bb (attempt 1/3): Simulated temporary processing failure
[CONSUMER] Failed to process 55aa66bb (attempt 2/3): Simulated temporary processing failure
[CONSUMER] Failed to process 55aa66bb (attempt 3/3): Simulated temporary processing failure
[CONSUMER] Sending order 55aa66bb to DLQ. Reason: max retries exceeded
```

## Step 6 : Show the DLQ working

In a **third terminal**:

```bash
source venv/bin/activate
python3 dlq_viewer.py
```

This proves to your marker that failed messages really end up in the
`orders-dlq` topic instead of being lost.

## Step 7 : For live demo

1. Start Kafka (`docker compose up -d`), wait ~30s.
2. Run `producer.py` in one terminal.
3. Run `consumer.py` in another, point out the running average updating live.
4. Run `dlq_viewer.py` in a third, wait for a simulated failure and show the
   message appearing in the DLQ.
5. Briefly explain: Avro schema (`order.avsc`) defines the message
   structure; `fastavro` encodes/decodes it to compact binary; retry
   logic lives in `send_with_retry()` (producer) and `handle_message()`
   (consumer); DLQ is just another Kafka topic, `orders-dlq`.

## How each requirement is met

- **Avro serialization** — `order.avsc` schema, encoded/decoded with `fastavro`.
- **Real-time aggregation** — running average kept in `consumer.py`, updated per message.
- **Retry logic** — both producer (on send failure) and consumer (on processing failure) retry up to `MAX_RETRIES` times before giving up.
- **Dead Letter Queue** — messages that exhaust retries are published to `orders-dlq`.
- **Live demo + Git repo** — see Steps 6–8 above.

## Notes / things you can change to make it your own

- Swap `fastavro` + manual Kafka client for Confluent's official
  `AvroSerializer` + a Schema Registry if your course wants that
  specifically (more setup, but "textbook" Confluent style).
- Adjust `MAX_RETRIES`, the failure-simulation rate in `process_order()`,
  or the product list to make the demo your own rather than an
  identical copy of anyone else's.
