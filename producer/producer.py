#!/usr/bin/env python3
"""
Simple CSV-to-Kafka producer
Reads `click_fraud_dataset.csv` and publishes one JSON message per line to a Kafka topic.
"""
import argparse
import csv
import json
import time
from kafka import KafkaProducer
from pathlib import Path


def iter_rows(csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # normalize numeric types
            try:
                row["click_duration"] = float(row.get("click_duration", "0") or 0)
            except Exception:
                row["click_duration"] = None
            for int_field in ("scroll_depth", "mouse_movement", "keystrokes_detected", "click_frequency", "time_since_last_click", "VPN_usage", "proxy_usage", "is_fraudulent"):
                if row.get(int_field) is not None:
                    try:
                        row[int_field] = int(row[int_field])
                    except Exception:
                        row[int_field] = None
            try:
                row["bot_likelihood_score"] = float(row.get("bot_likelihood_score", "0") or 0)
            except Exception:
                row["bot_likelihood_score"] = None

            yield row


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--file", "-f", default="click_fraud_dataset.csv", help="Path to CSV file")
    p.add_argument("--broker", "-b", default="localhost:9092", help="Kafka bootstrap server")
    p.add_argument("--topic", "-t", default="input-topic", help="Kafka topic to publish to")
    p.add_argument("--rate", "-r", type=float, default=100.0, help="Messages per second (use 0 for max speed)")
    p.add_argument("--loop", action="store_true", help="Loop over the file continuously")
    p.add_argument("--key-field", default="click_id", help="CSV column to use as Kafka key")
    args = p.parse_args()

    csv_path = Path(args.file)
    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")

    producer = KafkaProducer(bootstrap_servers=[args.broker],
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                             key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else None,
                             linger_ms=5)

    batch_sleep = 0 if args.rate <= 0 else 1.0 / args.rate

    try:
        while True:
            for row in iter_rows(csv_path):
                key = row.get(args.key_field)
                producer.send(args.topic, key=key, value=row)
                # keep throughput reasonable
                if batch_sleep:
                    time.sleep(batch_sleep)
            producer.flush()
            if not args.loop:
                break
    except KeyboardInterrupt:
        print("Interrupted — stopping producer")
    finally:
        producer.close()
