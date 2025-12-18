
#!/usr/bin/env python3
"""
Simple Kafka consumer for verifying messages on `output-topic`.
"""
import argparse
import json
from kafka import KafkaConsumer


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument("--broker", "-b", default="localhost:9092", help="Kafka bootstrap server")
    p.add_argument("--topic", "-t", default="output-topic", help="Topic to consume from")
    p.add_argument("--group", default="verify-group")
    p.add_argument("--max", type=int, default=0, help="Exit after reading this many messages (0 = infinite)")
    args = p.parse_args()

    consumer = KafkaConsumer(args.topic,
                             bootstrap_servers=[args.broker],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id=args.group,
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    count = 0
    try:
        for msg in consumer:
            print(json.dumps(msg.value, indent=2))
            count += 1
            if args.max and count >= args.max:
                break
    except KeyboardInterrupt:
        print("Stopping consumer")
    finally:
        consumer.close()
