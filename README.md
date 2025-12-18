# Click Fraud Streaming (Flink + Kafka)

This workspace provides a simple end-to-end example to stream `click_fraud_dataset.csv` through Kafka and process it with a PyFlink streaming job that filters fraudulent clicks.

## Components

- `producer/producer.py` — Python script that reads the CSV and publishes JSON messages to Kafka `input-topic`.
- `jobs/stream_processor.py` — PyFlink streaming job (Table API) that reads from `input-topic`, filters `is_fraudulent == 1`, and writes to `output-topic`.
- `consumer/consumer.py` — Simple verification consumer that reads from `output-topic` and prints messages.

## Setup (quick)

1. Start the Docker Compose stack (your `env-compose.yml` already contains Flink, Zookeeper and Kafka):

   docker-compose -f env-compose.yml up -d

2. Build producer container (optional) or run locally

   # build image (optional)
   docker build -t click-producer -f producer/Dockerfile producer/

   # run locally (recommended for quick tests)
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r producer/requirements.txt
   python producer/producer.py --file click_fraud_dataset.csv --broker localhost:9092 --rate 200

   # or run the container and mount data
   docker run --rm -v %cd%:/data -e BROKER=localhost:9092 click-producer --file /data/click_fraud_dataset.csv --broker $BROKER --rate 200

3. Submit the Flink job

   # copy job into jobmanager container and submit
   docker cp jobs/stream_processor.py jobmanager:/tmp/stream_processor.py
   docker exec -it jobmanager flink run -py /tmp/stream_processor.py

   Notes: the job's Kafka DDL points to `kafka:9093` (internal network). If you use a different network config, edit `jobs/stream_processor.py`.

4. Verify output

   pip install -r consumer/requirements.txt
   python consumer/consumer.py --broker localhost:9092 --topic output-topic --max 10

5. Cleanup

   docker-compose -f env-compose.yml down

## Tips

- If the Flink job fails with missing Kafka connector errors, ensure the Flink image (`my-flink`) includes the Kafka connector JARs. If not, add them to the image or use Flink SQL Gateway with the kafka-connector available.
- The included `flink.Dockerfile` installs Python and PyFlink and adds the Kafka connector; rebuild the custom image and recreate Flink containers with:

  docker build -t my-flink -f flink.Dockerfile .
  docker-compose -f env-compose.yml up -d --force-recreate jobmanager taskmanager

- Adjust producer `--rate` for higher throughput. Use `--loop` to stream continuously.

---

If you'd like, I can (pick one):
- Add a Docker Compose service for the producer so it's fully containerized
- Extend the Flink job to compute per-IP aggregates or sliding-window counts
- Add a small test script to verify end-to-end automatically

Tell me which of those you'd like next and I will implement it.
