"""PyFlink streaming job (Table API)

Reads JSON messages from Kafka topic `input-topic`, filters rows where `is_fraudulent == 1`,
and writes them as JSON to `output-topic`.

Submit with:
  docker cp jobs/stream_processor.py jobmanager:/tmp/stream_processor.py
  docker exec -it jobmanager flink run -py /tmp/stream_processor.py

Notes:
- The Flink cluster (inside Docker Compose) should be able to reach Kafka at `kafka:9093` (the INSIDE listener).
- If your Kafka bootstrap address differs, edit the DDLs below.
"""
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Source: Kafka topic `input-topic` (JSON format)
    t_env.execute_sql("""
    CREATE TABLE input_table (
        click_id STRING,
        `timestamp` STRING,
        user_id STRING,
        ip_address STRING,
        device_type STRING,
        browser STRING,
        operating_system STRING,
        referrer_url STRING,
        page_url STRING,
        click_duration DOUBLE,
        scroll_depth INT,
        mouse_movement INT,
        keystrokes_detected INT,
        ad_position STRING,
        click_frequency INT,
        time_since_last_click INT,
        device_ip_reputation STRING,
        VPN_usage INT,
        proxy_usage INT,
        bot_likelihood_score DOUBLE,
        is_fraudulent INT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'input-topic',
        'properties.bootstrap.servers' = 'kafka:9093',
        'properties.group.id' = 'flink-group',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'
    )
    """)

    # Sink: Kafka topic `output-topic` (JSON format)
    t_env.execute_sql("""
    CREATE TABLE output_table (
        click_id STRING,
        `timestamp` STRING,
        user_id STRING,
        ip_address STRING,
        device_type STRING,
        browser STRING,
        operating_system STRING,
        referrer_url STRING,
        page_url STRING,
        click_duration DOUBLE,
        scroll_depth INT,
        mouse_movement INT,
        keystrokes_detected INT,
        ad_position STRING,
        click_frequency INT,
        time_since_last_click INT,
        device_ip_reputation STRING,
        VPN_usage INT,
        proxy_usage INT,
        bot_likelihood_score DOUBLE,
        is_fraudulent INT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'output-topic',
        'properties.bootstrap.servers' = 'kafka:9093',
        'format' = 'json'
    )
    """)

    # Simple transformation: filter frauds
    t_env.execute_sql(
        "INSERT INTO output_table SELECT * FROM input_table WHERE is_fraudulent = 1"
    )

    # keep the job running
    t_env.execute("fraud-filter-job")


if __name__ == '__main__':
    main()
