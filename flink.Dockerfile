FROM flink:1.19.0-scala_2.12-java17

USER root

# Install Python, pip, and PyFlink; add Kafka connector JARs.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip wget ca-certificates \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && pip3 install --no-cache-dir apache-flink==1.19.0 py4j \
    && wget -q -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar || true \
    && wget -q -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar || true \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && chown -R flink:flink /opt/flink/lib

USER flink