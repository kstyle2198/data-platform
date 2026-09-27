FROM apache/spark:3.5.7

USER root

# =========================================================
# Java 17
# =========================================================

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=${JAVA_HOME}/bin:${PATH}


# =========================================================
# Versions
# =========================================================

ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV ICEBERG_VERSION=1.11.0

# Spark Kafka Connector
ENV KAFKA_CONNECTOR_VERSION=3.5.7

# Kafka Client
ENV KAFKA_CLIENT_VERSION=3.4.1

# Apache Commons Pool
ENV COMMONS_POOL2_VERSION=2.11.1


# =========================================================
# Hadoop AWS
# =========================================================

RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar


# =========================================================
# AWS SDK
# =========================================================

RUN curl -fL \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar


# =========================================================
# Apache Iceberg
# =========================================================

RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar


# =========================================================
# Spark Kafka Connector
# =========================================================

RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${KAFKA_CONNECTOR_VERSION}/spark-sql-kafka-0-10_2.12-${KAFKA_CONNECTOR_VERSION}.jar \
    -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-${KAFKA_CONNECTOR_VERSION}.jar


RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${KAFKA_CONNECTOR_VERSION}/spark-token-provider-kafka-0-10_2.12-${KAFKA_CONNECTOR_VERSION}.jar \
    -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-${KAFKA_CONNECTOR_VERSION}.jar


# =========================================================
# Kafka Client
# =========================================================

RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar \
    -o /opt/spark/jars/kafka-clients-${KAFKA_CLIENT_VERSION}.jar


# =========================================================
# Apache Commons Pool2
# =========================================================

RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar \
    -o /opt/spark/jars/commons-pool2-${COMMONS_POOL2_VERSION}.jar


# =========================================================
# Python dependencies
# =========================================================

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt


# =========================================================
# Runtime
# =========================================================

USER spark

WORKDIR /opt/spark/work