FROM apache/airflow:2.9.2

USER root

# Установка Java и PostgreSQL client
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      openjdk-17-jdk \
      postgresql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Установка Python пакетов
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    requests \
    clickhouse-driver
