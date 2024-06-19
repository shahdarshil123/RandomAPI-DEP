FROM apache/airflow:2.7.1-python3.11

USER root

# Define Spark version
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Install required packages
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget && \
    rm -rf /var/lib/apt/lists/*

# Download and install Apache Spark
RUN wget -qO- "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xzf - -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

USER airflow
RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark kafka-python