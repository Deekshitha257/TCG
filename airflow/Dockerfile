
ARG AIRFLOW_IMAGE=apache/airflow:2.9.2-python3.9
FROM ${AIRFLOW_IMAGE}

USER root

# Install build tools, procps (ps), and OpenJDK 17 (headless)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl procps wget unzip openjdk-17-jdk-headless bash \
    && rm -rf /var/lib/apt/lists/*


# Set Java env for OpenJDK 17 on Debian bookworm
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt /tmp/requirements.txt

USER airflow

RUN python -m pip install --no-cache-dir --upgrade pip \
 && python -m pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.9.txt"

USER root

# install a Spark client (prebuilt) into /opt/spark so spark-submit is present
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark/spark-3.4.1-bin-hadoop3

RUN wget -qO /tmp/spark.tgz \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
 && tar -xzf /tmp/spark.tgz -C /opt/ \
 && mkdir -p /opt/spark \
 && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark/spark-3.4.1-bin-hadoop3 \
 && rm /tmp/spark.tgz



ENV PATH="${SPARK_HOME}/bin:${PATH}"


RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins \
    /opt/airflow/spark_jobs /opt/airflow/jars \
    && chown -R airflow:root /opt/airflow

USER airflow
