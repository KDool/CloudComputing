FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    openjdk-8-jdk \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Java
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# Hadoop/Spark
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark

ENV HADOOP_CONF_DIR=/etc/hadoop/conf
ENV YARN_CONF_DIR=/etc/hadoop/conf

ENV PATH=$PATH:$HADOOP_HOME/bin:$SPARK_HOME/bin

# Workspace
RUN mkdir -p /workspace

# App
WORKDIR /app

COPY requirements.txt .

RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY src /app/src

WORKDIR /app/src

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
