FROM python:3.11-slim

# Install Java + utilities
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk-amd64 \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Hadoop environment
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark

ENV HADOOP_CONF_DIR=/etc/hadoop/conf
ENV YARN_CONF_DIR=/etc/hadoop/conf 

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin

# Create workspace
RUN mkdir -p /workspace

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src /app/src

WORKDIR /app/src

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]