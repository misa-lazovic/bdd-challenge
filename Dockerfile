FROM openjdk:11-jre-slim

WORKDIR /app

# Install necessary packages
RUN apt-get update && \
    apt-get install -y wget procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
    && tar xzf spark-3.5.1-bin-hadoop3.tgz \
    && mv spark-3.5.1-bin-hadoop3 /spark \
    && rm spark-3.5.1-bin-hadoop3.tgz

ENV SPARK_HOME=/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Copy application JAR
COPY build/libs/bdd-challenge-1.0.jar /app/bdd.jar

# Set entrypoint for Spark application
ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "--class", "com.bdd.report.ReportGenerator", "--master", "local[*]", "/app/bdd.jar"]
