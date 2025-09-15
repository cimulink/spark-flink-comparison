FROM flink:1.19

# Install Python and dependencies for monitoring
RUN apt-get update && apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/flink/jobs

# Copy the Flink SQL job and monitoring script
COPY order_processor.sql .
COPY metrics_server.py .
COPY requirements.txt .

# Install Python dependencies for metrics
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy Flink connector JARs
RUN wget -O /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.19.jar https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.19/flink-sql-connector-kafka-3.1.0-1.19.jar

# Copy startup script
COPY start.sh .
RUN chmod +x start.sh

CMD ["./start.sh"]