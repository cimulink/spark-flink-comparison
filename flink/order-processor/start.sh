#!/bin/bash

echo "Starting Flink Order Processor at $(date)"

# Wait for JobManager to be ready
echo "Waiting for JobManager to be ready..."
until curl -f http://jobmanager:8081/jobs > /dev/null 2>&1; do
    echo "JobManager not ready, waiting..."
    sleep 5
done

echo "JobManager is ready, starting metrics server..."

# Start metrics server in background
python3 metrics_server.py &
METRICS_PID=$!

echo "Metrics server started with PID $METRICS_PID"

# Submit the SQL job to Flink
echo "Submitting Flink SQL job..."

# Use Flink SQL Client to submit the job
/opt/flink/bin/sql-client.sh embedded \
    --jar /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.19.jar \
    -f order_processor.sql &

FLINK_JOB_PID=$!

echo "Flink SQL job submitted with PID $FLINK_JOB_PID"

# Wait for either process to exit
wait