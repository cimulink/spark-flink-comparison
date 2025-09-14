#!/bin/bash

echo "Starting Spark Order Processor with Kafka integration..."

# Use spark-submit with Kafka packages
spark-submit \
    --master $SPARK_MASTER \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    --conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoint \
    --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    /app/order_processor.py