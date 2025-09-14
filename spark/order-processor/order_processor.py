#!/usr/bin/env python3

import os
import json
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prometheus_client import CollectorRegistry, Gauge, Counter, start_http_server, write_to_textfile

class OrderProcessor:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.spark_master = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
        self.checkpoint_location = "/tmp/spark-checkpoint"
        self.metrics_file = "/app/metrics/spark_metrics.txt"

        # Initialize Prometheus metrics
        self.registry = CollectorRegistry()
        self.processed_orders = Counter('orders_processed_total', 'Total orders processed', registry=self.registry)
        self.processing_latency = Gauge('order_processing_latency_seconds', 'Order processing latency', registry=self.registry)
        self.orders_per_second = Gauge('orders_per_second', 'Orders processed per second', registry=self.registry)
        self.downtime_duration = Gauge('downtime_duration_seconds', 'System downtime duration', registry=self.registry)
        self.recovery_time = Gauge('recovery_time_seconds', 'Time to recover from failure', registry=self.registry)

        # Start metrics server
        start_http_server(8000)

    def create_spark_session(self):
        """Create and configure Spark session"""
        # When using spark-submit, we get the existing session
        return SparkSession.builder \
            .appName("OrderProcessor-DisasterRecoveryDrill") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()

    def get_order_schema(self):
        """Define order schema"""
        return StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("region", StringType(), True)
        ])

    def process_orders(self):
        """Main order processing logic"""
        spark = self.create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        # Read from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "orders") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # Parse JSON messages
        order_schema = self.get_order_schema()
        orders_df = kafka_df \
            .select(from_json(col("value").cast("string"), order_schema).alias("order")) \
            .select("order.*") \
            .withColumn("processing_time", current_timestamp())

        # Add business logic - calculate order value and categorize
        enriched_orders = orders_df \
            .withColumn("order_value", col("quantity") * col("price")) \
            .withColumn("order_category",
                when(col("order_value") > 1000, "high_value")
                .when(col("order_value") > 100, "medium_value")
                .otherwise("low_value")) \
            .withColumn("processing_latency",
                unix_timestamp(col("processing_time")) - unix_timestamp(col("timestamp")))

        # Write processed orders to console and update metrics
        query = enriched_orders.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .foreachBatch(self.update_metrics) \
            .trigger(processingTime='5 seconds') \
            .start()

        print(f"Order processing started at {datetime.now()}")
        print(f"Kafka servers: {self.kafka_servers}")
        print(f"Spark master: {self.spark_master}")
        print("Monitoring metrics available at http://localhost:8000")

        try:
            query.awaitTermination()
        except Exception as e:
            print(f"Error in stream processing: {e}")
            # Record downtime
            self.downtime_duration.set(time.time())
        finally:
            spark.stop()

    def update_metrics(self, batch_df, batch_id):
        """Update Prometheus metrics for each batch"""
        if batch_df.count() > 0:
            # Count processed orders
            order_count = batch_df.count()
            self.processed_orders.inc(order_count)

            # Calculate average latency
            avg_latency = batch_df.agg(avg("processing_latency")).collect()[0][0]
            if avg_latency:
                self.processing_latency.set(avg_latency)

            # Calculate orders per second (approximate)
            self.orders_per_second.set(order_count / 5)  # 5-second intervals

            print(f"Batch {batch_id}: Processed {order_count} orders, Avg latency: {avg_latency:.2f}s")

            # Write metrics to file for external monitoring
            os.makedirs("/app/metrics", exist_ok=True)
            write_to_textfile(self.metrics_file, self.registry)

            # Log high-value orders for business monitoring
            high_value_orders = batch_df.filter(col("order_category") == "high_value")
            if high_value_orders.count() > 0:
                print(f"Alert: {high_value_orders.count()} high-value orders processed in batch {batch_id}")

if __name__ == "__main__":
    processor = OrderProcessor()
    processor.process_orders()