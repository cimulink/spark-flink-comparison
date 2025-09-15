#!/usr/bin/env python3

import time
import json
import os
import threading
from datetime import datetime
from prometheus_client import CollectorRegistry, Gauge, Counter, start_http_server, write_to_textfile
from kafka import KafkaConsumer
import requests

class FlinkMetricsServer:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.jobmanager_url = os.getenv('JOBMANAGER_RPC_ADDRESS', 'jobmanager')
        self.metrics_file = "/app/metrics/flink_metrics.txt"

        # Initialize Prometheus metrics
        self.registry = CollectorRegistry()
        self.processed_orders = Counter('flink_orders_processed_total', 'Total orders processed by Flink', registry=self.registry)
        self.processing_latency = Gauge('flink_order_processing_latency_ms', 'Order processing latency in milliseconds', registry=self.registry)
        self.orders_per_second = Gauge('flink_orders_per_second', 'Orders processed per second by Flink', registry=self.registry)
        self.total_revenue = Gauge('flink_total_revenue', 'Total revenue processed by Flink', registry=self.registry)
        self.avg_order_value = Gauge('flink_avg_order_value', 'Average order value processed by Flink', registry=self.registry)
        self.high_value_orders = Gauge('flink_high_value_orders', 'Count of high value orders', registry=self.registry)
        self.medium_value_orders = Gauge('flink_medium_value_orders', 'Count of medium value orders', registry=self.registry)
        self.low_value_orders = Gauge('flink_low_value_orders', 'Count of low value orders', registry=self.registry)
        self.job_status = Gauge('flink_job_status', 'Flink job status (1=running, 0=not running)', registry=self.registry)
        self.downtime_duration = Gauge('flink_downtime_duration_seconds', 'System downtime duration', registry=self.registry)
        self.recovery_time = Gauge('flink_recovery_time_seconds', 'Time to recover from failure', registry=self.registry)

        # Start metrics HTTP server
        start_http_server(9249)
        print(f"Flink metrics server started on port 9249 at {datetime.now()}")

    def monitor_job_status(self):
        """Monitor Flink job status via REST API"""
        while True:
            try:
                # Get job overview
                response = requests.get(f"http://{self.jobmanager_url}:8081/jobs/overview", timeout=10)
                if response.status_code == 200:
                    jobs = response.json()
                    running_jobs = [job for job in jobs.get('jobs', []) if job.get('state') == 'RUNNING']
                    self.job_status.set(1 if running_jobs else 0)
                else:
                    self.job_status.set(0)

            except Exception as e:
                print(f"Error monitoring job status: {e}")
                self.job_status.set(0)

            time.sleep(10)

    def consume_flink_output(self):
        """Consume and parse Flink SQL output for metrics"""
        while True:
            try:
                # Note: In a real implementation, you might consume from a metrics topic
                # or parse Flink logs. For this demo, we'll simulate metrics based on
                # the orders topic consumption rate

                consumer = KafkaConsumer(
                    'orders',
                    bootstrap_servers=self.kafka_servers,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id='flink-metrics-monitor',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
                )

                order_count = 0
                start_time = time.time()
                total_revenue = 0
                order_values = []
                high_value_count = 0
                medium_value_count = 0
                low_value_count = 0

                print(f"Starting Flink metrics collection at {datetime.now()}")

                for message in consumer:
                    if message.value:
                        order = message.value
                        order_value = order.get('quantity', 0) * order.get('price', 0)

                        order_count += 1
                        total_revenue += order_value
                        order_values.append(order_value)

                        # Categorize orders
                        if order_value > 1000:
                            high_value_count += 1
                        elif order_value > 100:
                            medium_value_count += 1
                        else:
                            low_value_count += 1

                        # Update metrics every 5 seconds
                        if order_count % 50 == 0:  # Approximately every 5 seconds at 10 orders/sec
                            elapsed_time = time.time() - start_time
                            if elapsed_time > 0:
                                current_ops = order_count / elapsed_time
                                avg_value = total_revenue / order_count if order_count > 0 else 0

                                # Simulate processing latency (in real implementation, this would come from Flink metrics)
                                simulated_latency = 50 + (order_count % 100)  # Simulate 50-150ms latency

                                # Update Prometheus metrics
                                self.processed_orders.inc(50)
                                self.orders_per_second.set(current_ops)
                                self.total_revenue.set(total_revenue)
                                self.avg_order_value.set(avg_value)
                                self.processing_latency.set(simulated_latency)
                                self.high_value_orders.set(high_value_count)
                                self.medium_value_orders.set(medium_value_count)
                                self.low_value_orders.set(low_value_count)

                                # Write metrics to file
                                os.makedirs("/app/metrics", exist_ok=True)
                                write_to_textfile(self.metrics_file, self.registry)

                                print(f"[FLINK METRICS] Processed: {order_count}, OPS: {current_ops:.1f}, "
                                      f"Revenue: ${total_revenue:.2f}, Avg Value: ${avg_value:.2f}")

            except Exception as e:
                print(f"Error in metrics collection: {e}")
                time.sleep(10)

    def start(self):
        """Start all monitoring threads"""
        # Start job status monitoring
        job_thread = threading.Thread(target=self.monitor_job_status, daemon=True)
        job_thread.start()

        # Start metrics collection
        metrics_thread = threading.Thread(target=self.consume_flink_output, daemon=True)
        metrics_thread.start()

        print("Flink metrics server is running...")

        # Keep the main thread alive
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            print("Shutting down Flink metrics server...")

if __name__ == "__main__":
    # Wait for Kafka and Flink to be ready
    print("Waiting for Kafka and Flink to be ready...")
    time.sleep(20)

    server = FlinkMetricsServer()
    server.start()