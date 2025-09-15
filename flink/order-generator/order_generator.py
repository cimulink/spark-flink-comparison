#!/usr/bin/env python3

import os
import json
import time
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

class OrderGenerator:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.orders_per_second = int(os.getenv('ORDERS_PER_SECOND', '10'))
        self.topic = 'orders'

        # Initialize Faker for generating realistic data
        self.fake = Faker()

        # Initialize Kafka producer with retry configuration
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: str(x).encode('utf-8'),
            retries=5,
            retry_backoff_ms=300,
            request_timeout_ms=30000,
            acks='all'  # Wait for all replicas to acknowledge
        )

    def generate_order(self):
        """Generate a realistic customer order"""
        products = [
            {"id": "LAPTOP001", "name": "Gaming Laptop", "base_price": 1200},
            {"id": "PHONE001", "name": "Smartphone", "base_price": 800},
            {"id": "TABLET001", "name": "Tablet", "base_price": 400},
            {"id": "HEADPHONES001", "name": "Wireless Headphones", "base_price": 150},
            {"id": "MONITOR001", "name": "4K Monitor", "base_price": 300},
            {"id": "KEYBOARD001", "name": "Mechanical Keyboard", "base_price": 100},
            {"id": "MOUSE001", "name": "Gaming Mouse", "base_price": 60}
        ]

        regions = ["US-EAST", "US-WEST", "EU-CENTRAL", "ASIA-PACIFIC"]

        product = random.choice(products)
        quantity = random.randint(1, 5)
        # Add some price variation
        price_variation = random.uniform(0.9, 1.1)
        price = round(product["base_price"] * price_variation, 2)

        order = {
            "order_id": str(uuid.uuid4()),
            "customer_id": f"CUSTOMER_{random.randint(1000, 9999)}",
            "product_id": product["id"],
            "quantity": quantity,
            "price": price,
            "timestamp": datetime.now().isoformat(),
            "region": random.choice(regions)
        }

        return order

    def create_kafka_topic(self):
        """Create the orders topic if it doesn't exist"""
        from kafka.admin import KafkaAdminClient, NewTopic

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_servers,
                client_id='order_generator_admin'
            )

            topic_list = [NewTopic(
                name=self.topic,
                num_partitions=3,
                replication_factor=1
            )]

            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"Topic '{self.topic}' created successfully")

        except Exception as e:
            print(f"Topic creation info: {e}")  # Topic might already exist

        finally:
            if 'admin_client' in locals():
                admin_client.close()

    def start_generating(self):
        """Start generating and sending orders to Kafka"""
        print(f"Starting Flink order generation at {datetime.now()}")
        print(f"Target rate: {self.orders_per_second} orders/second")
        print(f"Kafka servers: {self.kafka_servers}")
        print(f"Topic: {self.topic}")

        # Create topic
        self.create_kafka_topic()

        # Wait a bit for topic to be ready
        time.sleep(5)

        order_count = 0
        start_time = time.time()

        try:
            while True:
                batch_start = time.time()

                # Generate orders for this second
                for _ in range(self.orders_per_second):
                    order = self.generate_order()

                    # Send to Kafka with order_id as key for even distribution
                    future = self.producer.send(
                        self.topic,
                        key=order['order_id'],
                        value=order
                    )

                    # Optional: Add callback for monitoring
                    future.add_callback(self.on_send_success)
                    future.add_errback(self.on_send_error)

                    order_count += 1

                # Ensure we maintain the target rate
                elapsed = time.time() - batch_start
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)

                # Log progress every 30 seconds
                if order_count % (self.orders_per_second * 30) == 0:
                    total_elapsed = time.time() - start_time
                    actual_rate = order_count / total_elapsed
                    print(f"[FLINK] Generated {order_count} orders in {total_elapsed:.1f}s "
                          f"(actual rate: {actual_rate:.1f}/s)")

        except KeyboardInterrupt:
            print(f"\nStopping Flink order generation. Total orders generated: {order_count}")
        except Exception as e:
            print(f"Error generating orders: {e}")
        finally:
            self.producer.flush()
            self.producer.close()

    def on_send_success(self, record_metadata):
        """Callback for successful message send"""
        pass  # Could log successful sends if needed

    def on_send_error(self, exception):
        """Callback for failed message send"""
        print(f"Failed to send message: {exception}")

if __name__ == "__main__":
    # Wait a bit for Kafka to be ready
    print("Waiting for Kafka to be ready...")
    time.sleep(10)

    generator = OrderGenerator()
    generator.start_generating()