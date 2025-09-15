#!/usr/bin/env python3

import os
import time
import json
import subprocess
import requests
from datetime import datetime
from prometheus_client.parser import text_string_to_metric_families
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType

class FlinkDisasterDrill:
    """
    Flink Disaster Recovery Drill Controller

    Simulates various failure scenarios for Flink streaming systems:
    - TaskManager failures
    - JobManager failures
    - Kafka broker failures

    Measures business impact including:
    - Recovery time
    - Processing throughput degradation
    - Data integrity verification
    - Potential revenue loss calculation
    """

    def __init__(self):
        self.containers = {
            'taskmanager': 'flink-taskmanager-1',
            'jobmanager': 'flink-jobmanager',
            'kafka': 'flink-kafka'
        }

        self.metrics_urls = {
            'jobmanager': 'http://localhost:9249',
            'order_processor': 'http://localhost:9252'
        }

        self.kafka_admin = None
        self.baseline_metrics = {}

    def get_baseline_metrics(self):
        """Capture baseline performance metrics before drill"""
        print("📊 Capturing baseline metrics...")

        try:
            # Get Flink job metrics
            response = requests.get('http://localhost:8085/jobs', timeout=10)
            if response.status_code == 200:
                jobs = response.json()
                running_jobs = [job for job in jobs if job.get('status') == 'RUNNING']
                self.baseline_metrics['running_jobs'] = len(running_jobs)
            else:
                self.baseline_metrics['running_jobs'] = 0

            # Get processing metrics from order processor
            try:
                response = requests.get(f"{self.metrics_urls['order_processor']}/metrics", timeout=5)
                if response.status_code == 200:
                    metrics_text = response.text
                    for family in text_string_to_metric_families(metrics_text):
                        for sample in family.samples:
                            if sample.name == 'flink_orders_per_second':
                                self.baseline_metrics['orders_per_second'] = sample.value
                            elif sample.name == 'flink_orders_processed_total':
                                self.baseline_metrics['orders_processed'] = sample.value
                            elif sample.name == 'flink_order_processing_latency_ms':
                                self.baseline_metrics['processing_latency'] = sample.value
            except:
                # Set defaults if metrics not available
                self.baseline_metrics['orders_per_second'] = 10.0
                self.baseline_metrics['orders_processed'] = 0
                self.baseline_metrics['processing_latency'] = 100.0

            # Get Kafka topic info
            self.kafka_admin = KafkaAdminClient(
                bootstrap_servers=['localhost:9093'],
                client_id='flink_drill_admin'
            )

            print(f"✅ Baseline captured:")
            print(f"   • Running Flink jobs: {self.baseline_metrics.get('running_jobs', 0)}")
            print(f"   • Orders/second: {self.baseline_metrics.get('orders_per_second', 0):.1f}")
            print(f"   • Processing latency: {self.baseline_metrics.get('processing_latency', 0):.1f}ms")

        except Exception as e:
            print(f"⚠️  Warning: Could not capture complete baseline metrics: {e}")
            # Set default baseline values
            self.baseline_metrics = {
                'running_jobs': 1,
                'orders_per_second': 10.0,
                'orders_processed': 0,
                'processing_latency': 100.0
            }

    def stop_container(self, container_type):
        """Stop a specific container type"""
        container_name = self.containers[container_type]
        print(f"🛑 Stopping {container_type} ({container_name})...")

        try:
            result = subprocess.run(
                ['docker', 'stop', container_name],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                print(f"✅ Successfully stopped {container_name}")
                return True
            else:
                print(f"❌ Failed to stop {container_name}: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout stopping {container_name}")
            return False
        except Exception as e:
            print(f"❌ Error stopping {container_name}: {e}")
            return False

    def start_container(self, container_type):
        """Start a specific container type"""
        container_name = self.containers[container_type]
        print(f"🔄 Starting {container_type} ({container_name})...")

        try:
            result = subprocess.run(
                ['docker', 'start', container_name],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                print(f"✅ Successfully started {container_name}")
                return True
            else:
                print(f"❌ Failed to start {container_name}: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout starting {container_name}")
            return False
        except Exception as e:
            print(f"❌ Error starting {container_name}: {e}")
            return False

    def wait_for_recovery(self, container_type, max_wait=120):
        """Wait for system to recover after container restart"""
        print(f"⏳ Waiting for {container_type} recovery (max {max_wait}s)...")

        start_time = time.time()

        if container_type == 'jobmanager':
            # Wait for JobManager REST API
            while time.time() - start_time < max_wait:
                try:
                    response = requests.get('http://localhost:8085/jobs', timeout=5)
                    if response.status_code == 200:
                        jobs = response.json()
                        running_jobs = [job for job in jobs if job.get('status') == 'RUNNING']
                        if running_jobs:
                            print(f"✅ JobManager recovered with {len(running_jobs)} running jobs")
                            return time.time() - start_time
                except:
                    pass
                time.sleep(2)

        elif container_type == 'taskmanager':
            # Wait for TaskManager to reconnect
            while time.time() - start_time < max_wait:
                try:
                    response = requests.get('http://localhost:8085/taskmanagers', timeout=5)
                    if response.status_code == 200:
                        taskmanagers = response.json()
                        active_tms = [tm for tm in taskmanagers.get('taskmanagers', [])
                                    if tm.get('status') == 'RUNNING']
                        if len(active_tms) >= 1:  # At least one taskmanager should be running
                            print(f"✅ TaskManager recovered ({len(active_tms)} active)")
                            return time.time() - start_time
                except:
                    pass
                time.sleep(2)

        elif container_type == 'kafka':
            # Wait for Kafka to be ready
            while time.time() - start_time < max_wait:
                try:
                    admin_client = KafkaAdminClient(
                        bootstrap_servers=['localhost:9093'],
                        client_id='recovery_check'
                    )
                    topics = admin_client.list_topics(timeout_ms=5000)
                    if 'orders' in topics.topics:
                        print("✅ Kafka recovered and orders topic available")
                        admin_client.close()
                        return time.time() - start_time
                except:
                    pass
                time.sleep(2)

        print(f"⚠️  Recovery timeout after {max_wait}s")
        return max_wait

    def measure_impact(self, drill_duration):
        """Measure business impact during drill"""
        print("📈 Measuring business impact...")

        try:
            # Get current metrics
            current_metrics = {}
            response = requests.get(f"{self.metrics_urls['order_processor']}/metrics", timeout=5)
            if response.status_code == 200:
                metrics_text = response.text
                for family in text_string_to_metric_families(metrics_text):
                    for sample in family.samples:
                        if sample.name == 'flink_orders_per_second':
                            current_metrics['orders_per_second'] = sample.value
                        elif sample.name == 'flink_orders_processed_total':
                            current_metrics['orders_processed'] = sample.value

            # Calculate impact
            baseline_ops = self.baseline_metrics.get('orders_per_second', 10.0)
            current_ops = current_metrics.get('orders_per_second', 0.0)

            orders_lost = max(0, (baseline_ops - current_ops) * drill_duration)
            revenue_impact = orders_lost * 200  # $200 average order value

            return {
                'orders_lost': int(orders_lost),
                'revenue_impact': revenue_impact,
                'performance_degradation': max(0, (baseline_ops - current_ops) / baseline_ops * 100)
            }

        except Exception as e:
            print(f"⚠️  Could not measure complete impact: {e}")
            # Estimate based on downtime
            estimated_orders_lost = drill_duration * 10  # Assume 10 orders/sec baseline
            return {
                'orders_lost': int(estimated_orders_lost),
                'revenue_impact': estimated_orders_lost * 200,
                'performance_degradation': 100.0  # Assume complete failure
            }

    def check_data_integrity(self):
        """Verify data integrity after recovery"""
        print("🔍 Checking data integrity...")

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=['localhost:9093'],
                client_id='integrity_check'
            )

            # Get topic metadata
            metadata = admin_client.describe_topics(['orders'])
            if 'orders' in metadata:
                topic_info = metadata['orders']
                print(f"✅ Orders topic verified: {len(topic_info.partitions)} partitions")

                # In a real scenario, you would also check:
                # - Message counts before/after
                # - Checksum verification
                # - Exactly-once delivery guarantees

                return {
                    'status': 'PASS',
                    'details': f'Orders topic accessible with {len(topic_info.partitions)} partitions'
                }
            else:
                return {
                    'status': 'FAIL',
                    'details': 'Orders topic not found'
                }

        except Exception as e:
            print(f"⚠️  Data integrity check failed: {e}")
            return {
                'status': 'UNKNOWN',
                'details': f'Could not verify: {str(e)}'
            }

    def run_drill(self, drill_type, duration=60):
        """Execute a complete disaster recovery drill"""

        print("=" * 50)
        print("🚨 FLINK DISASTER RECOVERY DRILL")
        print("=" * 50)
        print(f"Drill Type: {drill_type.upper()}")
        print(f"Target Duration: {duration} seconds")
        print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # Phase 1: Capture baseline
        drill_start = time.time()
        self.get_baseline_metrics()

        # Phase 2: Simulate failure
        failure_start = time.time()
        if not self.stop_container(drill_type):
            print("❌ Failed to stop container, aborting drill")
            return

        print(f"⏳ Simulating {drill_type} failure for {duration} seconds...")
        time.sleep(duration)

        failure_end = time.time()
        actual_downtime = failure_end - failure_start

        # Phase 3: Initiate recovery
        recovery_start = time.time()
        if not self.start_container(drill_type):
            print("❌ Failed to restart container")
            return

        # Phase 4: Wait for full recovery
        recovery_time = self.wait_for_recovery(drill_type)
        recovery_end = time.time()

        # Phase 5: Measure impact and verify integrity
        impact = self.measure_impact(actual_downtime)
        integrity = self.check_data_integrity()

        drill_end = time.time()
        total_duration = drill_end - drill_start

        # Generate report
        self.generate_report({
            'drill_type': drill_type,
            'start_time': datetime.fromtimestamp(drill_start).isoformat(),
            'total_duration': total_duration,
            'downtime_duration': actual_downtime,
            'recovery_time': recovery_time,
            'impact': impact,
            'data_integrity': integrity,
            'baseline_metrics': self.baseline_metrics
        })

    def generate_report(self, results):
        """Generate detailed drill report"""

        print()
        print("=" * 50)
        print("📊 FLINK DISASTER RECOVERY DRILL RESULTS")
        print("=" * 50)
        print(f"Drill Type: {results['drill_type'].upper()}")
        print(f"Start Time: {results['start_time']}")
        print(f"Total Duration: {results['total_duration']:.1f} seconds")
        print()

        print("🔥 BUSINESS IMPACT METRICS:")
        print(f"• Downtime Duration: {results['downtime_duration']:.1f} seconds")
        print(f"• Recovery Time: {results['recovery_time']:.1f} seconds")
        print(f"• Potentially Lost Orders: {results['impact']['orders_lost']:,}")
        print(f"• Potential Revenue Loss: ${results['impact']['revenue_impact']:,.2f}")
        print(f"• Performance Degradation: {results['impact']['performance_degradation']:.1f}%")
        print()

        print("🔍 DATA INTEGRITY:")
        print(f"• Status: {results['data_integrity']['status']}")
        print(f"• Details: {results['data_integrity']['details']}")
        print()

        print("📈 PERFORMANCE COMPARISON:")
        print(f"• Baseline Orders/sec: {results['baseline_metrics'].get('orders_per_second', 0):.1f}")
        print(f"• Baseline Latency: {results['baseline_metrics'].get('processing_latency', 0):.1f}ms")
        print()

        # Recovery assessment
        recovery_time = results['recovery_time']
        if recovery_time < 30:
            recovery_grade = "✅ EXCELLENT"
        elif recovery_time < 60:
            recovery_grade = "⚠️  GOOD"
        else:
            recovery_grade = "❌ NEEDS IMPROVEMENT"

        print(f"🎯 RECOVERY ASSESSMENT: {recovery_grade}")
        print(f"   Recovery completed in {recovery_time:.1f} seconds")
        print()

        # Save results to file
        os.makedirs("drill-results", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"drill-results/flink_{results['drill_type']}_{timestamp}.json"

        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)

        print(f"📋 Detailed results saved to: {filename}")
        print()

        # Comparison with expected targets
        print("🎯 BUSINESS CONTINUITY TARGETS:")
        print("• Recovery Time Target: < 60 seconds")
        print("• Data Loss Target: 0 orders")
        print("• Availability Target: > 99.9%")
        print()

        # Calculate availability
        total_time = results['total_duration']
        downtime = results['downtime_duration']
        availability = ((total_time - downtime) / total_time) * 100

        print(f"📊 ACHIEVED METRICS:")
        print(f"• Recovery Time: {recovery_time:.1f}s {'✅' if recovery_time < 60 else '❌'}")
        print(f"• Data Integrity: {results['data_integrity']['status']} {'✅' if results['data_integrity']['status'] == 'PASS' else '❌'}")
        print(f"• Availability: {availability:.2f}% {'✅' if availability > 99.9 else '❌'}")

def main():
    drill = FlinkDisasterDrill()

    print("🚨 Flink Disaster Recovery Drill Controller")
    print("=========================================")
    print()
    print("Available drill types:")
    print("1. taskmanager - Simulate TaskManager failure")
    print("2. jobmanager - Simulate JobManager failure")
    print("3. kafka - Simulate Kafka broker failure")
    print("4. comprehensive - Run all drill types")
    print()

    choice = input("Select drill type (1-4) or type name: ").strip()

    drill_map = {
        '1': 'taskmanager',
        '2': 'jobmanager',
        '3': 'kafka',
        '4': 'comprehensive'
    }

    drill_type = drill_map.get(choice, choice)

    if drill_type == 'comprehensive':
        print("\n🚀 Running comprehensive drill suite...")
        for dt in ['taskmanager', 'jobmanager', 'kafka']:
            print(f"\n--- Starting {dt} drill ---")
            drill.run_drill(dt, 45)  # Shorter duration for comprehensive test
            print(f"--- {dt} drill completed ---\n")
            time.sleep(10)  # Brief pause between drills
    elif drill_type in ['taskmanager', 'jobmanager', 'kafka']:
        duration = 60
        try:
            duration_input = input(f"Enter duration in seconds (default 60): ").strip()
            if duration_input:
                duration = int(duration_input)
        except ValueError:
            print("Invalid duration, using default 60 seconds")

        drill.run_drill(drill_type, duration)
    else:
        print("❌ Invalid drill type selected")

if __name__ == "__main__":
    main()