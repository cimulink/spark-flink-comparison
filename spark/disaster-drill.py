#!/usr/bin/env python3
"""
Disaster Recovery Drill Controller
Simulates failures and measures recovery metrics for Spark streaming system
"""

import os
import time
import json
import subprocess
import requests
from datetime import datetime, timedelta
from prometheus_client.parser import text_string_to_metric_families

class DisasterDrillController:
    def __init__(self):
        self.metrics_file = "./metrics/spark_metrics.txt"
        self.drill_results = []

    def run_drill(self, failure_type="worker", duration_seconds=60):
        """
        Execute disaster recovery drill

        Args:
            failure_type: Type of failure to simulate ("worker", "master", "kafka")
            duration_seconds: How long to keep the service down
        """
        print(f"\n{'='*60}")
        print(f"DISASTER RECOVERY DRILL - {failure_type.upper()} FAILURE")
        print(f"{'='*60}")

        drill_start = datetime.now()

        # 1. Capture baseline metrics
        baseline_metrics = self.capture_metrics()
        print(f"Baseline captured at {drill_start}")

        # 2. Simulate failure
        print(f"\n🚨 Simulating {failure_type} failure...")
        failure_start = datetime.now()

        if failure_type == "worker":
            self.stop_service("spark-worker-1")
        elif failure_type == "master":
            self.stop_service("spark-master")
        elif failure_type == "kafka":
            self.stop_service("kafka")

        # 3. Wait for specified duration
        print(f"⏳ Keeping {failure_type} down for {duration_seconds} seconds...")
        time.sleep(duration_seconds)

        # 4. Restore service
        print(f"\n🔧 Restoring {failure_type} service...")
        recovery_start = datetime.now()

        if failure_type == "worker":
            self.start_service("spark-worker-1")
        elif failure_type == "master":
            self.start_service("spark-master")
        elif failure_type == "kafka":
            self.start_service("kafka")

        # 5. Wait for recovery and measure
        print("⏳ Waiting for system recovery...")
        recovery_time = self.wait_for_recovery(failure_type)
        recovery_end = datetime.now()

        # 6. Capture post-recovery metrics
        time.sleep(10)  # Allow metrics to stabilize
        post_recovery_metrics = self.capture_metrics()

        drill_end = datetime.now()

        # 7. Calculate and report results
        results = self.calculate_drill_results(
            failure_type, drill_start, drill_end, failure_start,
            recovery_start, recovery_end, baseline_metrics, post_recovery_metrics
        )

        self.drill_results.append(results)
        self.report_results(results)

        return results

    def stop_service(self, service_name):
        """Stop a Docker service"""
        try:
            subprocess.run(["docker", "stop", service_name], check=True, capture_output=True)
            print(f"✅ Stopped {service_name}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to stop {service_name}: {e}")

    def start_service(self, service_name):
        """Start a Docker service"""
        try:
            subprocess.run(["docker", "start", service_name], check=True, capture_output=True)
            print(f"✅ Started {service_name}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to start {service_name}: {e}")

    def wait_for_recovery(self, failure_type, max_wait=300):
        """Wait for system to recover and return recovery time"""
        recovery_start = time.time()

        while time.time() - recovery_start < max_wait:
            try:
                if failure_type in ["worker", "master"]:
                    # Check Spark UI
                    response = requests.get("http://localhost:8080", timeout=5)
                    if response.status_code == 200:
                        recovery_time = time.time() - recovery_start
                        print(f"✅ System recovered in {recovery_time:.1f} seconds")
                        return recovery_time
                elif failure_type == "kafka":
                    # Check if we can list Kafka topics
                    result = subprocess.run(
                        ["docker", "exec", "kafka", "kafka-topics",
                         "--bootstrap-server", "localhost:9092", "--list"],
                        capture_output=True, timeout=10
                    )
                    if result.returncode == 0:
                        recovery_time = time.time() - recovery_start
                        print(f"✅ Kafka recovered in {recovery_time:.1f} seconds")
                        return recovery_time

            except (requests.RequestException, subprocess.TimeoutExpired):
                pass

            time.sleep(5)

        print(f"⚠️ System did not recover within {max_wait} seconds")
        return max_wait

    def capture_metrics(self):
        """Capture current system metrics"""
        metrics = {
            'timestamp': datetime.now(),
            'orders_processed': 0,
            'processing_latency': 0,
            'orders_per_second': 0
        }

        try:
            if os.path.exists(self.metrics_file):
                with open(self.metrics_file, 'r') as f:
                    content = f.read()
                    for family in text_string_to_metric_families(content):
                        if family.name == 'orders_processed_total':
                            metrics['orders_processed'] = family.samples[0].value
                        elif family.name == 'order_processing_latency_seconds':
                            metrics['processing_latency'] = family.samples[0].value
                        elif family.name == 'orders_per_second':
                            metrics['orders_per_second'] = family.samples[0].value
        except Exception as e:
            print(f"⚠️ Could not capture metrics: {e}")

        return metrics

    def calculate_drill_results(self, failure_type, drill_start, drill_end,
                               failure_start, recovery_start, recovery_end,
                               baseline_metrics, post_recovery_metrics):
        """Calculate business impact metrics from the drill"""

        downtime_duration = (recovery_start - failure_start).total_seconds()
        recovery_time = (recovery_end - recovery_start).total_seconds()
        total_drill_time = (drill_end - drill_start).total_seconds()

        # Calculate potential lost orders during downtime
        avg_orders_per_second = baseline_metrics.get('orders_per_second', 10)
        potentially_lost_orders = int(downtime_duration * avg_orders_per_second)

        # Calculate revenue impact (assuming average order value of $200)
        avg_order_value = 200
        potential_revenue_loss = potentially_lost_orders * avg_order_value

        results = {
            'drill_type': failure_type,
            'drill_start': drill_start,
            'drill_end': drill_end,
            'total_drill_duration_seconds': total_drill_time,
            'downtime_duration_seconds': downtime_duration,
            'recovery_time_seconds': recovery_time,
            'potentially_lost_orders': potentially_lost_orders,
            'potential_revenue_loss_usd': potential_revenue_loss,
            'baseline_metrics': baseline_metrics,
            'post_recovery_metrics': post_recovery_metrics,
            'data_integrity_check': self.check_data_integrity()
        }

        return results

    def check_data_integrity(self):
        """Check if data integrity is maintained after recovery"""
        try:
            # Check Kafka topic for message continuity
            result = subprocess.run([
                "docker", "exec", "kafka", "kafka-console-consumer",
                "--bootstrap-server", "localhost:9092",
                "--topic", "orders",
                "--from-beginning",
                "--timeout-ms", "5000"
            ], capture_output=True, text=True)

            if result.returncode == 0:
                message_count = len([line for line in result.stdout.split('\n') if line.strip()])
                return {
                    'status': 'PASS',
                    'message': f'Found {message_count} messages in Kafka topic',
                    'messages_in_topic': message_count
                }
            else:
                return {
                    'status': 'FAIL',
                    'message': 'Could not verify message count in Kafka topic',
                    'messages_in_topic': 0
                }
        except Exception as e:
            return {
                'status': 'ERROR',
                'message': f'Data integrity check failed: {e}',
                'messages_in_topic': 0
            }

    def report_results(self, results):
        """Generate a detailed report of drill results"""
        print(f"\n{'='*60}")
        print("DISASTER RECOVERY DRILL RESULTS")
        print(f"{'='*60}")
        print(f"Drill Type: {results['drill_type'].upper()}")
        print(f"Start Time: {results['drill_start']}")
        print(f"End Time: {results['drill_end']}")
        print(f"Total Duration: {results['total_drill_duration_seconds']:.1f} seconds")

        print(f"\n📊 BUSINESS IMPACT METRICS:")
        print(f"  • Downtime Duration: {results['downtime_duration_seconds']:.1f} seconds")
        print(f"  • Recovery Time: {results['recovery_time_seconds']:.1f} seconds")
        print(f"  • Potentially Lost Orders: {results['potentially_lost_orders']}")
        print(f"  • Potential Revenue Loss: ${results['potential_revenue_loss_usd']:,.2f}")

        integrity = results['data_integrity_check']
        print(f"\n🔍 DATA INTEGRITY:")
        print(f"  • Status: {integrity['status']}")
        print(f"  • Details: {integrity['message']}")

        print(f"\n📈 PERFORMANCE COMPARISON:")
        baseline = results['baseline_metrics']
        post_recovery = results['post_recovery_metrics']
        print(f"  • Baseline Orders/sec: {baseline.get('orders_per_second', 'N/A')}")
        print(f"  • Post-Recovery Orders/sec: {post_recovery.get('orders_per_second', 'N/A')}")

        # Save results to file
        results_file = f"drill_results_{results['drill_type']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w') as f:
            # Convert datetime objects to strings for JSON serialization
            results_json = results.copy()
            results_json['drill_start'] = results['drill_start'].isoformat()
            results_json['drill_end'] = results['drill_end'].isoformat()
            results_json['baseline_metrics']['timestamp'] = results['baseline_metrics']['timestamp'].isoformat()
            results_json['post_recovery_metrics']['timestamp'] = results['post_recovery_metrics']['timestamp'].isoformat()
            json.dump(results_json, f, indent=2)

        print(f"\n💾 Results saved to: {results_file}")

    def run_comprehensive_drill(self):
        """Run a comprehensive set of disaster recovery drills"""
        print("🚀 Starting Comprehensive Disaster Recovery Drill Suite")

        drill_scenarios = [
            ("worker", 30),   # Worker failure for 30 seconds
            ("worker", 60),   # Worker failure for 1 minute
            ("kafka", 45),    # Kafka failure for 45 seconds
        ]

        for failure_type, duration in drill_scenarios:
            print(f"\n⏭️ Next: {failure_type} failure for {duration} seconds")
            input("Press Enter to continue or Ctrl+C to stop...")
            self.run_drill(failure_type, duration)
            print("\n⏳ Waiting 60 seconds before next drill...")
            time.sleep(60)

        # Generate summary report
        self.generate_summary_report()

    def generate_summary_report(self):
        """Generate a summary report of all drills"""
        if not self.drill_results:
            print("No drill results to summarize")
            return

        print(f"\n{'='*60}")
        print("COMPREHENSIVE DRILL SUMMARY")
        print(f"{'='*60}")

        total_potential_loss = sum(r['potential_revenue_loss_usd'] for r in self.drill_results)
        avg_recovery_time = sum(r['recovery_time_seconds'] for r in self.drill_results) / len(self.drill_results)

        print(f"Total Drills Executed: {len(self.drill_results)}")
        print(f"Total Potential Revenue Loss: ${total_potential_loss:,.2f}")
        print(f"Average Recovery Time: {avg_recovery_time:.1f} seconds")

        print(f"\n📋 DRILL BREAKDOWN:")
        for i, result in enumerate(self.drill_results, 1):
            print(f"  {i}. {result['drill_type'].upper()}: "
                  f"{result['recovery_time_seconds']:.1f}s recovery, "
                  f"${result['potential_revenue_loss_usd']:,.2f} potential loss")

if __name__ == "__main__":
    drill_controller = DisasterDrillController()

    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == "comprehensive":
            drill_controller.run_comprehensive_drill()
        elif sys.argv[1] in ["worker", "master", "kafka"]:
            duration = int(sys.argv[2]) if len(sys.argv) > 2 else 60
            drill_controller.run_drill(sys.argv[1], duration)
        else:
            print("Usage: python disaster-drill.py [comprehensive|worker|master|kafka] [duration_seconds]")
    else:
        print("Interactive mode - choose drill type:")
        print("1. Worker failure")
        print("2. Kafka failure")
        print("3. Comprehensive drill suite")

        choice = input("Enter choice (1-3): ")
        if choice == "1":
            duration = int(input("Duration in seconds (default 60): ") or "60")
            drill_controller.run_drill("worker", duration)
        elif choice == "2":
            duration = int(input("Duration in seconds (default 60): ") or "60")
            drill_controller.run_drill("kafka", duration)
        elif choice == "3":
            drill_controller.run_comprehensive_drill()
        else:
            print("Invalid choice")