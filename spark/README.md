# Spark Disaster Recovery Drill

This experiment simulates a "disaster recovery drill" to measure business continuity metrics for Apache Spark streaming systems, as outlined in the PRD. The setup processes mock customer orders and measures system resilience during simulated failures.

## 🧠 What Has Been Built

This project creates a complete **real-time order processing system** using Apache Spark that can simulate and measure business impact during system failures. Think of it as a **controlled experiment** to understand how your streaming infrastructure handles disasters.

### 📦 Complete System Components

```
spark/
├── 🐳 docker-compose.yml          # Orchestrates entire infrastructure
├── 📊 prometheus.yml              # Metrics collection configuration
├── 🚨 disaster-drill.py           # Automated failure simulation controller
├── 📖 README.md                   # This comprehensive guide
├── order-generator/               # Simulates customer order stream
│   ├── Dockerfile                 # Container build instructions
│   ├── requirements.txt           # Python dependencies
│   └── order_generator.py         # Generates realistic orders
└── order-processor/               # Spark streaming application
    ├── Dockerfile                 # Container build instructions
    ├── requirements.txt           # Spark + Kafka dependencies
    └── order_processor.py         # Core business logic processor
```

## 🏗️ System Architecture Deep Dive

### The Data Flow Journey
```
🛒 Order Generator → 📨 Kafka Topic → ⚡ Spark Streaming → 📊 Metrics & Monitoring
     (10/sec)           (orders)        (Processing)        (Business KPIs)
```

### 1. **Order Generator Service** 📦
- **What it does**: Simulates a real e-commerce website generating customer orders
- **How it works**: Creates JSON orders with realistic data (customer IDs, products, prices)
- **Business value**: Provides consistent load to test system behavior under predictable conditions
- **Configuration**: Adjustable order rate (default 10 orders/second)

### 2. **Apache Kafka 4.1.0 with KRaft** 📨
- **What it does**: Acts as the "message highway" between order generation and processing
- **How it works**: Stores orders in a durable, distributed log that Spark can read from
- **Business value**: Ensures no orders are lost even during system failures
- **Key feature**: Uses KRaft (no Zookeeper needed) - simpler, more resilient architecture

### 3. **Spark Streaming Cluster** ⚡
- **What it does**: Processes orders in real-time, adding business logic and calculations
- **How it works**:
  - **Master node**: Coordinates the cluster and schedules work
  - **2 Worker nodes**: Actually process the data streams
  - **Processing logic**: Calculates order values, categorizes orders (high/medium/low value)
- **Business value**: Transforms raw orders into actionable business insights

### 4. **Monitoring Stack** 📊
- **Prometheus**: Collects and stores metrics (orders processed, latency, errors)
- **Grafana**: Creates dashboards to visualize system health and performance
- **Custom metrics**: Tracks business-specific KPIs like revenue at risk

### 5. **Disaster Drill Controller** 🚨
- **What it does**: Automatically simulates failures and measures recovery
- **How it works**:
  - Captures baseline performance metrics
  - Stops specific services (workers, Kafka, master)
  - Measures how long recovery takes
  - Calculates business impact (lost orders, revenue risk)
- **Business value**: Provides concrete data for risk assessment and technology decisions

## 🎯 The Complete Workflow

### Phase 1: Normal Operation
1. **Order Generator** creates 10 realistic orders per second
2. **Kafka** receives and stores these orders reliably
3. **Spark Streaming** reads orders and processes them:
   - Calculates total order value (quantity × price)
   - Categorizes orders: high (>$1000), medium ($100-1000), low (<$100)
   - Tracks processing latency
4. **Monitoring** records all metrics for baseline measurement

### Phase 2: Disaster Simulation
1. **Drill Controller** captures current performance metrics
2. **Simulated Failure** - Controller stops a critical service:
   - **Worker failure**: Reduces processing capacity by 50%
   - **Kafka failure**: Stops new order ingestion completely
   - **Master failure**: Brings down entire coordination system
3. **Impact Measurement** - System continues running with degraded capacity
4. **Recovery** - Failed service is restarted
5. **Metrics Collection** - Measures recovery time and data integrity

### Phase 3: Business Impact Analysis
The system calculates:
- **Recovery Time**: How long until full service restoration
- **Lost Revenue**: Orders that couldn't be processed × $200 average value
- **Data Integrity**: Verification that no orders were permanently lost
- **Performance Impact**: Comparison of before/after processing rates

## 🚀 Step-by-Step Execution Guide

### Prerequisites Check
Before starting, ensure you have:
- **Docker Desktop running** (green whale icon in system tray)
- **4GB+ RAM available** (check Task Manager)
- **Required ports free**: 3000, 8080-8082, 9090, 9092

### Step 1: Start Docker Desktop
```bash
# On Windows: Start Menu → Docker Desktop
# Wait for "Docker Desktop is running" notification
docker --version  # Should show version without errors
```

### Step 2: Build the System
```bash
cd spark
# Build custom applications (order generator & processor)
docker-compose build

# Start all services
docker-compose up -d
```

### Step 3: Verify System Health
```bash
# Check all containers are running
docker-compose ps

# Should see 8 services: kafka, spark-master, 2 workers, order-generator, order-processor, prometheus, grafana
# Wait 2-3 minutes for full startup

# Watch startup logs
docker-compose logs -f
```

### Step 4: Access Monitoring Dashboards
Open these URLs in your browser:
- **Spark Master UI**: http://localhost:8080 (see cluster status)
- **Spark Application UI**: http://localhost:4041 (see streaming job details)
- **Prometheus**: http://localhost:9090 (raw metrics data)
- **Grafana**: http://localhost:3000 (visual dashboards, login: admin/admin)

### Step 5: Verify Order Processing
```bash
# Check if orders are being generated
docker-compose logs order-generator

# Check if Spark is processing orders
docker-compose logs order-processor

# Manually verify Kafka has orders
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning --timeout-ms 5000
```

### Step 6: Run Disaster Drills

#### Option A: Interactive Mode (Recommended for first time)
```bash
python disaster-drill.py
# Choose: 1 (Worker failure), 2 (Kafka failure), or 3 (Comprehensive suite)
```

#### Option B: Direct Command
```bash
# Test worker resilience (good starting point)
python disaster-drill.py worker 60

# Test Kafka resilience (high impact)
python disaster-drill.py kafka 45

# Run all scenarios automatically
python disaster-drill.py comprehensive
```

## 📊 Understanding the Results

### Sample Drill Output Explained
```
DISASTER RECOVERY DRILL RESULTS
================================
Drill Type: WORKER                    # Which component was failed
Start Time: 2025-01-15 14:30:00      # When drill started
Total Duration: 95.2 seconds          # Complete drill time

📊 BUSINESS IMPACT METRICS:
• Downtime Duration: 45.2 seconds     # Service was degraded for this long
• Recovery Time: 32.1 seconds         # Time to fully restore after restart
• Potentially Lost Orders: 452        # Orders that couldn't be processed
• Potential Revenue Loss: $90,400.00  # Business impact (452 × $200)

🔍 DATA INTEGRITY:
• Status: PASS                        # No permanent data loss
• Details: Found 1,247 messages       # All orders accounted for in Kafka

📈 PERFORMANCE COMPARISON:
• Baseline Orders/sec: 10.0           # Normal processing rate
• Post-Recovery Orders/sec: 9.8       # Rate after recovery
```

### What These Numbers Mean for Business
- **Recovery Time < 60s**: ✅ Excellent - meets most SLA requirements
- **Recovery Time 60-120s**: ⚠️ Acceptable - may impact customer experience
- **Recovery Time > 120s**: ❌ Poor - significant business risk
- **Data Integrity PASS**: ✅ No orders lost permanently - good for compliance
- **Data Integrity FAIL**: ❌ Orders lost - unacceptable for financial systems

## 🔧 Advanced Configuration

### Increase Order Load (Stress Test)
Edit `docker-compose.yml`:
```yaml
order-generator:
  environment:
    - ORDERS_PER_SECOND=50  # Higher load test
```

### Add More Spark Workers (Scale Test)
Add to `docker-compose.yml`:
```yaml
spark-worker-3:
  image: bitnami/spark:3.4
  container_name: spark-worker-3
  ports:
    - "8083:8081"
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_MEMORY=1G
    - SPARK_WORKER_CORES=1
  # ... copy other settings from spark-worker-1
```

### Modify Business Logic
Edit `order-processor/order_processor.py`:
```python
# Change order categorization thresholds
.withColumn("order_category",
    when(col("order_value") > 2000, "premium")      # Raise high-value threshold
    .when(col("order_value") > 500, "standard")     # Adjust medium threshold
    .otherwise("basic"))
```

## 🚨 Drill Scenarios Explained

### 1. Worker Failure Drill
**What happens**: One Spark worker is stopped
**Real-world equivalent**: Server hardware failure, network partition
**Expected impact**: 50% reduction in processing capacity
**Recovery mechanism**: Spark master redistributes work to remaining worker
**Business lesson**: Importance of redundancy in cluster sizing

### 2. Kafka Failure Drill
**What happens**: Kafka broker is stopped
**Real-world equivalent**: Message queue system outage
**Expected impact**: No new orders can be ingested
**Recovery mechanism**: Orders resume processing when Kafka restarts
**Business lesson**: Message durability vs. availability trade-offs

### 3. Master Failure Drill (Advanced)
**What happens**: Spark master is stopped
**Real-world equivalent**: Control plane outage
**Expected impact**: Complete processing halt
**Recovery mechanism**: Master restart + worker reconnection
**Business lesson**: Single points of failure in distributed systems

## 🐛 Troubleshooting Guide

### Problem: "Docker Desktop not running"
```bash
# Start Docker Desktop application
# Wait for green whale icon in system tray
docker ps  # Should list containers without errors
```

### Problem: "Port already in use"
```bash
# Find what's using the port
netstat -ano | findstr :8080
# Kill the process or change port in docker-compose.yml
```

### Problem: "Kafka won't start"
```bash
# Check if previous Kafka data is corrupted
docker-compose down -v  # Removes volumes
rm -rf kafka-data/      # Clean slate
docker-compose up -d kafka
```

### Problem: "Spark workers won't connect"
```bash
# Check master is accessible
curl http://localhost:8080
# Restart workers if master is healthy
docker-compose restart spark-worker-1 spark-worker-2
```

### Problem: "No orders being generated"
```bash
# Check order generator logs
docker-compose logs order-generator
# Verify Kafka topic exists
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Problem: "Drill script fails"
```bash
# Ensure all containers are running
docker-compose ps
# Check Python dependencies
pip install prometheus-client requests
# Run drill with verbose output
python disaster-drill.py worker 30
```

## 📈 Performance Baselines

### Expected Normal Operation
- **Throughput**: 10 orders/second (configurable)
- **Latency**: < 2 seconds end-to-end processing
- **Resource Usage**: ~2GB RAM, 20% CPU
- **Availability**: 99.9% (brief pauses during processing)

### Expected During Failures
- **Worker Failure**: 50% throughput reduction, 2x latency increase
- **Kafka Failure**: 0% new ingestion, existing orders continue processing
- **Network Issues**: Gradual degradation, automatic retries

### Recovery Time Targets
- **Good**: < 30 seconds (enterprise grade)
- **Acceptable**: 30-60 seconds (standard systems)
- **Poor**: > 60 seconds (needs improvement)

## 🔄 What to Do After Running Experiments

### 1. Document Your Baselines
Record your specific results:
- Average recovery times for each failure type
- Actual vs. expected performance impact
- Any data integrity issues observed

### 2. Analyze Business Impact
Calculate real business metrics:
- Convert "potentially lost orders" to actual revenue numbers
- Consider your peak traffic periods
- Factor in customer satisfaction costs

### 3. Compare with Requirements
- Do recovery times meet your SLA commitments?
- Is data integrity sufficient for compliance needs?
- Can the system handle your expected traffic growth?

### 4. Set Up Flink Comparison
Create a similar setup for Apache Flink to compare:
- Recovery time differences
- Resource utilization patterns
- Operational complexity trade-offs

### 5. Make Technology Decision
Use concrete data to choose between Spark and Flink:
- **Spark advantages**: Mature ecosystem, easier operations, SQL support
- **Flink advantages**: Lower latency, better exactly-once guarantees, advanced state management

## 📝 Results Documentation

The system generates detailed JSON reports:
```json
{
  "drill_type": "worker",
  "downtime_duration_seconds": 45.2,
  "recovery_time_seconds": 32.1,
  "potentially_lost_orders": 452,
  "potential_revenue_loss_usd": 90400.00,
  "data_integrity_check": {
    "status": "PASS",
    "messages_in_topic": 1247
  }
}
```

Use these reports for:
- **Executive presentations**: Clear business impact numbers
- **Architecture decisions**: Concrete performance comparisons
- **Compliance documentation**: Data integrity proof
- **Capacity planning**: Resource requirement validation

## 🎯 Success Criteria

Your disaster recovery drill is successful when you can answer:
1. **"How long until we're back online?"** (Recovery time)
2. **"How much money are we losing?"** (Revenue impact)
3. **"Are we losing any customer orders?"** (Data integrity)
4. **"Should we choose Spark or Flink?"** (Comparison data)

---

**💡 Key Insight**: This isn't just a technical test - it's a business continuity assessment that provides concrete data for technology decisions and risk management.