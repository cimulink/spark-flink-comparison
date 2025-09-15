# Apache Flink Disaster Recovery Drill

This experiment implements a **real-time order processing system** using Apache Flink to measure business continuity metrics during simulated failures. This setup provides a comprehensive comparison point against the equivalent Apache Spark implementation.

## 🧠 What Has Been Built

This project creates a complete **event-driven order processing system** using Apache Flink that simulates and measures business impact during system failures. Think of it as a **controlled experiment** to understand how Flink streaming infrastructure handles disasters compared to Spark.

### 📦 Complete System Components

```
flink/
├── 🐳 docker-compose.yml          # Orchestrates entire Flink infrastructure
├── 📊 prometheus.yml              # Metrics collection configuration
├── 🚨 disaster-drill.py           # Automated failure simulation controller
├── 📖 README.md                   # This comprehensive guide
├── order-generator/               # Simulates customer order stream
│   ├── Dockerfile                 # Container build instructions
│   ├── requirements.txt           # Python dependencies
│   └── order_generator.py         # Generates realistic orders
└── order-processor/               # Flink streaming application
    ├── Dockerfile                 # Container build instructions
    ├── requirements.txt           # Dependencies for metrics server
    ├── order_processor.sql        # Flink SQL streaming job
    ├── metrics_server.py          # Custom metrics collection
    └── start.sh                   # Application startup script
```

## 🏗️ Flink Architecture Deep Dive

### The Data Flow Journey
```
🛒 Order Generator → 📨 Kafka Topic → ⚡ Flink SQL Engine → 📊 Metrics & Monitoring
     (10/sec)           (orders)        (Stream Processing)      (Business KPIs)
```

### 1. **Order Generator Service** 📦
- **What it does**: Identical to Spark version - simulates e-commerce order stream
- **How it works**: Creates JSON orders with realistic data (customer IDs, products, prices)
- **Business value**: Provides consistent load for comparing Flink vs Spark performance
- **Configuration**: Adjustable order rate (default 10 orders/second)

### 2. **Apache Kafka 7.4.0 with KRaft** 📨
- **What it does**: Same message broker as Spark setup for fair comparison
- **How it works**: Stores orders in durable, distributed log that Flink can consume
- **Business value**: Ensures no orders lost during system failures
- **Key feature**: Uses KRaft (no Zookeeper) - consistent with Spark setup

### 3. **Flink Streaming Cluster** ⚡
- **What it does**: Processes orders using **Flink SQL** for declarative stream processing
- **How it works**:
  - **JobManager**: Coordinates cluster, schedules tasks, manages checkpoints
  - **2 TaskManagers**: Execute the actual stream processing tasks
  - **Processing logic**: Uses SQL to calculate order values, categorize orders, window aggregations
- **Business value**: Provides lower-latency processing with exactly-once guarantees

### 4. **Flink SQL Processing Engine** 🔍
- **What makes it unique**:
  - **Declarative approach**: Business logic defined in SQL rather than code
  - **Automatic optimization**: Flink optimizes query execution plans
  - **State management**: Automatic state checkpointing and recovery
  - **Windowing**: Built-in support for time-based aggregations

### 5. **Advanced Monitoring Stack** 📊
- **Prometheus**: Collects Flink-specific metrics (checkpointing, backpressure, throughput)
- **Grafana**: Visualizes Flink cluster health and business metrics
- **Custom metrics server**: Bridges Flink output to Prometheus format
- **Flink Web UI**: Native monitoring dashboard for job status and performance

### 6. **Disaster Drill Controller** 🚨
- **What it does**: Simulates Flink-specific failures and measures recovery
- **Flink-specific scenarios**:
  - **TaskManager failure**: Tests task redistribution and state recovery
  - **JobManager failure**: Tests cluster coordination and job restart
  - **Kafka failure**: Tests source resilience and backpressure handling
- **Business value**: Provides data for Spark vs Flink technology decisions

## 🎯 Flink-Specific Features Demonstrated

### 1. **Exactly-Once Processing**
- **Checkpoint mechanism**: Automatic state snapshots every 10 seconds
- **Savepoint capability**: Manual snapshots for upgrades/maintenance
- **Kafka integration**: Transactional guarantees with Kafka producers/consumers

### 2. **Advanced Time Handling**
- **Event time processing**: Orders processed based on actual timestamp
- **Watermarks**: Handle late-arriving events gracefully
- **Time windows**: 5-second tumbling windows for real-time aggregations

### 3. **SQL-First Approach**
- **Declarative logic**: Business rules expressed in standard SQL
- **Real-time joins**: Complex stream processing without custom code
- **Table API**: Seamless integration between batch and stream processing

### 4. **State Management**
- **Fault tolerance**: Automatic recovery from TaskManager failures
- **Scalability**: State distributed across TaskManager instances
- **Performance**: RocksDB backend for large state scenarios

## 🚀 Step-by-Step Execution Guide

### Prerequisites Check
Before starting, ensure you have:
- **Docker Desktop running** (green whale icon in system tray)
- **6GB+ RAM available** (Flink requires more memory than Spark)
- **Required ports free**: 3001, 8085, 6123-6125, 9091, 9093, 9249-9252
- **Previous Spark setup stopped** (to avoid port conflicts)

### Step 1: Stop Spark Environment (If Running)
```bash
cd spark
docker-compose down
cd ../flink
```

### Step 2: Build the Flink System
```bash
cd flink
# Build custom applications (order generator & processor)
docker-compose build

# Start all services
docker-compose up -d
```

### Step 3: Verify System Health
```bash
# Check all containers are running
docker-compose ps

# Should see 8 services: kafka, jobmanager, 2 taskmanagers, order-generator, order-processor, prometheus, grafana
# Wait 3-4 minutes for full startup (Flink takes longer than Spark)

# Watch startup logs
docker-compose logs -f
```

### Step 4: Access Monitoring Dashboards
Open these URLs in your browser:
- **Flink Dashboard**: http://localhost:8085 (Job status, TaskManager health, metrics)
- **Prometheus**: http://localhost:9091 (Raw metrics data)
- **Grafana**: http://localhost:3001 (Visual dashboards, login: admin/admin)

### Step 5: Verify Flink Job Status
```bash
# Check Flink jobs are running
curl http://localhost:8085/jobs

# Check if orders are being generated
docker-compose logs flink-order-generator

# Check Flink SQL job output
docker-compose logs flink-order-processor

# Manually verify Kafka has orders
docker exec flink-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning --timeout-ms 5000
```

### Step 6: Run Flink Disaster Drills

#### Option A: Interactive Mode (Recommended for first time)
```bash
python disaster-drill.py
# Choose: 1 (TaskManager failure), 2 (JobManager failure), or 3 (Kafka failure)
```

#### Option B: Direct Command
```bash
# Test TaskManager resilience (tests Flink's task redistribution)
python disaster-drill.py taskmanager 60

# Test JobManager resilience (tests cluster coordination)
python disaster-drill.py jobmanager 45

# Test Kafka resilience (tests backpressure handling)
python disaster-drill.py kafka 30

# Run comprehensive suite
python disaster-drill.py comprehensive
```

## 📊 Understanding Flink vs Spark Results

### Sample Flink Drill Output Explained
```
FLINK DISASTER RECOVERY DRILL RESULTS
====================================
Drill Type: TASKMANAGER               # Which Flink component failed
Start Time: 2025-01-15 14:30:00      # When drill started
Total Duration: 87.3 seconds          # Complete drill time

🔥 BUSINESS IMPACT METRICS:
• Downtime Duration: 42.1 seconds     # Processing degraded for this long
• Recovery Time: 28.7 seconds         # Time to redistribute tasks
• Potentially Lost Orders: 421        # Orders during degraded performance
• Potential Revenue Loss: $84,200.00  # Business impact (421 × $200)
• Performance Degradation: 45.2%      # Throughput reduction during failure

🔍 DATA INTEGRITY:
• Status: PASS                        # Exactly-once guarantees maintained
• Details: Checkpoint recovery successful, 0 messages lost

📈 PERFORMANCE COMPARISON:
• Baseline Orders/sec: 10.0           # Normal processing rate
• Baseline Latency: 95ms              # Typical Flink processing latency
```

### Flink vs Spark: Expected Differences

| Metric | Flink | Spark | Winner |
|--------|-------|-------|---------|
| **Recovery Time** | 20-40s | 30-60s | 🏆 Flink (faster task redistribution) |
| **Processing Latency** | 50-150ms | 100-300ms | 🏆 Flink (lower latency) |
| **Data Integrity** | Exactly-once | At-least-once | 🏆 Flink (stronger guarantees) |
| **Memory Usage** | Higher | Lower | 🏆 Spark (more efficient) |
| **Operational Complexity** | Higher | Lower | 🏆 Spark (simpler operations) |
| **Failure Impact** | Graceful degradation | More noticeable impact | 🏆 Flink (better resilience) |

## 🔧 Flink-Specific Configuration

### Increase Parallelism (Performance Test)
Edit `docker-compose.yml`:
```yaml
jobmanager:
  environment:
    - FLINK_PROPERTIES=parallelism.default: 4  # Increase parallelism
      taskmanager.numberOfTaskSlots: 4
```

### Enable RocksDB State Backend (Large State Test)
Edit `docker-compose.yml`:
```yaml
jobmanager:
  environment:
    - FLINK_PROPERTIES=state.backend: rocksdb  # For large state
      state.backend.rocksdb.memory.write-buffer-ratio: 0.5
```

### Modify Checkpointing (Fault Tolerance Test)
Edit `order_processor.sql`:
```sql
-- Add checkpoint configuration
SET 'execution.checkpointing.interval' = '5s';
SET 'execution.checkpointing.min-pause' = '2s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
```

### Change Business Logic
Edit `order_processor.sql`:
```sql
-- Modify order categorization in the CASE statement
CASE
    WHEN quantity * price > 2000 THEN 'premium'      -- Raise high-value threshold
    WHEN quantity * price > 500 THEN 'standard'      -- Adjust medium threshold
    ELSE 'basic'
END as order_category
```

## 🚨 Flink Drill Scenarios Explained

### 1. TaskManager Failure Drill
**What happens**: One TaskManager container is stopped
**Real-world equivalent**: Worker node failure, container crash
**Flink's response**:
- Automatic task redistribution to remaining TaskManager
- State recovery from latest checkpoint
- Job continues with reduced parallelism
**Expected impact**: 50% capacity reduction, 20-40s recovery time
**Business lesson**: Importance of adequate TaskManager redundancy

### 2. JobManager Failure Drill
**What happens**: JobManager container is stopped
**Real-world equivalent**: Master node failure, coordination service outage
**Flink's response**:
- Complete job halt (JobManager is single point of failure)
- TaskManagers become orphaned
- Recovery requires JobManager restart + job resubmission
**Expected impact**: Complete processing halt, 45-90s recovery time
**Business lesson**: Need for JobManager HA setup in production

### 3. Kafka Failure Drill
**What happens**: Kafka broker is stopped
**Real-world equivalent**: Message queue system outage
**Flink's response**:
- Source operator enters backpressure mode
- Processing continues for buffered data
- Automatic reconnection when Kafka returns
**Expected impact**: No new data ingestion, graceful degradation
**Business lesson**: Source system resilience patterns

## 🔍 Advanced Flink Monitoring

### Checkpoint Monitoring
```bash
# Check checkpoint status via REST API
curl http://localhost:8085/jobs/{job-id}/checkpoints

# Monitor checkpoint duration and size
docker-compose logs flink-jobmanager | grep checkpoint
```

### Backpressure Detection
```bash
# Check for backpressure via Flink dashboard
# Look for "HIGH" backpressure indicators at http://localhost:8085

# Monitor task metrics
curl http://localhost:8085/jobs/{job-id}/vertices/{vertex-id}/metrics
```

### State Size Monitoring
```bash
# Check state size growth
curl "http://localhost:8085/jobs/{job-id}/vertices/{vertex-id}/subtasks/metrics?get=taskmanager_job_task_operator_rocksdb_size"
```

## 🐛 Flink-Specific Troubleshooting

### Problem: "JobManager cannot be reached"
```bash
# Check JobManager health
curl http://localhost:8085/overview
# Restart if needed
docker-compose restart jobmanager
```

### Problem: "TaskManager not connecting"
```bash
# Check TaskManager logs
docker-compose logs flink-taskmanager-1
# Verify JobManager address is accessible
docker exec flink-taskmanager-1 ping jobmanager
```

### Problem: "Flink SQL job fails to start"
```bash
# Check SQL syntax and connector availability
docker-compose logs flink-order-processor
# Verify Kafka connector JAR is loaded
docker exec flink-jobmanager ls /opt/flink/lib/
```

### Problem: "Checkpoints failing"
```bash
# Check checkpoint directory permissions
docker exec flink-jobmanager ls -la /tmp/flink-checkpoints/
# Increase checkpoint timeout
# Edit docker-compose.yml to add: execution.checkpointing.timeout: 60s
```

### Problem: "High backpressure"
```bash
# Identify bottleneck operators
curl http://localhost:8085/jobs/{job-id}/vertices/{vertex-id}/backpressure
# Scale up TaskManager resources or parallelism
```

## 📈 Flink Performance Baselines

### Expected Normal Operation
- **Throughput**: 10 orders/second (same as Spark for comparison)
- **Latency**: < 100ms end-to-end (faster than Spark)
- **Resource Usage**: ~3GB RAM, 25% CPU (higher than Spark)
- **Availability**: 99.95% (better than Spark due to exactly-once guarantees)

### Expected During Failures
- **TaskManager Failure**: 50% throughput reduction, faster recovery than Spark
- **JobManager Failure**: 0% throughput, longer recovery than Spark worker failure
- **Network Issues**: Better backpressure handling than Spark

### Recovery Time Targets
- **Excellent**: < 25 seconds (Flink's strength)
- **Good**: 25-45 seconds (standard Flink performance)
- **Poor**: > 45 seconds (needs tuning)

## 🔄 What to Do After Running Flink Experiments

### 1. Compare with Spark Results
Direct comparison metrics:
- **Recovery times**: Which system recovers faster?
- **Data integrity**: Compare exactly-once vs at-least-once
- **Operational complexity**: Which is easier to manage?
- **Resource utilization**: Which uses resources more efficiently?

### 2. Flink-Specific Analysis
Evaluate Flink advantages:
- **Lower latency**: Sub-100ms processing vs Spark's 200ms+
- **Exactly-once guarantees**: No duplicate processing during failures
- **Advanced windowing**: Time-based aggregations with late event handling
- **SQL abstraction**: Business logic without Java/Scala code

### 3. Business Decision Criteria
Use these factors to choose:

**Choose Flink when**:
- ✅ Sub-second latency requirements
- ✅ Exactly-once processing mandatory (financial transactions)
- ✅ Complex event time processing needed
- ✅ Team comfortable with SQL-first approach

**Choose Spark when**:
- ✅ Batch + streaming unified platform needed
- ✅ Large existing Spark ecosystem
- ✅ Simpler operational requirements
- ✅ Cost optimization priority (lower resource usage)

### 4. Production Planning
Based on drill results:

**For Flink Production**:
- Plan for JobManager HA setup
- Allocate 2-3x memory vs Spark
- Implement comprehensive checkpoint monitoring
- Train team on Flink SQL and state management

**For Spark Production**:
- Plan for longer recovery times
- Implement duplicate detection for exactly-once semantics
- Focus on cluster resource optimization
- Leverage existing Spark expertise

## 📊 Comparative Results Dashboard

Create a decision matrix:

| Requirement | Weight | Flink Score | Spark Score | Winner |
|-------------|--------|-------------|-------------|---------|
| Latency | High | 9/10 | 6/10 | Flink |
| Data Consistency | High | 10/10 | 7/10 | Flink |
| Operational Simplicity | Medium | 6/10 | 9/10 | Spark |
| Resource Efficiency | Medium | 6/10 | 8/10 | Spark |
| Recovery Time | High | 8/10 | 7/10 | Flink |
| Learning Curve | Low | 6/10 | 8/10 | Spark |

## 🎯 Success Criteria for Flink Evaluation

Your Flink disaster recovery drill is successful when you can answer:

1. **"Does Flink recover faster than Spark?"** (Compare recovery times)
2. **"Are we getting exactly-once guarantees?"** (Data integrity validation)
3. **"What's the latency difference?"** (Performance comparison)
4. **"Is the operational complexity worth it?"** (Cost-benefit analysis)
5. **"Which system fits our requirements better?"** (Final technology decision)

## 📝 Automated Comparison Report

The system can generate comparison reports:
```json
{
  "comparison": {
    "spark_recovery_time": 45.2,
    "flink_recovery_time": 28.7,
    "winner": "flink",
    "improvement": "36.5% faster recovery"
  },
  "recommendation": {
    "technology": "flink",
    "reasoning": "Lower latency and exactly-once guarantees outweigh operational complexity",
    "confidence": "high"
  }
}
```

---

**💡 Key Insight**: This Flink implementation provides a complete comparison framework against Spark, focusing on real-world business scenarios where latency and data consistency requirements drive technology decisions. The side-by-side comparison enables data-driven architecture choices rather than technology preferences.

## 🔗 Related Documentation

- **Spark Comparison**: See `../spark/README.md` for equivalent Spark implementation
- **Architecture Decisions**: Document your technology choice reasoning
- **Production Setup**: Use drill results to plan production deployment
- **Team Training**: Leverage SQL-first approach for broader team adoption