# Spark Performance Techniques - Scala/Maven Edition

Comprehensive Spark performance optimization techniques with **real executor OOM failures** and proven solutions. Test locally with configurable memory before deploying to EMR.

##  Focus: Executor-Level Performance

Unlike driver OOM (which is rare), **executor OOM** is the #1 production issue:
- Large shuffles exhausting executor memory
- Wide transformations with insufficient memory
- Skewed partitions causing single executor failure
- Cache/persist without proper memory allocation

Each module **deliberately causes executor OOM**, then fixes it with proper configuration.

##  Project Structure

```
spark-performance-techniques/
├── pom.xml                                  # Maven configuration
├── src/
│   └── main/
│       ├── scala/
│       │   ├── common/
│       │   │   ├── SparkSessionFactory.scala
│       │   │   ├── DataGenerator.scala
│       │   │   └── PerformanceMonitor.scala
│       │   │
│       │   ├── 01_memory_management/
│       │   │   ├── ExecutorOOMFail.scala
│       │   │   ├── ExecutorOOMPass.scala
│       │   │   └── README.md
│       │   │
│       │   ├── 02_shuffle_optimization/
│       │   │   ├── ShuffleSpillFail.scala
│       │   │   ├── ShuffleSpillPass.scala
│       │   │   └── README.md
│       │   │
│       │   ├── 03_partition_tuning/
│       │   ├── 04_broadcast_joins/
│       │   ├── 05_cache_persist/
│       │   ├── 06_skew_handling/
│       │   ├── 07_aqe_optimization/
│       │   └── 08_spill_management/
│       │
│       └── resources/
│           └── log4j.properties
│
├── scripts/
│   ├── 01-run-memory-fail.sh
│   ├── 01-run-memory-pass.sh
│   ├── 02-run-shuffle-fail.sh
│   ├── 02-run-shuffle-pass.sh
│   └── run-all-tests.sh
│
└── data/                                    # Generated test data
```

##  Quick Start

### Build

```bash
cd spark-performance-techniques
mvn clean package
```

This creates: `target/spark-performance-techniques-1.0-SNAPSHOT.jar`

### Run Fail Scenario (Causes Executor OOM)

```bash
spark-submit \
  --master local[4] \
  --executor-memory 2g \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.memory.fraction=0.6 \
  --class "01_memory_management.ExecutorOOMFail" \
  target/spark-performance-techniques-1.0-SNAPSHOT.jar
```

**Expected Output:**
```
 FAILED: ExecutorLostFailure (executor 2 exited caused by one of the running tasks)
Reason: Container killed by YARN for exceeding memory limits. 
2.5 GB of 2.5 GB physical memory used.
```

### Run Pass Scenario (Succeeds)

```bash
spark-submit \
  --master local[4] \
  --executor-memory 8g \
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=2g \
  --class "01_memory_management.ExecutorOOMPass" \
  target/spark-performance-techniques-1.0-SNAPSHOT.jar
```

**Expected Output:**
```
 SUCCESS
Duration: 145 seconds
Records Processed: 50,000,000
Peak Executor Memory: 6.2 GB / 8 GB (77.5%)
Memory Spill: 0 bytes
```

##  Optimization Techniques

### 01. Memory Management (Executor OOM)
**Scenario**: Wide transformation on 50M records  
**Fail Config**: 2GB executor memory → OOM at 30M records  
**Pass Config**: 8GB executor memory + tuned memory.fraction  
**Learning**: Executor memory calculation, overhead, memory.fraction tuning

### 02. Shuffle Optimization (Spill to Disk)
**Scenario**: GroupBy with 100M records, 1000 keys  
**Fail Config**: Default partitions → 50GB shuffle spill  
**Pass Config**: Optimized partitions + map-side combine → 5GB shuffle  
**Learning**: Shuffle partition sizing, compression, buffer tuning

### 03. Partition Tuning (Task Failures)
**Scenario**: 10GB data in 10,000 tiny partitions  
**Fail Config**: Too many small tasks → scheduler overhead + OOM  
**Pass Config**: Right-sized 200 partitions  
**Learning**: Partition size calculation (128MB target), coalesce vs repartition

### 04. Broadcast Joins (Shuffle OOM)
**Scenario**: Join 50GB table with 500MB dimension  
**Fail Config**: Sort-merge join → 30GB executor shuffle → OOM  
**Pass Config**: Broadcast join → 0 shuffle  
**Learning**: Broadcast threshold tuning, autoBroadcastJoinThreshold

### 05. Cache/Persist (Memory Pressure)
**Scenario**: Iterative job caching 20GB dataset  
**Fail Config**: MEMORY_ONLY → eviction → recompute → OOM  
**Pass Config**: MEMORY_AND_DISK_SER → graceful spill  
**Learning**: Storage level selection, cache memory allocation

### 06. Skew Handling (Single Executor OOM)
**Scenario**: Highly skewed key (1 key = 80% of data)  
**Fail Config**: One executor processes 40GB → OOM  
**Pass Config**: Salting + AQE skew join optimization  
**Learning**: Skew detection, salting technique, AQE skewed join

### 07. Adaptive Query Execution (Suboptimal Memory)
**Scenario**: Multi-stage query with varying partition sizes  
**Fail Config**: Static plan → some executors OOM  
**Pass Config**: AQE dynamically coalesces partitions  
**Learning**: AQE partition coalescing, shuffle optimization

### 08. Spill Management (Disk Thrashing)
**Scenario**: Large sort operation  
**Fail Config**: Insufficient sort buffer → 100GB disk spill → OOM  
**Pass Config**: Tuned shuffle/sort buffers → minimal spill  
**Learning**: Spill detection, buffer sizing, off-heap memory

## Local Testing - Executor Memory Focus

### Simulating Executor OOM Locally

```bash
# Use local-cluster mode to simulate real executors
spark-submit \
  --master local-cluster[4,2,2048] \  # 4 workers, 2 cores each, 2GB RAM each
  --conf spark.executor.memoryOverhead=512m \
  --class "01_memory_management.ExecutorOOMFail" \
  target/spark-performance-techniques-1.0-SNAPSHOT.jar
```

This spawns actual executor JVMs with memory limits, so OOM is real!

### Memory Configuration Patterns

#### Fail Pattern (Designed to OOM)
```bash
--executor-memory 2g                          # Insufficient
--conf spark.executor.memoryOverhead=512m     # Too small (should be 10-20% of executor memory)
--conf spark.memory.fraction=0.6              # Default, conservative
--conf spark.sql.shuffle.partitions=200       # May create large partitions
```

#### Pass Pattern (Optimized)
```bash
--executor-memory 8g                          # Adequate for workload
--conf spark.executor.memoryOverhead=2g       # 20% overhead
--conf spark.memory.fraction=0.8              # More for execution/storage
--conf spark.memory.storageFraction=0.3       # Balance execution vs storage
--conf spark.memory.offHeap.enabled=true      # Reduce GC pressure
--conf spark.memory.offHeap.size=2g
--conf spark.sql.shuffle.partitions=100       # Right-sized partitions
```

### Memory Calculation Formula

```
Total Executor Memory = executor-memory + executor.memoryOverhead

Usable Memory (memory.fraction = 0.8):
  - Execution Memory: 0.8 × 8GB × 0.7 = 4.48 GB  (for shuffles, joins, sorts)
  - Storage Memory: 0.8 × 8GB × 0.3 = 1.92 GB   (for cache, persist)
  - User Memory: 0.2 × 8GB = 1.6 GB             (for user data structures)

Overhead Memory (memoryOverhead = 2GB):
  - Off-heap allocations
  - Internal metadata
  - Native libraries
```

##  Data Generation

```bash
# Build first
mvn clean package

# Generate small dataset (1GB - quick testing)
java -cp target/spark-performance-techniques-1.0-SNAPSHOT.jar \
  common.DataGenerator small 1GB data/small

# Generate medium dataset (10GB - causes OOM in fail scenarios)
java -cp target/spark-performance-techniques-1.0-SNAPSHOT.jar \
  common.DataGenerator medium 10GB data/medium

# Generate large dataset (50GB - realistic benchmarking)
java -cp target/spark-performance-techniques-1.0-SNAPSHOT.jar \
  common.DataGenerator large 50GB data/large
```

## Monitoring Executor Memory

### Spark UI Analysis

Navigate to http://localhost:4040 and check:

1. **Executors Tab**:
   - Storage Memory Used/Available
   - Peak Memory per executor
   - Failed tasks per executor

2. **Stages Tab**:
   - Shuffle Read/Write per executor
   - Spill Memory/Disk
   - GC Time per executor

3. **SQL Tab** (for DataFrame/Dataset operations):
   - Physical plan
   - Memory consumed per operator

### Example Metrics Output

```
=== Executor Memory Analysis ===
Executor ID: 1
  Status:  Completed /  Failed (OOM)
  Peak Memory: 7.2 GB / 8.0 GB (90%)
  Shuffle Read: 12.5 GB
  Shuffle Write: 8.3 GB
  Spill (Memory): 2.1 GB
  Spill (Disk): 5.8 GB
  GC Time: 45 seconds (8% of task time)
  Tasks Failed: 0 / 250
```

##   Config

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 79 \
  --executor-cores 5 \
  --executor-memory 20g \              # Based on 366GB / 79 executors
  --driver-memory 16g \
  --conf spark.executor.memoryOverhead=4g \     # 20% overhead
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=800 \     # ~10x cores
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --class "your.MainClass" \
  target/spark-performance-techniques-1.0-SNAPSHOT.jar
```


