# 01. Memory Management - Executor OOM

## Problem Statement

Executor OutOfMemory errors are the #1 cause of Spark job failures in production. Unlike driver OOM (which happens with collect() or broadcast), executor OOM occurs during normal data processing when:

1. **Insufficient executor memory** for the data volume
2. **Large shuffles** exhausting execution memory
3. **Wide transformations** requiring significant temporary storage
4. **Memory-intensive operations** like window functions, joins, aggregations

## Scenario

Process **50 million clickstream records** (~10GB in-memory) with complex transformations:
- User session aggregation with `collect_list` (creates large arrays)
- Multi-level aggregations (session → user → ranking)
- Window functions requiring shuffle and sort
- Multiple actions forcing materialization

## Fail Configuration (ExecutorOOMFail.scala)

```scala
--executor-memory 2g                      // INSUFFICIENT
--conf spark.executor.memoryOverhead=512m // Too small
--conf spark.memory.fraction=0.6          // Only 1.2GB for execution
--conf spark.sql.shuffle.partitions=200
```

**Memory Breakdown:**
```
Total Executor Memory: 2GB + 512MB = 2.5GB
  ├─ Execution/Storage (0.6 × 2GB = 1.2GB)
  │    ├─ Execution Memory: 840MB  (shuffles, joins, sorts)
  │    └─ Storage Memory: 360MB    (cache, persist)
  ├─ User Memory (0.4 × 2GB = 800MB)
  └─ Overhead: 512MB (off-heap allocations)

 Problem: Shuffle + window functions need >1.2GB → OOM
```

**Expected Failure:**
```
org.apache.spark.SparkException: Job aborted due to stage failure:
ExecutorLostFailure (executor 2 exited caused by one of the running tasks)
Reason: Container killed by YARN for exceeding memory limits.
2.5 GB of 2.5 GB physical memory used.
```

## Pass Configuration (ExecutorOOMPass.scala)

```scala
--executor-memory 8g                          // ADEQUATE
--conf spark.executor.memoryOverhead=2g       // 20% overhead
--conf spark.memory.fraction=0.8              // More execution memory
--conf spark.memory.storageFraction=0.3
--conf spark.memory.offHeap.enabled=true      // Reduce GC pressure
--conf spark.memory.offHeap.size=2g
--conf spark.sql.shuffle.partitions=100       // Larger partitions
--conf spark.sql.adaptive.enabled=true
```

**Memory Breakdown:**
```
Total Executor Memory: 8GB + 2GB = 10GB
  ├─ Execution/Storage (0.8 × 8GB = 6.4GB)
  │    ├─ Execution Memory: 4.48GB  (70% of 6.4GB)
  │    └─ Storage Memory: 1.92GB    (30% of 6.4GB)
  ├─ User Memory (0.2 × 8GB = 1.6GB)
  ├─ Off-Heap Memory: 2GB (additional execution memory)
  └─ Overhead: 2GB

✅ Solution: 4.48GB + 2GB = 6.48GB for execution → No OOM
```

## Build & Run

### 1. Build the Project

```bash
cd spark-performance-techniques
mvn clean package
```

### 2. Run FAIL Scenario

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
 EXECUTOR OOM FAILURE (Expected)
Error: Java heap space
Root Causes:
  1. Executor memory too small (2GB)
  2. Memory fraction only 0.6 → 1.2GB for execution/storage
  ...
```

### 3. Run PASS Scenario

```bash
spark-submit \
  --master local[4] \
  --executor-memory 8g \
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=2g \
  --conf spark.sql.shuffle.partitions=100 \
  --class "01_memory_management.ExecutorOOMPass" \
  target/spark-performance-techniques-1.0-SNAPSHOT.jar
```

**Expected Output:**
```
✅ SUCCESS! Job completed without OOM.
Duration: 145 seconds
Peak Memory: 6.2 GB / 8 GB (77.5%)
Memory Spill: 0 bytes
```

### 4. Use Local-Cluster Mode (Simulates Real Executors)

```bash
# FAIL scenario with real executor JVMs
spark-submit \
  --master local-cluster[4,2,2048] \  # 4 workers, 2 cores, 2GB each
  --class "01_memory_management.ExecutorOOMFail" \
  target/spark-performance-techniques-1.0-SNAPSHOT.jar

# PASS scenario with adequate memory
spark-submit \
  --master local-cluster[4,4,8192] \  # 4 workers, 4 cores, 8GB each
  --class "01_memory_management.ExecutorOOMPass" \
  target/spark-performance-techniques-1.0-SNAPSHOT.jar
```

## Memory Calculation Formula

### Step 1: Calculate Required Execution Memory

```
Data Size in Memory = Records × Avg Record Size
                    = 50M × 200 bytes = 10GB

Shuffle Buffer Size (per partition) = 128MB default
Number of Partitions = 100
Peak Shuffle Memory = 100 × 128MB = 12.8GB (across all executors)

Required Execution Memory per Executor (4 executors):
  = 12.8GB / 4 = 3.2GB per executor (minimum)
```

### Step 2: Add Storage Memory (for intermediate results)

```
Storage Memory = 20-30% of execution memory
               = 0.3 × 3.2GB = 960MB
```

### Step 3: Calculate Total Executor Memory

```
Execution + Storage = 3.2GB + 0.96GB = 4.16GB
User Memory (20%) = 4.16GB / 0.8 × 0.2 = 1.04GB

Total Heap Memory = 4.16GB / 0.8 = 5.2GB

Add Overhead (20%) = 5.2GB × 0.2 = 1.04GB

Recommended Executor Memory:
  --executor-memory 6g
  --conf spark.executor.memoryOverhead=2g
```

### Step 4: Add Safety Buffer (Production)

```
Production Config (with 20% buffer):
  --executor-memory 8g
  --conf spark.executor.memoryOverhead=2g
  --conf spark.memory.offHeap.size=2g
```

## Monitoring & Debugging

### 1. Check Spark UI (http://localhost:4040)

**Executors Tab:**
- Storage Memory: Used / Available
- Disk Used: Indicates spill
- Failed Tasks: Check for OOM-related failures

**Stages Tab:**
- Shuffle Read/Write: Identify memory-intensive stages
- Spill (Memory/Disk): Indicates insufficient memory

### 2. Look for OOM Indicators

```bash
# Check executor logs
grep -i "OutOfMemoryError\|Container killed\|exceeding memory" spark-logs/*

# Common OOM patterns:
# - "Java heap space"
# - "GC overhead limit exceeded"
# - "Container killed by YARN for exceeding memory limits"
# - "ExecutorLostFailure"
```

### 3. Memory Metrics

```scala
// Add to your code for monitoring
val executorMemoryStatus = spark.sparkContext.getExecutorMemoryStatus
executorMemoryStatus.foreach { case (executor, (used, available)) =>
  println(s"Executor $executor: Used ${used / 1e9}GB / Available ${available / 1e9}GB")
}
```

## Key Learnings

1. **Executor memory >> Driver memory**: Most processing happens on executors
2. **Memory.fraction=0.8**: Maximize execution/storage memory (default 0.6 too conservative)
3. **MemoryOverhead**: Should be 10-20% of executor memory, minimum 384MB
4. **Off-heap memory**: Reduces GC pressure, improves stability
5. **Fewer, larger partitions**: Better memory efficiency than many tiny partitions
6. **AQE**: Dynamically optimizes partitions at runtime
7. **Monitor spill**: Disk spill indicates memory pressure

## Production Checklist

- [ ] Calculate required memory based on data size
- [ ] Set executor memory with 20-30% buffer
- [ ] Configure memoryOverhead (minimum 2GB for large jobs)
- [ ] Enable off-heap memory for stability
- [ ] Set memory.fraction=0.8
- [ ] Tune shuffle partitions (target 128MB per partition)
- [ ] Enable AQE for dynamic optimization
- [ ] Monitor Spark UI for spill and GC time
- [ ] Test with production-scale data locally first
