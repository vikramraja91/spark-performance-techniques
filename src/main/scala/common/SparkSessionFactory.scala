package common

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def createFailSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(s"$appName-FAIL")
      .master("local[4]")  // 4 cores to simulate multiple executors
      .config("spark.executor.memory", "512m")  // INSUFFICIENT - will cause OOM
      .config("spark.executor.memoryOverhead", "512m")  // Too small
      .config("spark.memory.fraction", "0.6")  // Default, conservative
      .config("spark.memory.storageFraction", "0.5")
      .config("spark.sql.shuffle.partitions", "200")  // May create large partitions
      .config("spark.default.parallelism", "200")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")  // Disable auto broadcast
      .config("spark.ui.enabled", "true")
      .config("spark.ui.port", "4040")
      .getOrCreate()
  }

  def createPassSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(s"$appName-PASS")
      .master("local[4]")
      .config("spark.executor.memory", "8g")  // ADEQUATE memory
      .config("spark.executor.memoryOverhead", "2g")  // 20% overhead
      .config("spark.memory.fraction", "0.8")  // More memory for execution/storage
      .config("spark.memory.storageFraction", "0.3")  // Favor execution over storage
      .config("spark.memory.offHeap.enabled", "true")  // Reduce GC pressure
      .config("spark.memory.offHeap.size", "2g")
      .config("spark.sql.shuffle.partitions", "100")  // Right-sized partitions
      .config("spark.default.parallelism", "100")
      .config("spark.sql.adaptive.enabled", "true")  // Enable AQE
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrationRequired", "false")
      .config("spark.ui.enabled", "true")
      .config("spark.ui.port", "4040")
      .getOrCreate()
  }

  /**
   * Use local-cluster mode to simulate real executor JVMs
   * Format: local-cluster[numWorkers, coresPerWorker, memoryPerWorker]
   */
  def createLocalClusterFailSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(s"$appName-LocalCluster-FAIL")
      .master("local-cluster[4,2,2048]")  // 4 workers, 2 cores each, 2GB RAM each
      .config("spark.executor.memoryOverhead", "512m")
      .config("spark.memory.fraction", "0.6")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.ui.enabled", "true")
      .config("spark.ui.port", "4040")
      .getOrCreate()
  }

  def createLocalClusterPassSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(s"$appName-LocalCluster-PASS")
      .master("local-cluster[4,4,8192]")  // 4 workers, 4 cores each, 8GB RAM each
      .config("spark.executor.memoryOverhead", "2g")
      .config("spark.memory.fraction", "0.8")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "2g")
      .config("spark.sql.shuffle.partitions", "100")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.ui.enabled", "true")
      .config("spark.ui.port", "4040")
      .getOrCreate()
  }

  /**
   * Production-like configuration for EMR r7g.12xlarge
   * 79 executors × 5 cores × 20GB = 395 cores, 1.6TB memory
   */
  def createProductionSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(s"$appName-Production")
      .master("yarn")
      .config("spark.executor.instances", "79")
      .config("spark.executor.cores", "5")
      .config("spark.executor.memory", "20g")
      .config("spark.driver.memory", "16g")
      .config("spark.executor.memoryOverhead", "4g")  // 20% of 20GB
      .config("spark.driver.memoryOverhead", "2g")
      .config("spark.memory.fraction", "0.8")
      .config("spark.memory.storageFraction", "0.3")
      .config("spark.sql.shuffle.partitions", "800")  // ~10x cores
      .config("spark.default.parallelism", "800")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.shuffle.compress", "true")
      .config("spark.shuffle.spill.compress", "true")
      .config("spark.io.compression.codec", "snappy")
      .getOrCreate()
  }
}
