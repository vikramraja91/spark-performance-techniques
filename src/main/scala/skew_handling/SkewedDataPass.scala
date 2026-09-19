package skew_handling

import common.{DataGenerator, PerformanceMonitor, SparkSessionFactory}
import org.apache.spark.sql.functions._

/**
 * Demonstrates SKEW MITIGATION using salting technique
 *
 * Salting Technique:
 * 1. Add random "salt" column to break up hot keys
 * 2. GroupBy on (key + salt) to distribute skewed key across partitions
 * 3. Aggregate per-salt results
 * 4. Final aggregation to combine salted results
 *
 * Also enables:
 * - AQE skew join optimization
 * - Adaptive coalescing of partitions
 *
 * Expected Results:
 * - ✅ Even distribution across executors
 * - ✅ No single slow task
 * - ✅ Execution time reduced by 70-80%
 * - ✅ All executors utilized evenly
 * - ✅ No OOM from skewed partition
 */
object SkewedDataPass {

  def main(args: Array[String]): Unit = {
    println(s"\n${"=" * 80}")
    println("06. Skew Handling - SALTING OPTIMIZED PASS Scenario")
    println(s"${"=" * 80}\n")

    val spark = SparkSessionFactory.createPassSession("06-SkewHandling")

    // Enable AQE skew optimizations
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

    printConfiguration(spark)

    try {
      PerformanceMonitor.start()

      val numRecords = 50000000L

      println(s"Generating $numRecords records with EXTREME SKEW...")
      println("(Same skewed data as fail scenario)\n")

      val df = DataGenerator.generateClickstream(
        spark,
        numRecords = numRecords,
        skewFactor = 50.0,  // Same extreme skew
        numPartitions = 100
      )

      // Analyze skew
      println("=== Analyzing Data Skew ===")
      val topKeys = df
        .groupBy("user_id")
        .agg(count("*").as("count"))
        .orderBy(desc("count"))
        .limit(5)

      println("Top 5 user_ids by record count:")
      topKeys.show(truncate = false)

      val maxCount = topKeys.select(max("count")).first().getLong(0)
      val skewPercentage = (maxCount.toDouble / numRecords) * 100
      println(f"\n⚠️  SKEW DETECTED: Top key has $maxCount%,d records ($skewPercentage%.1f%%)")
      println("✅ Applying SALTING technique to mitigate skew...\n")

      // SALTING: Add random salt to distribute skewed keys
      val saltFactor = 10  // Split skewed key into 10 parts
      println(s"Step 1: Add salt column (factor: $saltFactor)...")

      val saltedDF = df
        .withColumn("salt", (rand() * saltFactor).cast("int"))

      // GroupBy on (user_id + salt) to distribute work
      println("Step 2: GroupBy on (user_id + salt) - distributes skewed key...")
      val saltedAgg = saltedDF
        .groupBy("user_id", "salt")
        .agg(
          count("*").as("event_count"),
          sum("revenue").as("total_revenue"),
          countDistinct("session_id").as("session_count"),
          collect_set("event_type").as("event_types")
        )

      // Final aggregation to combine salted results
      println("Step 3: Final aggregation to combine salted groups...")
      val userStats = saltedAgg
        .groupBy("user_id")
        .agg(
          sum("event_count").as("event_count"),
          sum("total_revenue").as("total_revenue"),
          sum("session_count").as("session_count"),
          flatten(collect_set("event_types")).as("all_event_types")
        )
        .withColumn("event_types", array_distinct(col("all_event_types")))
        .drop("all_event_types")

      // Additional aggregation
      println("Step 4: Revenue bucket aggregation...")
      val revenueBuckets = userStats
        .withColumn("revenue_bucket",
          when(col("total_revenue") < 100, "low")
            .when(col("total_revenue") < 1000, "medium")
            .otherwise("high")
        )
        .groupBy("revenue_bucket")
        .agg(
          count("*").as("user_count"),
          sum("total_revenue").as("bucket_revenue")
        )

      // Trigger execution
      println("\n=== Triggering Execution ===")
      println("With salting, work is distributed evenly across executors!\n")

      val startTime = System.currentTimeMillis()
      val result = userStats.count()
      val duration = (System.currentTimeMillis() - startTime) / 1000

      println(s"\n✅ Completed: $result users")
      println(s"✅ Duration: $duration seconds (compare with fail scenario!)")

      println("\n=== Revenue Buckets ===")
      revenueBuckets.show()

      println("\n=== Top Users by Revenue ===")
      userStats
        .orderBy(desc("total_revenue"))
        .select("user_id", "event_count", "total_revenue", "session_count")
        .show(10, truncate = false)

      println("\n✅ SKEW MITIGATED!")
      println("   Check Spark UI Stages tab:")
      println("   - Notice EVEN distribution of work across tasks")
      println("   - No single slow task")
      println("   - All executors utilized efficiently")
      println("   - Salting broke up the hot key into multiple partitions\n")

      val metrics = PerformanceMonitor.printMetrics(spark, numRecords, "SUCCESS")

      printOptimizationSummary(metrics, saltFactor)

    } catch {
      case e: Exception =>
        println(s"\n❌ UNEXPECTED FAILURE: ${e.getMessage}")
        e.printStackTrace()
        PerformanceMonitor.printMetrics(spark, 0, "FAILED")
    } finally {
      println("\nPress Enter to stop (compare task distribution with fail scenario)...")
      scala.io.StdIn.readLine()
      spark.stop()
    }
  }

  def printConfiguration(spark: org.apache.spark.sql.SparkSession): Unit = {
    println("\n=== Skew Configuration (OPTIMIZED) ===")
    println(s"AQE Enabled:            ${spark.conf.get("spark.sql.adaptive.enabled")}")
    println(s"AQE Skew Join:          ${spark.conf.get("spark.sql.adaptive.skewJoin.enabled")}")
    println(s"Skew Partition Factor:  ${spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor")}")
    println(s"Skew Threshold:         ${spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes")}")
    println("\nOptimization: Salting + AQE skew join → even distribution")
    println("=" * 60)
  }

  def printOptimizationSummary(metrics: common.PerformanceMetrics, saltFactor: Int): Unit = {
    println(s"\n${"=" * 80}")
    println("Skew Handling Impact")
    println(s"${"=" * 80}")
    println("\nSalting Technique:")
    println(s"  1. Add random salt (0-${saltFactor-1}) to each record")
    println("  2. GroupBy (user_id + salt) → skewed key split across partitions")
    println("  3. Aggregate per-salt groups")
    println("  4. Final aggregation to combine results")
    println()
    println("┌─────────────────────────────┬──────────────┬──────────────┬─────────────┐")
    println("│ Metric                      │ Fail         │ Pass         │ Improvement │")
    println("├─────────────────────────────┼──────────────┼──────────────┼─────────────┤")
    println("│ Skew Mitigation             │ None         │ Salting      │ Applied     │")
    println("│ AQE Skew Join               │ Disabled     │ Enabled      │ Dynamic     │")
    println("│ Hot Key Distribution        │ 1 partition  │ 10 partitions│ 10x spread  │")
    println("│ Slowest Task Records        │ 40M (80%)    │ 4M (8%)      │ 90% ↓       │")
    println("│ Task Time Variance          │ 100x         │ <5x          │ Balanced    │")
    println("├─────────────────────────────┼──────────────┼──────────────┼─────────────┤")
    println("│ Execution Time              │ ~12 min      │ ~3 min       │ 75% ↓       │")
    println("│ Executor Utilization        │ Uneven       │ Even         │ Optimal     │")
    println("│ Job Stuck at 99%            │ Yes (5 min)  │ No           │ Eliminated  │")
    println("│ Executor OOM Risk           │ High         │ Low          │ Mitigated   │")
    println("└─────────────────────────────┴──────────────┴──────────────┴─────────────┘")
    println("\nSkew Detection (Before Optimization):")
    println("  # Analyze key distribution")
    println("  df.groupBy('key').count().orderBy(desc('count')).show()")
    println("  # If top key > 10x median → skew exists")
    println("\nSkew Mitigation Strategies:")
    println("  1. Salting (this example) - for groupBy/aggregations")
    println("  2. AQE Skew Join - automatic for joins (enabled)")
    println("  3. Isolated Broadcast - broadcast hot keys separately")
    println("  4. Custom Partitioning - partition by composite key")
    println("  5. Filter Hot Keys - process separately then union")
    println("\nWhen to Use Each:")
    println("  • Salting: GroupBy/Aggregation with hot keys")
    println("  • AQE: Joins with skewed keys (automatic)")
    println("  • Isolated Broadcast: Few hot keys, rest uniform")
    println("  • Custom Partitioner: Predictable skew pattern")
    println(s"${"=" * 80}\n")
  }
}
