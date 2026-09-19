package memory_management

import common.{DataGenerator, PerformanceMonitor, SparkSessionFactory}
import org.apache.spark.sql.functions._


/**
 * Demonstrates SUCCESSFUL execution with optimized memory configuration
 *
 * Scenario:
 * - Same workload as ExecutorOOMFail (50M records, complex aggregations)
 * - Proper memory configuration prevents OOM
 *
 * Optimizations:
 * - 8GB executor memory (vs 2GB)
 * - Memory.fraction = 0.8 (vs 0.6) → more memory for execution
 * - MemoryOverhead = 2GB (vs 512MB)
 * - Off-heap memory enabled (2GB)
 * - Reduced shuffle partitions (100 vs 200) for larger partition size
 * - AQE enabled for dynamic optimization
 *
 * Expected Result:
 * - ✅ Job completes successfully
 * - Memory utilization ~75-85%
 * - Minimal or zero spill to disk
 */
object ExecutorOOMPass {

  def main(args: Array[String]): Unit = {
    println(s"\n${"=" * 80}")
    println("01. Memory Management - OPTIMIZED PASS Scenario")
    println(s"${"=" * 80}\n")

    // Create Spark session with OPTIMIZED executor memory
    val spark = SparkSessionFactory.createPassSession("01-MemoryManagement")

    printConfiguration(spark)

    try {
      PerformanceMonitor.start()

      // Generate same large dataset as fail scenario
      val numRecords = 50000000L
      val df = DataGenerator.generateClickstream(
        spark,
        numRecords = numRecords,
        skewFactor = 2.0,
        numPartitions = 100  // Fewer partitions → larger partitions → better memory efficiency
      )

      println(s"\nProcessing $numRecords records with complex transformations...")
      println("This SHOULD succeed with optimized memory configuration!\n")


      // Same transformations as fail scenario
      // Transformation 1: User session analysis
      val userSessions = df
        .groupBy("user_id", "session_id")
        .agg(
          org.apache.spark.sql.functions.count("*").as("event_count"),
          sum("revenue").as("session_revenue"),
          collect_list("event_type").as("event_sequence"),
          min("timestamp").as("session_start"),
          max("timestamp").as("session_end")
        )

      println("✓ Session aggregation computed")

      // Transformation 2: User profile
      val userProfiles = userSessions
        .groupBy("user_id")
        .agg(
          org.apache.spark.sql.functions.count("*").as("total_sessions"),
          sum("event_count").as("total_events"),
          sum("session_revenue").as("lifetime_value"),
          avg("event_count").as("avg_events_per_session"),
          max("session_end").as("last_seen")
        )

      println("✓ User profiles computed")

      // Transformation 3: Window function (with AQE optimization)
      val userRanking = userProfiles
        .withColumn(
          "user_rank",
          row_number().over(
            org.apache.spark.sql.expressions.Window
              .orderBy(desc("lifetime_value"))
          )
        )
        .withColumn(
          "ltv_percentile",
          percent_rank().over(
            org.apache.spark.sql.expressions.Window
              .orderBy("lifetime_value")
          )
        )

      println("✓ Window functions computed")

      // Trigger execution
      println("\n=== Triggering Execution ===")

      val topUsers = userRanking
        .filter(col("user_rank") <= 1000)
        .orderBy(desc("lifetime_value"))

      val count = topUsers.count()
      println(s"✓ Count: $count top users")

      // Show sample results
      println("\n=== Top 10 Users by Lifetime Value ===")
      topUsers.select(
        "user_id",
        "user_rank",
        "lifetime_value",
        "total_sessions",
        "total_events",
        "avg_events_per_session"
      ).show(10, truncate = false)

      // Additional analysis to stress memory further
      println("\n=== Revenue Distribution Analysis ===")
      userProfiles
        .select(
          round(avg("lifetime_value"), 2).as("avg_ltv"),
          round(stddev("lifetime_value"), 2).as("stddev_ltv"),
          round(min("lifetime_value"), 2).as("min_ltv"),
          round(max("lifetime_value"), 2).as("max_ltv"),
          round(sum("lifetime_value"), 2).as("total_revenue")
        )
        .show(truncate = false)

      println("\n✅ SUCCESS! Job completed without OOM.")
      println("   Optimized memory configuration handled the workload efficiently.\n")

      val metrics = PerformanceMonitor.printMetrics(spark, numRecords, "SUCCESS")

      // Print optimization comparison
      printOptimizationSummary(metrics)

    } catch {
      case e: Exception =>
        println(s"\n❌ UNEXPECTED FAILURE: ${e.getClass.getSimpleName}")
        println(s"Message: ${e.getMessage}")
        println("\nThis should not happen with optimized configuration!")
        println("Check:")
        println("  - Is executor memory actually set to 8GB?")
        println("  - Is off-heap memory enabled?")
        println("  - Check Spark UI for executor memory stats")
        e.printStackTrace()

        PerformanceMonitor.printMetrics(spark, 0, "FAILED")

    } finally {
      println("\nPress Enter to stop Spark session (check Spark UI first)...")
      scala.io.StdIn.readLine()
      spark.stop()
    }
  }

  def printConfiguration(spark: org.apache.spark.sql.SparkSession): Unit = {
    println("\n=== Executor Configuration (OPTIMIZED) ===")
    println(s"Executor Memory:        ${spark.conf.get("spark.executor.memory")}")
    println(s"Memory Overhead:        ${spark.conf.get("spark.executor.memoryOverhead")}")
    println(s"Memory Fraction:        ${spark.conf.get("spark.memory.fraction")}")
    println(s"Storage Fraction:       ${spark.conf.get("spark.memory.storageFraction")}")
    println(s"Off-Heap Enabled:       ${spark.conf.get("spark.memory.offHeap.enabled")}")
    println(s"Off-Heap Size:          ${spark.conf.getOption("spark.memory.offHeap.size").getOrElse("N/A")}")
    println(s"Shuffle Partitions:     ${spark.conf.get("spark.sql.shuffle.partitions")}")
    println(s"AQE Enabled:            ${spark.conf.get("spark.sql.adaptive.enabled")}")
    println(s"Serializer:             ${spark.conf.get("spark.serializer")}")
    println("=" * 60)
  }

  def printOptimizationSummary(metrics: common.PerformanceMetrics): Unit = {
    println(s"\n${"=" * 80}")
    println("Optimization Impact Summary")
    println(s"${"=" * 80}")
    println("\n┌─────────────────────────────┬──────────────┬──────────────┬─────────────┐")
    println("│ Configuration               │ Fail         │ Pass         │ Improvement │")
    println("├─────────────────────────────┼──────────────┼──────────────┼─────────────┤")
    println("│ Executor Memory             │ 2 GB         │ 8 GB         │ 4x          │")
    println("│ Memory Overhead             │ 512 MB       │ 2 GB         │ 4x          │")
    println("│ Memory Fraction             │ 0.6          │ 0.8          │ +33%        │")
    println("│ Off-Heap Memory             │ Disabled     │ 2 GB         │ N/A         │")
    println("│ Shuffle Partitions          │ 200          │ 100          │ Optimized   │")
    println("│ AQE Enabled                 │ No           │ Yes          │ Dynamic     │")
    println("├─────────────────────────────┼──────────────┼──────────────┼─────────────┤")
    println("│ Result                      │ OOM ❌       │ Success ✅   │ 100%        │")
    println(f"│ Memory Utilization          │ >100%% (OOM)  │ ${metrics.memoryUtilizationPercent}%.1f%%       │ Optimal     │")
    println("└─────────────────────────────┴──────────────┴──────────────┴─────────────┘")
    println("\nKey Learnings:")
    println("  1. Executor memory is critical - 4x increase prevented OOM")
    println("  2. Memory.fraction=0.8 maximizes execution/storage memory")
    println("  3. Adequate memoryOverhead prevents container kills")
    println("  4. Off-heap memory reduces GC pressure")
    println("  5. Fewer, larger partitions improve memory efficiency")
    println("  6. AQE dynamically optimizes at runtime")
    println(s"${"=" * 80}\n")
  }
}
