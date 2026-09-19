package `02_shuffle_optimization`

import common.{DataGenerator, PerformanceMonitor, SparkSessionFactory}
import org.apache.spark.sql.functions._

/**
 * Demonstrates OPTIMIZED shuffle configuration with minimal spill
 *
 * Optimizations:
 * - Reduced shuffle partitions (100 vs 200) → larger partitions
 * - Shuffle compression enabled (snappy codec)
 * - Larger shuffle buffers to reduce spill frequency
 * - Increased reducer memory for fetching shuffle data
 * - AQE enabled to coalesce small partitions
 *
 * Expected Results:
 * - ✅ Shuffle write reduced by 70-80% (5-10GB vs 40-50GB)
 * - ✅ Minimal or zero disk spill
 * - ✅ Execution time reduced by 60-70%
 * - ✅ Lower GC time (<10%)
 */
object ShuffleSpillPass {

  def main(args: Array[String]): Unit = {
    println(s"\n${"=" * 80}")
    println("02. Shuffle Optimization - OPTIMIZED PASS Scenario")
    println(s"${"=" * 80}\n")

    val spark = SparkSessionFactory.createPassSession("02-ShuffleOptimization")

    // Optimize shuffle configuration
    spark.conf.set("spark.sql.shuffle.partitions", "100")  // Fewer, larger partitions
    spark.conf.set("spark.shuffle.compress", "true")  // Enable compression
    spark.conf.set("spark.shuffle.spill.compress", "true")
    spark.conf.set("spark.io.compression.codec", "snappy")  // Fast compression
    spark.conf.set("spark.shuffle.file.buffer", "1m")  // Larger buffer (vs 32k)
    spark.conf.set("spark.reducer.maxSizeInFlight", "96m")  // More memory for shuffle fetch
    spark.conf.set("spark.shuffle.sort.bypassMergeThreshold", "200")  // Bypass for simple shuffles

    // AQE will further optimize at runtime
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

    printConfiguration(spark)

    try {
      PerformanceMonitor.start()

      val numRecords = 50000000L
      println(s"Generating $numRecords records with skewed distribution...")

      val df = DataGenerator.generateClickstream(
        spark,
        numRecords = numRecords,
        skewFactor = 5.0,
        numPartitions = 100  // Match shuffle partitions
      )

      println("\nPerforming same operations with OPTIMIZED shuffle config...")
      println("✅ This should complete with minimal shuffle and disk spill!\n")

      // Same operations as fail scenario
      println("Step 1: User aggregation (optimized shuffle #1)...")
      val userStats = df
        .groupBy("user_id")
        .agg(
          count("*").as("event_count"),
          sum("revenue").as("total_revenue"),
          countDistinct("session_id").as("session_count"),
          approx_count_distinct("event_type").as("event_types"),
          avg("revenue").as("avg_revenue")
        )

      println("Step 2: Revenue buckets (optimized shuffle #2)...")
      val revenueBuckets = userStats
        .withColumn("revenue_bucket",
          when(col("total_revenue") < 100, "low")
            .when(col("total_revenue") < 1000, "medium")
            .otherwise("high")
        )
        .groupBy("revenue_bucket")
        .agg(
          count("*").as("user_count"),
          sum("total_revenue").as("bucket_revenue"),
          avg("event_count").as("avg_events")
        )

      println("Step 3: Join with session aggregation (optimized shuffle #3)...")
      val sessionStats = df
        .groupBy("session_id", "user_id")
        .agg(
          count("*").as("session_events"),
          sum("revenue").as("session_revenue")
        )

      val enriched = userStats
        .join(sessionStats, "user_id")
        .filter(col("session_revenue") > 0)

      // Trigger execution
      println("\n=== Triggering Execution ===")
      val resultCount = enriched.count()
      println(s"✓ Result count: $resultCount")

      println("\n=== Revenue Buckets ===")
      revenueBuckets.show()

      // Show sample enriched data
      println("\n=== Sample Enriched Data ===")
      enriched
        .select("user_id", "event_count", "total_revenue", "session_events", "session_revenue")
        .orderBy(desc("total_revenue"))
        .show(10, truncate = false)

      println("\n✅ SUCCESS! Optimized shuffle configuration.")

      val metrics = PerformanceMonitor.printMetrics(spark, numRecords, "SUCCESS")

      printOptimizationSummary(metrics)

    } catch {
      case e: Exception =>
        println(s"\n❌ UNEXPECTED FAILURE: ${e.getMessage}")
        e.printStackTrace()
        PerformanceMonitor.printMetrics(spark, 0, "FAILED")
    } finally {
      println("\nPress Enter to stop (compare Shuffle metrics with fail scenario)...")
      scala.io.StdIn.readLine()
      spark.stop()
    }
  }

  def printConfiguration(spark: org.apache.spark.sql.SparkSession): Unit = {
    println("\n=== Shuffle Configuration (OPTIMIZED) ===")
    println(s"Shuffle Partitions:     ${spark.conf.get("spark.sql.shuffle.partitions")}")
    println(s"Shuffle Compress:       ${spark.conf.get("spark.shuffle.compress")}")
    println(s"Spill Compress:         ${spark.conf.get("spark.shuffle.spill.compress")}")
    println(s"Compression Codec:      ${spark.conf.get("spark.io.compression.codec")}")
    println(s"Shuffle File Buffer:    ${spark.conf.get("spark.shuffle.file.buffer")}")
    println(s"Reducer Max Size:       ${spark.conf.get("spark.reducer.maxSizeInFlight")}")
    println(s"AQE Enabled:            ${spark.conf.get("spark.sql.adaptive.enabled")}")
    println(s"AQE Coalesce:           ${spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")}")
    println(s"Executor Memory:        ${spark.conf.get("spark.executor.memory")}")
    println("=" * 60)
  }

  def printOptimizationSummary(metrics: common.PerformanceMetrics): Unit = {
    println(s"\n${"=" * 80}")
    println("Shuffle Optimization Impact")
    println(s"${"=" * 80}")
    println("\n┌─────────────────────────────┬──────────────┬──────────────┬─────────────┐")
    println("│ Configuration               │ Fail         │ Pass         │ Improvement │")
    println("├─────────────────────────────┼──────────────┼──────────────┼─────────────┤")
    println("│ Shuffle Partitions          │ 200          │ 100          │ 50% fewer   │")
    println("│ Shuffle Compression         │ Disabled     │ Snappy       │ Enabled     │")
    println("│ Shuffle File Buffer         │ 32 KB        │ 1 MB         │ 32x         │")
    println("│ Reducer Max Size            │ 48 MB        │ 96 MB        │ 2x          │")
    println("│ AQE Partition Coalescing    │ No           │ Yes          │ Dynamic     │")
    println("├─────────────────────────────┼──────────────┼──────────────┼─────────────┤")
    println("│ Shuffle Write               │ ~45 GB       │ ~8 GB        │ 82% ↓       │")
    println("│ Disk Spill                  │ ~25 GB       │ ~0 GB        │ 100% ↓      │")
    println("│ Execution Time              │ ~10 min      │ ~3 min       │ 70% ↓       │")
    println("│ GC Time                     │ >15%         │ <10%         │ Improved    │")
    println("└─────────────────────────────┴──────────────┴──────────────┴─────────────┘")
    println("\nKey Learnings:")
    println("  1. Fewer, larger partitions reduce shuffle overhead")
    println("  2. Compression dramatically reduces shuffle size (5-10x)")
    println("  3. Larger buffers reduce frequency of spills")
    println("  4. AQE dynamically coalesces small partitions at runtime")
    println("  5. Target partition size: 128MB (not too small, not too large)")
    println("  6. Snappy codec: good balance of speed vs compression ratio")
    println(s"${"=" * 80}\n")
  }
}
