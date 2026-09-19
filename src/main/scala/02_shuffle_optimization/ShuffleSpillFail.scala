package `02_shuffle_optimization`

import common.{DataGenerator, PerformanceMonitor, SparkSessionFactory}
import org.apache.spark.sql.functions._

/**
 * Demonstrates EXCESSIVE SHUFFLE causing disk spill and slow performance
 *
 * Scenario:
 * - Process 50M records with multiple groupBy operations
 * - Default 200 shuffle partitions create small partitions
 * - Excessive shuffle write/read (>50GB)
 * - Significant disk spill degrading performance
 *
 * Expected Issues:
 * - Large shuffle write (40-50GB)
 * - Disk spill (20-30GB)
 * - Slow execution (10+ minutes)
 * - High GC time (>15%)
 *
 * Root Causes:
 * - Too many shuffle partitions (200) creating overhead
 * - No shuffle compression
 * - Small shuffle buffers causing frequent spills
 * - No map-side aggregation optimization
 */
object ShuffleSpillFail {

  def main(args: Array[String]): Unit = {
    println(s"\n${"=" * 80}")
    println("02. Shuffle Optimization - EXCESSIVE SHUFFLE FAIL Scenario")
    println(s"${"=" * 80}\n")

    val spark = SparkSessionFactory.createFailSession("02-ShuffleOptimization")

    // Additional shuffle-specific bad configs
    spark.conf.set("spark.sql.shuffle.partitions", "200")  // Too many
    spark.conf.set("spark.shuffle.compress", "false")  // No compression
    spark.conf.set("spark.shuffle.spill.compress", "false")
    spark.conf.set("spark.shuffle.file.buffer", "32k")  // Default, small
    spark.conf.set("spark.reducer.maxSizeInFlight", "48m")  // Default

    printConfiguration(spark)

    try {
      PerformanceMonitor.start()

      val numRecords = 50000000L
      println(s"Generating $numRecords records with skewed distribution...")

      val df = DataGenerator.generateClickstream(
        spark,
        numRecords = numRecords,
        skewFactor = 5.0,  // High skew to increase shuffle
        numPartitions = 200  // Many partitions
      )

      println("\nPerforming operations that cause heavy shuffle...")
      println("⚠️  This will cause large shuffle write and disk spill!\n")

      // Operation 1: Large GroupBy aggregation (causes shuffle)
      println("Step 1: User aggregation (shuffle #1)...")
      val userStats = df
        .groupBy("user_id")
        .agg(
          count("*").as("event_count"),
          sum("revenue").as("total_revenue"),
          countDistinct("session_id").as("session_count"),
          approx_count_distinct("event_type").as("event_types"),
          avg("revenue").as("avg_revenue")
        )

      // Operation 2: Another GroupBy on shuffled data (shuffle #2)
      println("Step 2: Revenue buckets (shuffle #2)...")
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

      // Operation 3: Join back to original (shuffle #3)
      println("Step 3: Join with session aggregation (shuffle #3)...")
      val sessionStats = df
        .groupBy("session_id", "user_id")
        .agg(
          count("*").as("session_events"),
          sum("revenue").as("session_revenue")
        )

      val enriched = userStats
        .join(sessionStats, "user_id")  // Large shuffle join
        .filter(col("session_revenue") > 0)

      // Trigger execution
      println("\n=== Triggering Execution ===")
      val resultCount = enriched.count()
      println(s"Result count: $resultCount")

      println("\n=== Revenue Buckets ===")
      revenueBuckets.show()

      println("\n⚠️  Job completed but likely with poor performance!")
      println("   Check Spark UI for:")
      println("   - Large Shuffle Write (40-50GB expected)")
      println("   - Disk Spill (20-30GB expected)")
      println("   - High GC time (>15%)")
      println("   - Long execution time (>10 minutes)\n")

      val metrics = PerformanceMonitor.printMetrics(spark, numRecords, "SLOW_COMPLETED")

      if (metrics.shuffleWriteGB > 40) {
        println(s"\n❌ EXCESSIVE SHUFFLE DETECTED!")
        println(s"   Shuffle Write: ${metrics.shuffleWriteGB}%.2f GB")
        println(s"   This is the expected BAD scenario.")
      }

    } catch {
      case e: Exception =>
        println(s"\n❌ FAILURE: ${e.getMessage}")
        e.printStackTrace()
        PerformanceMonitor.printMetrics(spark, 0, "FAILED")
    } finally {
      println("\nPress Enter to stop (check Spark UI Stages tab for shuffle metrics)...")
      scala.io.StdIn.readLine()
      spark.stop()
    }
  }

  def printConfiguration(spark: org.apache.spark.sql.SparkSession): Unit = {
    println("\n=== Shuffle Configuration (FAIL Scenario) ===")
    println(s"Shuffle Partitions:     ${spark.conf.get("spark.sql.shuffle.partitions")}")
    println(s"Shuffle Compress:       ${spark.conf.get("spark.shuffle.compress")}")
    println(s"Spill Compress:         ${spark.conf.get("spark.shuffle.spill.compress")}")
    println(s"Shuffle File Buffer:    ${spark.conf.get("spark.shuffle.file.buffer")}")
    println(s"Reducer Max Size:       ${spark.conf.get("spark.reducer.maxSizeInFlight")}")
    println(s"Executor Memory:        ${spark.conf.get("spark.executor.memory")}")
    println("=" * 60)
  }
}
