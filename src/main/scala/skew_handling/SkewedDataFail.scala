package skew_handling

import common.{DataGenerator, PerformanceMonitor, SparkSessionFactory}
import org.apache.spark.sql.functions._

/**
 * Demonstrates DATA SKEW causing one executor to run 50x longer
 *
 * Scenario:
 * - Highly skewed data: 1 key has 80% of records
 * - GroupBy on skewed key causes uneven partition distribution
 * - One executor processes 40M records while others process <1M each
 * - Job appears stuck at 99% waiting for single slow task
 *
 * Expected Issues:
 * - Job stuck at 99% for long time
 * - One executor maxed out (CPU 100%, high memory)
 * - Other executors idle or completed
 * - Single task takes 50-100x longer than others
 * - Possible executor OOM on skewed partition
 *
 * Root Causes:
 * - Skewed key distribution (Zipf distribution / hot keys)
 * - Hash partitioning sends all same-key records to same partition
 * - No skew handling enabled
 * - Single executor becomes bottleneck
 */
object SkewedDataFail {

  def main(args: Array[String]): Unit = {
    println(s"\n${"=" * 80}")
    println("06. Skew Handling - SKEWED DATA FAIL Scenario")
    println(s"${"=" * 80}\n")

    val spark = SparkSessionFactory.createFailSession("06-SkewHandling")

    // Disable AQE skew join optimization
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")

    printConfiguration(spark)

    try {
      PerformanceMonitor.start()

      val numRecords = 50000000L

      println(s"Generating $numRecords records with EXTREME SKEW...")
      println("⚠️  80% of records will have the SAME user_id!")
      println("   This creates massive partition skew!\n")

      // Generate highly skewed data
      val df = DataGenerator.generateClickstream(
        spark,
        numRecords = numRecords,
        skewFactor = 50.0,  // EXTREME skew!
        numPartitions = 100
      )

      // Analyze skew before processing
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
      println(f"\n⚠️  SKEW DETECTED: Top key has $maxCount%,d records ($skewPercentage%.1f%%)\n")

      // Operation that will suffer from skew
      println("Step 1: GroupBy on skewed key...")
      println("Watch Spark UI Stages tab:")
      println("  - One task will process 40M+ records")
      println("  - Other tasks process <1M records each")
      println("  - Job will appear stuck at 99%\n")

      val userStats = df
        .groupBy("user_id")
        .agg(
          count("*").as("event_count"),
          sum("revenue").as("total_revenue"),
          countDistinct("session_id").as("session_count"),
          collect_set("event_type").as("event_types")  // Memory intensive!
        )

      // Additional aggregation to compound the issue
      println("Step 2: Further aggregation...")
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
      println("This will take a LONG time due to skew...")
      println("Monitor Spark UI Stages tab to see the imbalance!\n")

      val startTime = System.currentTimeMillis()
      val result = userStats.count()
      val duration = (System.currentTimeMillis() - startTime) / 1000

      println(s"\n✓ Completed: $result users")
      println(s"✓ Duration: $duration seconds")

      revenueBuckets.show()

      println("\n⚠️  SKEW IMPACT:")
      println("   Check Spark UI Stages tab:")
      println("   - Find the groupBy stage")
      println("   - Click 'Show Additional Metrics' → Check 'Input Size / Records'")
      println("   - Notice ONE task processed 80% of data")
      println("   - Other tasks completed quickly but had to wait")
      println("   - Total time = Time of slowest (skewed) task\n")

      PerformanceMonitor.printMetrics(spark, numRecords, "COMPLETED_WITH_SKEW")

    } catch {
      case oom: OutOfMemoryError =>
        println(s"\n❌ OOM due to SKEW!")
        println("   Skewed partition exhausted executor memory")
        println(s"   Error: ${oom.getMessage}\n")
        PerformanceMonitor.printMetrics(spark, 0, "FAILED_SKEW_OOM")

      case e: Exception =>
        println(s"\n❌ FAILURE: ${e.getMessage}")
        e.printStackTrace()
        PerformanceMonitor.printMetrics(spark, 0, "FAILED")
    } finally {
      println("\nPress Enter to stop (check Spark UI for task skew metrics)...")
      scala.io.StdIn.readLine()
      spark.stop()
    }
  }

  def printConfiguration(spark: org.apache.spark.sql.SparkSession): Unit = {
    println("\n=== Skew Configuration (FAIL Scenario) ===")
    println(s"AQE Enabled:            ${spark.conf.getOption("spark.sql.adaptive.enabled").getOrElse("false")}")
    println(s"AQE Skew Join:          ${spark.conf.getOption("spark.sql.adaptive.skewJoin.enabled").getOrElse("false")}")
    println("\nProblem: No skew mitigation → single executor bottleneck")
    println("=" * 60)
  }
}
