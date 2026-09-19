package memory_management

import common.{DataGenerator, PerformanceMonitor, SparkSessionFactory}
import org.apache.spark.sql.functions._

/**
 * Demonstrates EXECUTOR OOM failure with insufficient memory configuration
 *
 * Scenario:
 * - Process 50M clickstream records with complex aggregations
 * - Wide transformations requiring significant executor memory
 * - Multiple shuffles and window functions
 *
 * Expected Failure:
 * - OutOfMemoryError: Java heap space
 * - Or: ExecutorLostFailure (container killed by YARN for exceeding memory limits)
 * - Job fails around 30-40M records processed
 *
 * Root Cause:
 * - Only 2GB executor memory allocated
 * - Memory.fraction = 0.6 means only 1.2GB for execution/storage
 * - Insufficient memoryOverhead (512MB)
 * - Large shuffle operations exhaust available memory
 */
object ExecutorOOMFail {

  def main(args: Array[String]): Unit = {
    println(s"\n${"=" * 80}")
    println("01. Memory Management - EXECUTOR OOM FAIL Scenario")
    println(s"${"=" * 80}\n")

    // Create Spark session with INSUFFICIENT executor memory
    val spark = SparkSessionFactory.createFailSession("01-MemoryManagement")
    spark.sparkContext.setLogLevel("INFO")

    printConfiguration(spark)

    try {
      PerformanceMonitor.start()

      // Generate large dataset (50M records ~ 10GB in memory)
      val numRecords = 50000000L
      val df = DataGenerator.generateClickstream(
        spark,
        numRecords = numRecords,
        skewFactor = 2.0,  // Some skew
        numPartitions = 200
      )

      println(s"\nProcessing $numRecords records with complex transformations...")
      println("This WILL cause executor OOM!\n")

      // Transformation 1: User session analysis (wide transformation)
      val userSessions = df
        .groupBy("user_id", "session_id")
        .agg(
          org.apache.spark.sql.functions.count("*").as("event_count"),
          sum("revenue").as("session_revenue"),
          collect_list("event_type").as("event_sequence"),  // ⚠️ Creates large arrays in memory
          min("timestamp").as("session_start"),
          max("timestamp").as("session_end")
        )

      println("✓ Session aggregation computed")

      // Transformation 2: User profile (another aggregation on top)
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

      // Transformation 3: Window function requiring shuffle and sort
      // This is where OOM typically occurs due to shuffle + sort buffers
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

      // Trigger execution with multiple actions
      println("\n=== Triggering Execution ===")

      val topUsers = userRanking
        .filter(col("user_rank") <= 1000)
        .orderBy(desc("lifetime_value"))

      val count = topUsers.count()
      println(s"✓ Count: $count records")

      // Second action forcing materialization
      topUsers.show(10, truncate = false)

      // If we get here, the job succeeded (unexpected!)
      println("\n⚠️  WARNING: Job completed without OOM!")
      println("   Dataset might be too small or executor memory is sufficient.")
      println("   Try increasing numRecords or reducing executor memory further.\n")

      PerformanceMonitor.printMetrics(spark, numRecords, "UNEXPECTED_SUCCESS")

    } catch {
      case oom: OutOfMemoryError =>
        println(s"\n${"=" * 80}")
        println("❌ EXECUTOR OOM FAILURE (Expected)")
        println(s"${"=" * 80}")
        println(s"Error: ${oom.getMessage}")
        println("\nRoot Causes:")
        println("  1. Executor memory too small (2GB)")
        println("  2. Memory fraction only 0.6 → 1.2GB for execution/storage")
        println("  3. Insufficient memory overhead (512MB)")
        println("  4. Wide transformations + shuffles + window functions")
        println("  5. collect_list creates large in-memory arrays")
        println("\nSolutions:")
        println("  ✓ Increase executor memory to 8GB")
        println("  ✓ Set memory.fraction to 0.8")
        println("  ✓ Increase memoryOverhead to 2GB")
        println("  ✓ Enable off-heap memory")
        println("  ✓ Optimize shuffle partitions")
        println(s"${"=" * 80}\n")

        PerformanceMonitor.printMetrics(spark, 0, "FAILED_OOM")

      case e: Exception =>
        println(s"\n❌ FAILURE: ${e.getClass.getSimpleName}")
        println(s"Message: ${e.getMessage}")

        if (e.getMessage != null && (
          e.getMessage.contains("ExecutorLostFailure") ||
            e.getMessage.contains("Container killed") ||
            e.getMessage.contains("exceeding memory limits")
        )) {
          println("\n✅ This is an EXPECTED executor memory failure!")
          println("   Container was killed by resource manager for exceeding memory limits.")
        }

        e.printStackTrace()
        PerformanceMonitor.printMetrics(spark, 0, "FAILED")

    } finally {
      println("\nPress Enter to stop Spark session (check Spark UI first)...")
      scala.io.StdIn.readLine()
      spark.stop()
    }
  }

  def printConfiguration(spark: org.apache.spark.sql.SparkSession): Unit = {
    println("\n=== Executor Configuration (FAIL Scenario) ===")
    println(s"Executor Memory:        ${spark.conf.get("spark.executor.memory")}")
    println(s"Memory Overhead:        ${spark.conf.get("spark.executor.memoryOverhead")}")
    println(s"Memory Fraction:        ${spark.conf.get("spark.memory.fraction")}")
    println(s"Storage Fraction:       ${spark.conf.get("spark.memory.storageFraction")}")
    println(s"Shuffle Partitions:     ${spark.conf.get("spark.sql.shuffle.partitions")}")
    println(s"Off-Heap Enabled:       ${spark.conf.getOption("spark.memory.offHeap.enabled").getOrElse("false")}")
    println("=" * 60)
  }
}
