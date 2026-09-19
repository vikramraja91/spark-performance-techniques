package `03_partition_tuning`

import common.{DataGenerator, PerformanceMonitor, SparkSessionFactory}
import org.apache.spark.sql.functions._

/**
 * Demonstrates EXCESSIVE PARTITIONS causing scheduler overhead and poor performance
 *
 * Scenario:
 * - 10GB data split into 10,000 tiny partitions (~1MB each)
 * - Each partition creates task scheduling overhead
 * - Small tasks complete too quickly, causing scheduling bottleneck
 * - Executor thread pool underutilized
 *
 * Expected Issues:
 * - 10,000+ tasks created
 * - Scheduler overhead (task serialization, scheduling delay)
 * - Poor CPU utilization despite having cores available
 * - Long execution time despite small data size
 * - Many small output files
 *
 * Root Causes:
 * - Default partitioning not adjusted for data size
 * - Each partition too small (< 10MB)
 * - Scheduler can't keep up with rapid task completion
 */
object PartitionOverheadFail {

  def main(args: Array[String]): Unit = {
    println(s"\n${"=" * 80}")
    println("03. Partition Tuning - EXCESSIVE PARTITIONS FAIL Scenario")
    println(s"${"=" * 80}\n")

    val spark = SparkSessionFactory.createFailSession("03-PartitionTuning")

    printConfiguration(spark)

    try {
      PerformanceMonitor.start()

      // Generate 20M records (~4GB in memory)
      val numRecords = 20000000L
      val numPartitions = 2000  // TOO MANY! (~10K records per partition = ~2MB each)

      println(s"Generating $numRecords records in $numPartitions partitions...")
      println(s"⚠️  Each partition will have only ${numRecords / numPartitions} records (~2MB)")
      println("   This creates excessive task scheduling overhead!\n")

      val df = DataGenerator.generateClickstream(
        spark,
        numRecords = numRecords,
        skewFactor = 1.0,
        numPartitions = numPartitions
      )

      println(s"✓ Data generated in $numPartitions partitions")
      println(s"  Actual partitions: ${df.rdd.getNumPartitions}")

      // Operation 1: Simple aggregation (will create many tiny tasks)
      println("\nStep 1: User aggregation with excessive partitions...")
      val userStats = df
        .groupBy("user_id")
        .agg(
          count("*").as("event_count"),
          sum("revenue").as("total_revenue")
        )

      // Don't repartition - keep the excessive partitions
      println(s"  Shuffle partitions: ${spark.conf.get("spark.sql.shuffle.partitions")}")

      // Operation 2: Another aggregation (compounds the problem)
      println("Step 2: Event type distribution...")
      val eventDist = df
        .groupBy("event_type")
        .agg(
          count("*").as("count"),
          sum("revenue").as("revenue")
        )

      // Trigger execution
      println("\n=== Triggering Execution ===")
      println("Watch Spark UI Jobs tab - you'll see MANY small tasks!\n")

      val userCount = userStats.count()
      println(s"✓ User count: $userCount")

      println("\n=== Event Distribution ===")
      eventDist.show()

      // Write output to demonstrate small file problem
      val outputPath = "data/partition-fail-output"
      println(s"\nWriting output to $outputPath...")
      userStats
        .write
        .mode("overwrite")
        .parquet(outputPath)

      // Check number of files created
      val spark2 = spark
      import spark2.implicits._
      val fileCount = spark.read
        .text(outputPath)
        .inputFiles
        .length

      println(s"\n⚠️  Small File Problem:")
      println(s"   Created $fileCount output files!")
      println(s"   Each file is tiny (<5MB)")
      println(s"   This is inefficient for HDFS/S3 and downstream readers")

      println("\n⚠️  Check Spark UI:")
      println("   - Stages tab: Notice the large number of tasks")
      println("   - Many tasks complete in <100ms (too fast = overhead)")
      println("   - Poor parallelism despite having cores available\n")

      PerformanceMonitor.printMetrics(spark, numRecords, "COMPLETED_WITH_OVERHEAD")

    } catch {
      case e: Exception =>
        println(s"\n❌ FAILURE: ${e.getMessage}")
        e.printStackTrace()
        PerformanceMonitor.printMetrics(spark, 0, "FAILED")
    } finally {
      println("\nPress Enter to stop (check Spark UI Stages tab for task count)...")
      scala.io.StdIn.readLine()
      spark.stop()
    }
  }

  def printConfiguration(spark: org.apache.spark.sql.SparkSession): Unit = {
    println("\n=== Partition Configuration (FAIL Scenario) ===")
    println(s"Default Parallelism:    ${spark.conf.get("spark.default.parallelism")}")
    println(s"Shuffle Partitions:     ${spark.conf.get("spark.sql.shuffle.partitions")}")
    println(s"Executor Cores:         4 (local[4])")
    println("\nProblem: Too many small partitions for data size")
    println("=" * 60)
  }
}
