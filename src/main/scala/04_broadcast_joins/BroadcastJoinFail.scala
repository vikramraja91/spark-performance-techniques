package `04_broadcast_joins`

import common.{DataGenerator, PerformanceMonitor, SparkSessionFactory}
import org.apache.spark.sql.functions._

/**
 * Demonstrates SHUFFLE JOIN when broadcast would be better
 *
 * Scenario:
 * - Join large fact table (50M records, ~10GB) with small dimension table (100K records, ~20MB)
 * - Auto-broadcast disabled, forcing sort-merge join
 * - Both tables shuffled across network
 * - Unnecessary shuffle of large fact table
 *
 * Expected Issues:
 * - Huge shuffle write (20-30GB) for just joining with tiny table
 * - Long execution time (8-12 minutes)
 * - Wasted network I/O shuffling fact table
 * - Multiple shuffle stages
 *
 * Root Causes:
 * - autoBroadcastJoinThreshold disabled or too small
 * - Spark doesn't know dimension table is small
 * - Defaults to expensive sort-merge join
 */
object BroadcastJoinFail {

  def main(args: Array[String]): Unit = {
    println(s"\n${"=" * 80}")
    println("04. Broadcast Joins - SHUFFLE JOIN FAIL Scenario")
    println(s"${"=" * 80}\n")

    val spark = SparkSessionFactory.createFailSession("04-BroadcastJoins")

    // DISABLE auto-broadcast to force shuffle join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  // Disabled
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")

    printConfiguration(spark)

    try {
      PerformanceMonitor.start()

      // Generate LARGE fact table (50M records)
      val numFactRecords = 50000000L
      println(s"Generating LARGE fact table: $numFactRecords records...")
      val factTable = DataGenerator.generateClickstream(
        spark,
        numRecords = numFactRecords,
        skewFactor = 2.0,
        numPartitions = 200
      )
      println(s"✓ Fact table: ${factTable.rdd.getNumPartitions} partitions")

      // Generate SMALL dimension table (100K records ~ 20MB)
      val numDimRecords = 100000L
      println(s"\nGenerating SMALL dimension table: $numDimRecords records...")

      import spark.implicits._
      val dimTable = spark.range(0, numDimRecords)
        .select(
          col("id").as("user_id"),
          lit("Premium").as("user_tier"),
          (rand() * 100).cast("int").as("user_age"),
          lit("US").as("country")
        )

      println(s"✓ Dimension table: ${dimTable.rdd.getNumPartitions} partitions (~20MB)\n")

      println("⚠️  Performing JOIN with auto-broadcast DISABLED...")
      println("   Spark will use expensive Sort-Merge Join!")
      println("   Both tables will be shuffled!\n")

      // Join large fact table with small dimension table
      println("Step 1: Join fact table with dimension (shuffle join)...")
      val joined = factTable
        .join(dimTable, Seq("user_id"), "left")
        .select(
          factTable("*"),
          dimTable("user_tier"),
          dimTable("user_age"),
          dimTable("country")
        )

      // Aggregate on joined result
      println("Step 2: Aggregate by user tier...")
      val tierStats = joined
        .groupBy("user_tier")
        .agg(
          count("*").as("event_count"),
          countDistinct("user_id").as("unique_users"),
          sum("revenue").as("total_revenue"),
          avg("revenue").as("avg_revenue")
        )

      // Trigger execution
      println("\n=== Triggering Execution ===")
      println("Watch Spark UI SQL tab for:")
      println("  - SortMergeJoin operation (not BroadcastHashJoin)")
      println("  - Exchange (shuffle) for BOTH tables\n")

      tierStats.show()

      println("\n=== Sample Joined Data ===")
      joined
        .select("user_id", "event_type", "revenue", "user_tier", "user_age")
        .show(10, truncate = false)

      println("\n⚠️  SHUFFLE JOIN DETECTED!")
      println("   Check Spark UI SQL tab:")
      println("   - Look for 'SortMergeJoin' in the physical plan")
      println("   - Notice 'Exchange' (shuffle) for both tables")
      println(s"   - Fact table (10GB) was unnecessarily shuffled!")
      println("   - This is the expected BAD scenario.\n")

      val metrics = PerformanceMonitor.printMetrics(spark, numFactRecords, "SLOW_COMPLETED")

      if (metrics.shuffleWriteGB > 15) {
        println(s"\n❌ EXCESSIVE SHUFFLE from avoiding broadcast!")
        println(s"   Shuffle Write: ${metrics.shuffleWriteGB}%.2f GB")
      }

    } catch {
      case e: Exception =>
        println(s"\n❌ FAILURE: ${e.getMessage}")
        e.printStackTrace()
        PerformanceMonitor.printMetrics(spark, 0, "FAILED")
    } finally {
      println("\nPress Enter to stop (check Spark UI SQL tab for join strategy)...")
      scala.io.StdIn.readLine()
      spark.stop()
    }
  }

  def printConfiguration(spark: org.apache.spark.sql.SparkSession): Unit = {
    println("\n=== Join Configuration (FAIL Scenario) ===")
    println(s"Auto Broadcast Threshold:   ${spark.conf.get("spark.sql.autoBroadcastJoinThreshold")} (DISABLED)")
    println(s"Prefer Sort-Merge Join:     ${spark.conf.get("spark.sql.join.preferSortMergeJoin")}")
    println("\nProblem: Small table not broadcasted → unnecessary shuffle")
    println("=" * 60)
  }
}
