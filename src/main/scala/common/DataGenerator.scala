package common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.Random

object DataGenerator {

  /**
   * Generate clickstream data with configurable size and skew
   *
   * @param spark SparkSession
   * @param numRecords Number of records to generate
   * @param skewFactor Skew factor (1.0 = uniform, 10.0 = highly skewed)
   * @param numPartitions Number of partitions for parallelism
   * @return DataFrame with clickstream data
   */
  def generateClickstream(
      spark: SparkSession,
      numRecords: Long = 50000000L,  // 50M records
      skewFactor: Double = 1.0,
      numPartitions: Int = 100
  ): DataFrame = {
    import spark.implicits._

    println(s"Generating $numRecords clickstream records with skew factor $skewFactor...")

    val recordsPerPartition = numRecords / numPartitions

    spark.range(0, numPartitions, 1, numPartitions)
      .flatMap { partitionId =>
        (0L until recordsPerPartition).map { i =>
          val recordId = partitionId * recordsPerPartition + i
          val timestamp = System.currentTimeMillis() - Random.nextInt(86400000)  // Last 24 hours

          // Generate user_id with configurable skew (Zipf distribution)
          val userId = if (Random.nextDouble() < (skewFactor / 10.0)) {
            // Hot users (10% of users get most traffic)
            Random.nextInt(1000)
          } else {
            // Cold users
            Random.nextInt(10000000) + 1000
          }

          val eventType = Random.shuffle(List("page_view", "click", "purchase", "add_to_cart", "search")).head
          val sessionId = s"session_${userId}_${timestamp / 3600000}"
          val pageUrl = s"/page/${Random.nextInt(10000)}"
          val revenue = if (eventType == "purchase") Random.nextDouble() * 1000 else 0.0

          (recordId, timestamp, userId, eventType, sessionId, pageUrl, revenue)
        }
      }
      .toDF("record_id", "timestamp", "user_id", "event_type", "session_id", "page_url", "revenue")
  }

  /**
   * Generate transaction data with temporal patterns
   */
  def generateTransactions(
      spark: SparkSession,
      numRecords: Long = 10000000L,  // 10M records
      numPartitions: Int = 100
  ): DataFrame = {
    import spark.implicits._

    println(s"Generating $numRecords transaction records...")

    val recordsPerPartition = numRecords / numPartitions

    spark.range(0, numPartitions, 1, numPartitions)
      .flatMap { partitionId =>
        (0L until recordsPerPartition).map { i =>
          val transactionId = s"txn_${partitionId}_$i"
          val userId = Random.nextInt(1000000)
          val productId = Random.nextInt(100000)
          val quantity = Random.nextInt(10) + 1
          val price = Random.nextDouble() * 500 + 10
          val amount = quantity * price
          val timestamp = System.currentTimeMillis() - Random.nextInt(2592000)  // Last 30 days
          val category = Random.shuffle(List("Electronics", "Clothing", "Food", "Books", "Home")).head

          (transactionId, userId, productId, quantity, price, amount, timestamp, category)
        }
      }
      .toDF("transaction_id", "user_id", "product_id", "quantity", "price", "amount", "timestamp", "category")
  }

  /**
   * Generate wide table with many columns (for testing columnar formats)
   */
  def generateWideTable(
      spark: SparkSession,
      numRecords: Long = 1000000L,
      numColumns: Int = 500,
      numPartitions: Int = 50
  ): DataFrame = {
    import spark.implicits._

    println(s"Generating $numRecords records with $numColumns columns...")

    val recordsPerPartition = numRecords / numPartitions

    val baseDF = spark.range(0, numPartitions, 1, numPartitions)
      .flatMap { partitionId =>
        (0L until recordsPerPartition).map { i =>
          (partitionId * recordsPerPartition + i, Random.nextInt(1000))
        }
      }
      .toDF("id", "base_value")

    // Add many columns
    var wideDF = baseDF
    for (colIdx <- 1 to numColumns) {
      wideDF = wideDF.withColumn(s"col_$colIdx", expr(s"base_value + $colIdx"))
    }

    wideDF
  }

  /**
   * Save DataFrame to disk in various formats
   */
  def saveData(df: DataFrame, path: String, format: String = "parquet"): Unit = {
    println(s"Saving data to $path in $format format...")
    df.write
      .mode("overwrite")
      .format(format)
      .save(path)
    println(s"Data saved successfully!")
  }

  /**
   * Main method for standalone data generation
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataGenerator")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .getOrCreate()

    val dataType = if (args.length > 0) args(0) else "clickstream"
    val size = if (args.length > 1) args(1) else "small"
    val outputPath = if (args.length > 2) args(2) else s"data/$dataType/$size"

    val numRecords = size match {
      case "small" => 1000000L      // 1M records
      case "medium" => 10000000L    // 10M records
      case "large" => 50000000L     // 50M records
      case "xlarge" => 100000000L   // 100M records
      case _ => size.toLong
    }

    val df = dataType match {
      case "clickstream" => generateClickstream(spark, numRecords)
      case "transactions" => generateTransactions(spark, numRecords)
      case "wide" => generateWideTable(spark, numRecords)
      case _ =>
        println(s"Unknown data type: $dataType. Using clickstream.")
        generateClickstream(spark, numRecords)
    }

    saveData(df, outputPath)

    println(s"\n=== Data Generation Summary ===")
    println(s"Type: $dataType")
    println(s"Records: $numRecords")
    println(s"Output: $outputPath")
    println(s"Schema:")
    df.printSchema()
    df.show(5, truncate = false)

    spark.stop()
  }
}
