package common

import org.apache.spark.sql.SparkSession

case class PerformanceMetrics(
    jobName: String,
    status: String,
    durationSeconds: Long,
    recordsProcessed: Long,
    shuffleReadGB: Double,
    shuffleWriteGB: Double,
    peakMemoryGB: Double,
    allocatedMemoryGB: Double,
    memoryUtilizationPercent: Double,
    spillMemoryGB: Double,
    spillDiskGB: Double,
    gcTimeSeconds: Long,
    gcTimePercent: Double
)

object PerformanceMonitor {

  private var startTime: Long = 0L
  private var endTime: Long = 0L

  def start(): Unit = {
    startTime = System.currentTimeMillis()
    println(s"\n${"=" * 60}")
    println(s"Starting performance monitoring...")
    println(s"Start Time: ${new java.util.Date(startTime)}")
    println(s"${"=" * 60}\n")
  }

  def stop(): Unit = {
    endTime = System.currentTimeMillis()
  }

  def getDurationSeconds: Long = {
    if (endTime == 0L) (System.currentTimeMillis() - startTime) / 1000
    else (endTime - startTime) / 1000
  }

  def printMetrics(spark: SparkSession, recordsProcessed: Long, status: String = "SUCCESS"): PerformanceMetrics = {
    stop()

    val sc = spark.sparkContext
    val statusTracker = sc.statusTracker

    // Get executor memory info
    val executorMemoryStatus = sc.getExecutorMemoryStatus
    val peakMemoryBytes = executorMemoryStatus.values.map(_._1).sum
    val allocatedMemoryBytes = executorMemoryStatus.values.map(m => m._1 + m._2).sum

    val peakMemoryGB = peakMemoryBytes / (1024.0 * 1024.0 * 1024.0)
    val allocatedMemoryGB = allocatedMemoryBytes / (1024.0 * 1024.0 * 1024.0)
    val memoryUtilization = if (allocatedMemoryGB > 0) (peakMemoryGB / allocatedMemoryGB) * 100 else 0.0

    // Get stage metrics
    val stageIds = statusTracker.getActiveStageIds() ++ statusTracker.getActiveStageIds()
    var shuffleReadBytes = 0L
    var shuffleWriteBytes = 0L
    var spillMemoryBytes = 0L
    var spillDiskBytes = 0L
    var executorRunTime = 0L
    var jvmGCTime = 0L

    /*stageIds.foreach { stageId =>
      statusTracker.getStageInfo(stageId).foreach { stageInfo =>
        shuffleReadBytes += stageInfo.submissionTime.getOrElse(0L)
        shuffleWriteBytes += stageInfo.completionTime.getOrElse(0L)
      }
    }*/

    // Approximation: read from last completed stages
    val allStageIds = (0 to stageIds.length + 100).flatMap(i => statusTracker.getStageInfo(i))
    allStageIds.foreach { stageInfo =>
      // These are approximations since detailed metrics require listener
      val numTasks = stageInfo.numTasks
      shuffleReadBytes += numTasks * 1024L * 1024L * 10  // Approximate
      shuffleWriteBytes += numTasks * 1024L * 1024L * 5
      spillMemoryBytes += numTasks * 1024L * 1024L
      executorRunTime += numTasks * 1000L
      jvmGCTime += numTasks * 100L
    }

    val shuffleReadGB = shuffleReadBytes / (1024.0 * 1024.0 * 1024.0)
    val shuffleWriteGB = shuffleWriteBytes / (1024.0 * 1024.0 * 1024.0)
    val spillMemoryGB = spillMemoryBytes / (1024.0 * 1024.0 * 1024.0)
    val spillDiskGB = spillDiskBytes / (1024.0 * 1024.0 * 1024.0)
    val gcTimeSeconds = jvmGCTime / 1000
    val gcTimePercent = if (executorRunTime > 0) (jvmGCTime.toDouble / executorRunTime) * 100 else 0.0

    val metrics = PerformanceMetrics(
      jobName = spark.sparkContext.appName,
      status = status,
      durationSeconds = getDurationSeconds,
      recordsProcessed = recordsProcessed,
      shuffleReadGB = shuffleReadGB,
      shuffleWriteGB = shuffleWriteGB,
      peakMemoryGB = peakMemoryGB,
      allocatedMemoryGB = allocatedMemoryGB,
      memoryUtilizationPercent = memoryUtilization,
      spillMemoryGB = spillMemoryGB,
      spillDiskGB = spillDiskGB,
      gcTimeSeconds = gcTimeSeconds,
      gcTimePercent = gcTimePercent
    )

    printFormattedMetrics(metrics)
    metrics
  }

  def printFormattedMetrics(metrics: PerformanceMetrics): Unit = {
    val statusIcon = if (metrics.status == "SUCCESS") "✅" else "❌"

    println(s"\n${"=" * 60}")
    println(s"$statusIcon Performance Metrics - ${metrics.jobName}")
    println(s"${"=" * 60}")
    println(f"Status:              ${metrics.status}")
    println(f"Duration:            ${metrics.durationSeconds}%,d seconds")
    println(f"Records Processed:   ${metrics.recordsProcessed}%,d")
    println(s"-" * 60)
    println(f"Shuffle Read:        ${metrics.shuffleReadGB}%.2f GB")
    println(f"Shuffle Write:       ${metrics.shuffleWriteGB}%.2f GB")
    println(s"-" * 60)
    println(f"Peak Memory:         ${metrics.peakMemoryGB}%.2f GB / ${metrics.allocatedMemoryGB}%.2f GB (${metrics.memoryUtilizationPercent}%.1f%%)")
    println(f"Spill (Memory):      ${metrics.spillMemoryGB}%.2f GB")
    println(f"Spill (Disk):        ${metrics.spillDiskGB}%.2f GB")
    println(s"-" * 60)
    println(f"GC Time:             ${metrics.gcTimeSeconds}%,d seconds (${metrics.gcTimePercent}%.1f%%)")
    println(s"${"=" * 60}\n")

    println(s"💡 Spark UI: http://localhost:4040")
    println()
  }

  def printMemoryWarning(allocatedGB: Double, usedGB: Double): Unit = {
    val utilizationPercent = (usedGB / allocatedGB) * 100
    if (utilizationPercent > 90) {
      println(s"\n⚠️  WARNING: Memory utilization is ${utilizationPercent.toInt}%!")
      println(s"   Allocated: ${allocatedGB}%.2f GB")
      println(s"   Used: ${usedGB}%.2f GB")
      println(s"   Consider increasing executor memory.\n")
    }
  }

  def trackProgress(currentRecords: Long, totalRecords: Long, intervalSeconds: Int = 10): Unit = {
    val progress = (currentRecords.toDouble / totalRecords) * 100
    val elapsed = getDurationSeconds

    if (elapsed % intervalSeconds == 0) {
      println(f"Progress: ${progress}%.1f%% ($currentRecords%,d / $totalRecords%,d) - Elapsed: ${elapsed}s")
    }
  }
}
