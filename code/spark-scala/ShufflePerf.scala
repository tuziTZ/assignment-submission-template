import java.io._
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Try
import scala.util.Random
import scala.math

import org.apache.spark.{SparkConf, SparkContext}

object ShufflePerf {
  def getenvOr(name: String, default: String) = sys.env.getOrElse(name, default)

  def jvmMem(): (Long, Long, Long) = {
    val r = Runtime.getRuntime
    (r.totalMemory(), r.freeMemory(), r.maxMemory())
  }

  def log(s: String): Unit = {
    System.err.println(s)
    System.err.flush()
  }

  def main(args: Array[String]): Unit = {
    // -----------------------------
    // Read env / settings
    // -----------------------------
    val dataSize = Try(getenvOr("DATA_SIZE", "1000000").toLong).getOrElse(1000000L)
    val numKeys = Try(getenvOr("NUM_KEYS", "1000").toInt).getOrElse(1000)
    val parts = Try(getenvOr("PARTS", "8").toInt).getOrElse(8)
    val workload = getenvOr("WORKLOAD", "agg")
    val dist = getenvOr("DIST", "uniform") // uniform / zipf / skew
    val output = getenvOr("OUTPUT", "/tmp/bench_result.csv")
    val runId = getenvOr("RUN_ID", s"run-${System.currentTimeMillis()}")

    log(s"ShufflePerf starting: dataSize=$dataSize, numKeys=$numKeys, parts=$parts, workload=$workload, dist=$dist, runId=$runId")

    // -----------------------------
    // Spark context
    // -----------------------------
    val conf = new SparkConf().setAppName("Shuffle Performance Test")
    val sc = new SparkContext(conf)

    try {
      // -----------------------------
      // Create base RDD using mapPartitionsWithIndex to avoid serializing Random
      // -----------------------------
      // We'll create a Range for indices partitioned into 'parts' partitions.
      val indices = 0L until dataSize

      // parallelize the range with given number of partitions and then use mapPartitionsWithIndex
      val base = sc.parallelize(indices, parts).mapPartitionsWithIndex(
        (partIndex: Int, iter: scala.Iterator[Long]) => {
          // local Random created inside partition (executor) -> not serialized from driver
          val seed = System.currentTimeMillis() ^ partIndex
          val rnd = new Random(seed)

          def generateKeyLocal(i: Long): Int = dist match {
            case "uniform" => (i % numKeys).toInt
            case "zipf" =>
              val theta = 1.2
              val r = rnd.nextDouble()
              val idx = (numKeys * math.pow(r, -1.0 / theta)).toInt
              math.min(math.max(idx, 0), numKeys - 1)
            case "skew" =>
              if (rnd.nextDouble() < 0.1) 0 else 1 + rnd.nextInt(math.max(1, numKeys - 1))
            case other =>
              throw new IllegalArgumentException("Unknown DIST=" + other)
          }

          // transform iterator of Long -> iterator of (Int, Int)
          iter.map { i =>
            val k = generateKeyLocal(i)
            (k, 1)
          }
        }
      ).cache()

      val inputCount = base.count()
      log(s"Input RDD materialized: records=$inputCount")

      // JVM mem before
      val startMem = jvmMem()
      val t0 = System.nanoTime()

      // -----------------------------
      // Execute workload
      // -----------------------------
      val resultSummary = Try {
        workload match {
          case "agg" =>
            val r = base.reduceByKey(_ + _).map { case (_, v) => v.toLong }.sum()
            ("agg", r)
          case "group" =>
            val grouped = base.groupByKey(parts).map { case (k, iter) => iter.size }
            val r = grouped.count()
            ("group", r)
          case "join" =>
            val other = sc.parallelize(0L until dataSize, parts).mapPartitionsWithIndex(
              (partIndex: Int, iter: scala.Iterator[Long]) => {
                val seed = System.currentTimeMillis() ^ partIndex ^ 0x9e3779b9L.toInt
                val rnd = new Random(seed)
                def generateKeyLocal(i: Long): Int = dist match {
                  case "uniform" => (i % numKeys).toInt
                  case "zipf" =>
                    val theta = 1.2
                    val r = rnd.nextDouble()
                    val idx = (numKeys * math.pow(r, -1.0 / theta)).toInt
                    math.min(math.max(idx, 0), numKeys - 1)
                  case "skew" =>
                    if (rnd.nextDouble() < 0.1) 0 else 1 + rnd.nextInt(math.max(1, numKeys - 1))
                  case other =>
                    throw new IllegalArgumentException("Unknown DIST=" + other)
                }
                iter.map { i =>
                  val k = generateKeyLocal(i)
                  (k, 1)
                }
              }
            ).cache()
            other.count()
            val j = base.join(other).count()
            ("join", j)
          case "sort" =>
            val r = base.sortByKey(true).count()
            ("sort", r)
          case "repart" =>
            val r = base.repartition(parts * 2).count()
            ("repart", r)
          case other =>
            throw new IllegalArgumentException("Unknown WORKLOAD=" + other)
        }
      }.get

      val t1 = System.nanoTime()
      val endMem = jvmMem()
      val durationSeconds = (t1 - t0).toDouble / 1e9

      // -----------------------------
      // Write CSV result
      // -----------------------------
      val header = "runId,timestamp,workload,dist,dataSize,numKeys,parts,duration_s,totalRecords,resultVal,totalMemory,freeMemory,maxMemory"
      val ts = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date())
      val totalMemory = endMem._1
      val freeMemory = endMem._2
      val maxMemory = endMem._3
      val csvLine = s"$runId,$ts,$workload,$dist,$dataSize,$numKeys,$parts,$durationSeconds,${inputCount},${resultSummary._2},$totalMemory,$freeMemory,$maxMemory\n"

      // ensure output dir exists
      val outFile = new File(output)
      val parent = outFile.getParentFile
      if (parent != null && !parent.exists()) parent.mkdirs()

      val fw = new FileWriter(outFile, true)
      try {
        if (outFile.length() == 0) fw.write(header + "\n")
        fw.write(csvLine)
      } finally {
        fw.close()
      }

      log("DONE: " + csvLine)
      println(s"Shuffle elapsed (s): $durationSeconds, result: ${resultSummary._2}")

    } catch {
      case e: Throwable =>
        log("ERROR running benchmark: " + e.getMessage)
        e.printStackTrace(System.err)
    } finally {
      sc.stop()
    }
  }
}
