// ShuffleBench_fixed.scala
// Safe to run with: spark-shell -i ShuffleBench_fixed.scala (see run command below)

import java.io._
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Try
import math.ceil

// read parameters from environment
def getenvOr(name: String, default: String) = sys.env.getOrElse(name, default)
val dataSize   = getenvOr("DATA_SIZE", "1000000").toLong
val numKeys    = getenvOr("NUM_KEYS", "1000").toInt
val parts      = getenvOr("PARTS", "8").toInt
val workload   = getenvOr("WORKLOAD", "agg")
val output     = getenvOr("OUTPUT", "/tmp/bench_result.csv")
val runId      = getenvOr("RUN_ID", s"run-${System.currentTimeMillis()}")

// ensure output directory exists
{
  val outFile = new java.io.File(output)
  val parent = outFile.getParentFile
  if (parent != null && !parent.exists()) parent.mkdirs()
}

def log(s: String): Unit = {
  // use System.err to avoid name conflicts in REPL
  System.err.println(s)
  System.err.flush()
}

log(s"ShuffleBench starting: dataSize=$dataSize, numKeys=$numKeys, parts=$parts, workload=$workload, runId=$runId")

// Important: avoid capturing non-serializable objects in closures.
// We'll create local immutable copies of primitives so closures capture only those.
val localNumKeys = numKeys
val localDataSize = dataSize
val localNumPartitions = parts
val localWorkload = workload

// create RDD: use sc directly (sc is provided by spark-shell) but avoid capturing sc in nested closures later
val base = sc.parallelize(0L until localDataSize, localNumPartitions).map { i =>
  val key = (i % localNumKeys).toInt
  (key, 1)
}.cache()

// materialize input
base.count()

def jvmMem(): (Long, Long, Long) = {
  val r = Runtime.getRuntime
  (r.totalMemory(), r.freeMemory(), r.maxMemory())
}

val startMem = jvmMem()
val t0 = System.nanoTime()

val resultSummary = Try {
  localWorkload match {
    case "agg" =>
      // reduceByKey (has map-side combine)
      val r = base.reduceByKey(_ + _).map(_._2).sum()    // closure only references base (OK)
      ("agg", r)
    case "group" =>
      // groupByKey - heavy shuffle
      val r = base.groupByKey(localNumPartitions).map{ case (k, iter) => (k, iter.size) }.count()
      ("group", r)
    case "join" =>
      // prepare second RDD; avoid capturing outside vars
      val other = sc.parallelize(0L until localDataSize, localNumPartitions).map { i =>
        val key = (i % localNumKeys).toInt
        (key, 1)
      }.cache()
      other.count()
      val j = base.join(other).count()
      ("join", j)
    case other =>
      throw new IllegalArgumentException("Unknown WORKLOAD: " + other)
  }
}.get

val t1 = System.nanoTime()
val endMem = jvmMem()
val durationSeconds = (t1 - t0).toDouble / 1e9
val wallSecondsSinceStart = (t1 - t0).toDouble / 1e9

// write result to csv (append)
val header = "runId,timestamp,workload,dataSize,numKeys,parts,duration_s,totalRecords,resultVal,totalMemory,freeMemory,maxMemory"
val ts = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date())
val totalMemory = endMem._1
val freeMemory = endMem._2
val maxMemory = endMem._3
val csvLine = s"$runId,$ts,$localWorkload,$localDataSize,$localNumKeys,$localNumPartitions,$durationSeconds,${localDataSize},${resultSummary._2},$totalMemory,$freeMemory,$maxMemory\n"

val outFile = new File(output)
val fw = new FileWriter(outFile, true)
try {
  if (outFile.length() == 0) fw.write(header + "\n")
  fw.write(csvLine)
} finally {
  fw.close()
}

log("DONE: " + csvLine)

// exit to ensure spark-shell process ends when run in non-interactive mode
sys.exit(0)
