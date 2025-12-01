#!/usr/bin/env bash
SPARK_SHELL=/home/tuzi/spark-1.6.3-bin-hadoop2.6/bin/spark-shell
SCALA_SCRIPT=/home/tuzi/bench/ShuffleBench.scala
EVENT_DIR=/home/tuzi/spark-event-logs
RESULT_DIR=/home/tuzi/bench_results
PARALLELISM=8

mkdir -p "$EVENT_DIR" "$RESULT_DIR"

# parameters to sweep
declare -a SIZES=(1000000 5000000 20000000)   # 1M, 5M, 20M records (根据机器能力调整)
declare -a KEYSETS=(100 1000 10000)           # small/med/large cardinality
WORKLOADS=(agg group join)
MANAGERS=(hash sort)

for mgr in "${MANAGERS[@]}"; do
  for size in "${SIZES[@]}"; do
    for keys in "${KEYSETS[@]}"; do
      for w in "${WORKLOADS[@]}"; do
        runid="${mgr}_${w}_${size}_${keys}_$(date +%s)"
        outcsv="${RESULT_DIR}/${runid}.csv"
        echo "RUN: mgr=$mgr workload=$w size=$size keys=$keys > $runid"

        # set event log location per run (so parse can map file -> run)
        export SPARK_EVENT_LOG_DIR="${EVENT_DIR}/${runid}"
        mkdir -p "${SPARK_EVENT_LOG_DIR}"
        # We will pass spark.eventLog.dir via --conf and event logging enabled
        DATA_SIZE=$size NUM_KEYS=$keys PARTS=$PARALLELISM WORKLOAD=$w OUTPUT=$outcsv RUN_ID=$runid \
          $SPARK_SHELL -i $SCALA_SCRIPT \
          --conf spark.eventLog.enabled=true \
          --conf spark.eventLog.dir=file:${SPARK_EVENT_LOG_DIR} \
          --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
          --conf spark.shuffle.manager=$mgr \
          --driver-memory 8g

        # parse event logs of this run
        python3 /home/tuzi/bench/parse_event_log.py "${SPARK_EVENT_LOG_DIR}" "${RESULT_DIR}/${runid}_shuffle.csv"
        # small pause
        sleep 3
      done
    done
  done
done
