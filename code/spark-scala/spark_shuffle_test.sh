#!/bin/bash
# run_local_shuffletest.sh
set -euo pipefail

SPARK_HOME=/home/tuzi/spark-1.6.3-bin-hadoop2.6  # 修改为你的 spark 路径
JAR=/home/tuzi/spark-scala/spark-shuffle-test.jar   # 修改为你的 jar
OUTDIR=./output
mkdir -p "$OUTDIR"

# experiment param grid (你可以按需编辑)
DATA_SIZES=(1000000 5000000)
NUM_KEYS=(1000 10000)
PARTS=(8 32)
DISTS=(uniform zipf)
WORKLOADS=(agg join repart)

CORES=8  # local 模式的并发 cores，调整为你本机 CPU
MASTER="local[$CORES]"

# shuffle managers to compare
SHUFFLE_MANAGERS=(hash sort)

for SM in "${SHUFFLE_MANAGERS[@]}"; do
  for DS in "${DATA_SIZES[@]}"; do
    for NK in "${NUM_KEYS[@]}"; do
      for P in "${PARTS[@]}"; do
        for DIST in "${DISTS[@]}"; do
          for W in "${WORKLOADS[@]}"; do
            RUN_ID="local-${SM}-${W}-${DIST}-${DS}-${NK}-${P}-$(date +%s)"
            OUTFILE="${OUTDIR}/run-${RUN_ID}.csv"
            echo "=== RUN $RUN_ID ==="
            echo "SM=$SM DS=$DS NK=$NK PARTS=$P DIST=$DIST WORKLOAD=$W"
            DATA_SIZE=$DS NUM_KEYS=$NK PARTS=$P DIST=$DIST WORKLOAD=$W \
            OUTPUT="$OUTFILE" RUN_ID="$RUN_ID" \
            spark-submit \
              --class ShufflePerf \
              --master "$MASTER" \
              --conf spark.shuffle.manager="$SM" \
              "$JAR" \
            || echo "Run failed: $RUN_ID"
            sleep 1
          done
        done
      done
    done
  done
done

echo "All local runs finished. Results in $OUTDIR"
