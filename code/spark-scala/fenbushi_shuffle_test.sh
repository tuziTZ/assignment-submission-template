#!/bin/bash
# run_cluster_shuffletest.sh
set -euo pipefail

SPARK_HOME=/home/ubuntu/spark-1.6.3-bin-hadoop2.6
JAR=/home/ubuntu/spark-shuffle-test.jar
MASTER_URL=spark://ecnu01:7077  # 修改为你的 master 地址
OUTDIR=/home/ubuntu/shuffle_results        # 本地/共享目录，driver(client)会写到这里
mkdir -p "$OUTDIR/spark-events"

# grid
DATA_SIZES=(100000)
NUM_KEYS=(100 1000 10000)
PARTS=(4 8 16)
DISTS=(uniform zipf skew)
WORKLOADS=(agg join repart sort)

# resources (调整为你集群的可用资源)
NUM_EXECUTORS=4
EXECUTOR_CORES=2
EXECUTOR_MEM=1G
DRIVER_MEM=1G

SHUFFLE_MANAGERS=(hash sort)

for SM in "${SHUFFLE_MANAGERS[@]}"; do
  for DS in "${DATA_SIZES[@]}"; do
    for NK in "${NUM_KEYS[@]}"; do
      for P in "${PARTS[@]}"; do
        for DIST in "${DISTS[@]}"; do
          for W in "${WORKLOADS[@]}"; do
            RUN_ID="cluster-${SM}-${W}-${DIST}-${DS}-${NK}-${P}-$(date +%s)"
            OUTFILE="${OUTDIR}/run-${RUN_ID}.csv"
            echo "Submitting $RUN_ID to $MASTER_URL (SM=$SM)..."
            DATA_SIZE=$DS NUM_KEYS=$NK PARTS=$P DIST=$DIST WORKLOAD=$W \
            OUTPUT="$OUTFILE" RUN_ID="$RUN_ID" \
            $SPARK_HOME/bin/spark-submit \
              --class ShufflePerf \
              --master "$MASTER_URL" \
              --deploy-mode client \
              --num-executors $NUM_EXECUTORS \
              --executor-cores $EXECUTOR_CORES \
              --executor-memory $EXECUTOR_MEM \
              --driver-memory $DRIVER_MEM \
              --conf spark.shuffle.manager="$SM" \
              --conf spark.eventLog.enabled=true \
              --conf spark.eventLog.dir=file:$OUTDIR/spark-events \
              "$JAR" \
            || echo "Run failed: $RUN_ID"
            sleep 3
          done
        done
      done
    done
  done
done

echo "All cluster runs submitted. Results will be in $OUTDIR"