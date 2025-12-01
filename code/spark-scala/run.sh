mkdir -p /home/tuzi/spark-event-logs/run_hash
DATA_SIZE=10000000 NUM_KEYS=1000 PARTS=8 WORKLOAD=agg OUTPUT=/home/tuzi/bench_results/run_hash.csv RUN_ID=hash_run_1 \
/home/tuzi/spark-1.6.3-bin-hadoop2.6/bin/spark-shell -i /home/tuzi/spark-scala/ShuffleBench.scala \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:/home/tuzi/spark-event-logs/run_hash \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.shuffle.manager=hash \
  --driver-memory 8g
