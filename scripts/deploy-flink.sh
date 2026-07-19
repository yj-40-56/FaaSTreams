#!/bin/bash
set -e

VM_NAME="flink-ais"
ZONE="europe-west3-a"
FLINK_DIR="$HOME/flink-1.19.3"
JAR_NAME="ais-spe-1.0-SNAPSHOT.jar"

mvn -f ../src/benchmarks/flink/pom.xml clean package -q

echo "Uploading JAR..."
gcloud compute scp ../src/benchmarks/flink/target/$JAR_NAME $VM_NAME:$FLINK_DIR/ --zone=$ZONE

echo "Cancelling old jobs..."
gcloud compute ssh $VM_NAME --zone=$ZONE --command="
  timeout 20 $FLINK_DIR/bin/flink list 2>/dev/null | grep RUNNING | awk '{print \$4}' | xargs -r -I{} $FLINK_DIR/bin/flink cancel {} || true
"

echo "Restarting Flink..."
gcloud compute ssh $VM_NAME --zone=$ZONE --command="
  $FLINK_DIR/bin/stop-cluster.sh || true
  sleep 2
  $FLINK_DIR/bin/start-cluster.sh
  sleep 3
"

echo "Submitting job..."
gcloud compute ssh $VM_NAME --zone=$ZONE --command="
  $FLINK_DIR/bin/flink run -d -c com.ais.AisPipeline $FLINK_DIR/$JAR_NAME
"

echo "Done."