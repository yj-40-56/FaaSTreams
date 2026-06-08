#!/bin/bash
set -e

VM_NAME="flink-ais"
ZONE="europe-west3-a"
FLINK_DIR="$HOME/flink-1.19.3"
JAR_NAME="ais-spe-1.0-SNAPSHOT.jar"

echo ">>> Build..."
mvn clean package -q

echo ">>> Copy jar to VM..."
gcloud compute scp target/$JAR_NAME $VM_NAME:$FLINK_DIR/ --zone=$ZONE

echo ">>> Stop old job..."
gcloud compute ssh $VM_NAME --zone=$ZONE --command="
  $FLINK_DIR/bin/flink list 2>/dev/null | grep RUNNING | awk '{print \$4}' | xargs -I{} $FLINK_DIR/bin/flink cancel {} 2>/dev/null || true
"

echo ">>> Restart cluster..."
gcloud compute ssh $VM_NAME --zone=$ZONE --command="
  $FLINK_DIR/bin/stop-cluster.sh
  sleep 2
  $FLINK_DIR/bin/start-cluster.sh
  sleep 3
"

echo ">>> Start job..."
gcloud compute ssh $VM_NAME --zone=$ZONE --command="
  $FLINK_DIR/bin/flink run -c com.ais.AisPipeline $FLINK_DIR/$JAR_NAME
"