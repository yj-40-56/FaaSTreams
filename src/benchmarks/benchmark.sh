#!/bin/bash
set -euo pipefail

REDIS_HOST="10.101.64.19"
SUB_AIS="ais-stream-sub"
SUB_SPE="spe-input-sub"
CSV_PATH="../../data/ais.csv"

gcloud run services update coordinator-benchmark-sid \
  --region=europe-west3 \
  --max-instances=1

gcloud compute ssh redis-bastion --zone=europe-west3-a --command "
  redis-cli -h $REDIS_HOST FLUSHALL
"

NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
gcloud pubsub subscriptions seek "$SUB_AIS" --time="$NOW"
gcloud pubsub subscriptions seek "$SUB_SPE" --time="$NOW"

#mvn -f flink/pom.xml clean package -q

JAR_NAME="ais-spe-1.0-SNAPSHOT.jar"
VM_NAME="flink-ais"
ZONE="europe-west3-a"
FLINK_DIR="~/flink-1.19.3"

#gcloud compute scp flink/target/$JAR_NAME $VM_NAME:$FLINK_DIR/ --zone=$ZONE

gcloud compute ssh $VM_NAME --zone=$ZONE --command="
  $FLINK_DIR/bin/flink list 2>/dev/null | grep RUNNING | awk '{print \$4}' | xargs -I{} $FLINK_DIR/bin/flink cancel {} 2>/dev/null || true
  $FLINK_DIR/bin/stop-cluster.sh
  sleep 2
  $FLINK_DIR/bin/start-cluster.sh
  sleep 3
  $FLINK_DIR/bin/flink run -d -c com.ais.AisPipeline $FLINK_DIR/$JAR_NAME
"
sleep 10

gcloud run services update coordinator-benchmark-sid \
  --region=europe-west3 \
  --max-instances=10

cd ../simulator
PUBSUB_PROJECT_ID=faastreams PUBSUB_TOPIC_ID=ais-stream CSV_PATH=$CSV_PATH go run .
