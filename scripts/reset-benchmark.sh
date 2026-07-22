#!/bin/bash
set -euo pipefail

VM_NAME="flink-ais"
ZONE="europe-west3-a"
FLINK_DIR="\$HOME/flink-1.19.3"
JAR_NAME="ais-spe-1.0-SNAPSHOT.jar"

REDIS_HOST="10.101.64.19"
SUB_FAAS="ais-stream-pull"
SUB_SPE="spe-sub"

echo "Flush redis"
gcloud compute ssh redis-bastion --zone=$ZONE --command="
  redis-cli -h $REDIS_HOST FLUSHALL
"

echo "Reset Pub/sub subs"
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
gcloud pubsub subscriptions seek "$SUB_SPE" --time="$NOW"
gcloud pubsub subscriptions seek "$SUB_FAAS" --time="$NOW"

echo "Build flink JAR"
mvn -f ../src/benchmarks/flink/pom.xml clean package -q

echo "Uplaod JAR to VM"
gcloud compute scp ../src/benchmarks/flink/target/$JAR_NAME $VM_NAME:$FLINK_DIR/ --zone=$ZONE

gcloud compute ssh $VM_NAME --zone=$ZONE --command="
  timeout 20 $FLINK_DIR/bin/flink list 2>/dev/null | grep RUNNING | awk '{print \$4}' | xargs -r -I{} $FLINK_DIR/bin/flink cancel {} || true
"

echo "Restart flink cluster"
gcloud compute ssh $VM_NAME --zone=$ZONE --command="
  $FLINK_DIR/bin/stop-cluster.sh || true
  sleep 2
  $FLINK_DIR/bin/start-cluster.sh
  sleep 5
"

echo "Redeploy jobs"
gcloud compute ssh $VM_NAME --zone=$ZONE --command="
  $FLINK_DIR/bin/flink run -d -c com.ais.AisPipeline $FLINK_DIR/$JAR_NAME
"

echo "Start simulator"
./run-simulator.sh

echo "Done"