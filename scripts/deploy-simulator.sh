gcloud run jobs deploy simulator-job \
  --source=. \
  --region=europe-west3 \
  --set-env-vars="CONFIG_BUCKET=faastreams-config,CONFIG_OBJECT=query-config.yaml,PUBSUB_PROJECT_ID=faastreams,PUBSUB_TOPIC_ID=ais-stream,SOURCE_NAME=ais_data_v1,SIM_RUNTIME=180m,SIM_SCALE_FACTOR=24" \
  --memory=512Mi \
  --task-timeout=200m