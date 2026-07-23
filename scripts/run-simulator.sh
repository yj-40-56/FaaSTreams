cd ../src/simulator

# SIM_RUNTIME sets duration of the simulation in real time
# SIM_SCALE_FACTOR overrides scale_factor from query-config.yaml
CONFIG_BUCKET=faastreams-config \
  CONFIG_OBJECT=query-config.yaml \
  PUBSUB_PROJECT_ID=faastreams \
  PUBSUB_TOPIC_ID=ais-stream \
  SOURCE_NAME=ais_data_v1 \
  SIM_RUNTIME=2m \
  SIM_SCALE_FACTOR=24 \
  go run .
