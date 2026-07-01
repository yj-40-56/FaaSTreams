cd ../src/simulator

CONFIG_BUCKET=faastreams-config \
CONFIG_OBJECT=query-config.yaml \
PUBSUB_PROJECT_ID=faastreams \
PUBSUB_TOPIC_ID=ais-stream \
SOURCE_NAME=ais_data_v1 \
go run .