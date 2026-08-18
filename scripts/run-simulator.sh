cd ../src/simulator

# The simulator is configured from env vars only, it doesn't read the query config.
# SOURCE_NAME and the timestamp settings must still match the source in that config.
# SIM_SCALE_FACTOR is CSV duration / desired real duration, SIM_RUNTIME sets duration
# of the simulation in real time. SIM_CSV_DELIMITER defaults to ","
PUBSUB_PROJECT_ID=faastreams \
  PUBSUB_TOPIC_ID=ais-stream \
  SOURCE_NAME=ais_data_v1 \
  SIM_CSV_PATH=../../data/ais.csv \
  SIM_TIMESTAMP_FIELD="# Timestamp" \
  SIM_TIMESTAMP_FORMAT="02/01/2006 15:04:05" \
  SIM_RUNTIME=2m \
  SIM_SCALE_FACTOR=24 \
  go run .
