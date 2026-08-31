cd ../src/simulator

GO_BIN="${GO_BIN:-$(command -v go || true)}"
if [ -z "$GO_BIN" ]; then
  for candidate in /usr/local/go/bin/go /usr/lib/golang/bin/go "$HOME/go/bin/go" /snap/bin/go; do
    [ -x "$candidate" ] && GO_BIN="$candidate" && break
  done
fi
[ -n "$GO_BIN" ] || { echo "go not found -- set GO_BIN=/path/to/go or add it to PATH" >&2; exit 1; }

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
  SIM_SEQ_FIELD=_seq \
  "$GO_BIN" run .
