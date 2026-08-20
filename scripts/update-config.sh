#!/bin/bash
set -e

gsutil cp gs://faastreams-config/query-config.yaml ./query-config.yaml
${EDITOR:-nano} ./query-config.yaml
gsutil cp ./query-config.yaml gs://faastreams-config/query-config.yaml

echo "Config updated, redeploying ingestor and windower..."
bash deploy-ingestor-pull.sh
bash deploy-windower.sh
echo "Done"