#!/bin/bash
set -e

gsutil cp gs://faastreams-config/query-config-sidar.yaml ./query-config-sidar.yaml
${EDITOR:-nano} ./query-config-sidar.yaml
gsutil cp ./query-config-sidar.yaml gs://faastreams-config/query-config-sidar.yaml

echo "Config updated, redeploying ingestor and windower..."
bash deploy-ingestor-pull.sh
bash deploy-windower.sh
echo "Done"