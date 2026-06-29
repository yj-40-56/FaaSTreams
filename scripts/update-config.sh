#!/bin/bash
set -e

gsutil cp gs://faastreams-config/query-config.yaml ./query-config.yaml
${EDITOR:-nano} ./query-config.yaml
gsutil cp ./query-config.yaml gs://faastreams-config/query-config.yaml

echo "Config updated, redeploying coordinator..."
bash ../src/coordinator/deploy-demo.sh
echo "Done"