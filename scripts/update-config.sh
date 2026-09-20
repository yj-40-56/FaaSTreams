#!/bin/bash
set -euo pipefail

PROJECT="${PROJECT:?set PROJECT to the target GCP project, e.g. PROJECT=faas-pj $0}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REGION="$(make --no-print-directory -s -C "$REPO_ROOT/terraform" output OUTPUT_ARGS='-raw region')"
CONFIG_BUCKET="${CONFIG_BUCKET:-$PROJECT-config}"
CONFIG_OBJECT="${CONFIG_OBJECT:-query-config.yaml}"

gcloud storage cp "gs://$CONFIG_BUCKET/$CONFIG_OBJECT" ./query-config.yaml --project="$PROJECT"
${EDITOR:-nano} ./query-config.yaml
gcloud storage cp ./query-config.yaml "gs://$CONFIG_BUCKET/$CONFIG_OBJECT" --project="$PROJECT"

# Both services read the config in init(), so only a cold start picks up the new
# one. Bumping an env var rolls a fresh revision; warm instances would otherwise
# keep serving the old config.
echo "Config updated, rolling ingestor-pull and windower..."
for service in ingestor-pull windower; do
  gcloud run services update "$service" \
    --region="$REGION" --project="$PROJECT" \
    --update-env-vars="CONFIG_RELOADED_AT=$(date -u +%Y%m%dT%H%M%SZ)"
done
echo "Done"
