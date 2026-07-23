#!/bin/bash
set -e

# Creates a fresh pull subscription on ais-stream for the pull ingestor to drain.
# Run this before each pull-ingestor test run, and delete-pull-subscription.sh
# afterwards — the pull ingestor and push ingestor are never both deployed at
# once (see CONTEXT.md), so this subscription shouldn't outlive a single test run
# or it'll silently accumulate backlog between runs like the push path once did.

SUBSCRIPTION=ais-stream-pull

if gcloud pubsub subscriptions describe "$SUBSCRIPTION" >/dev/null 2>&1; then
  echo "Subscription $SUBSCRIPTION already exists — delete it first with delete-pull-subscription.sh"
  exit 1
fi

gcloud pubsub subscriptions create "$SUBSCRIPTION" --topic=ais-stream
echo "Created pull subscription $SUBSCRIPTION on topic ais-stream."
