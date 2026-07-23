#!/bin/bash
set -e

SUBSCRIPTION=ais-stream-pull

gcloud pubsub subscriptions delete "$SUBSCRIPTION"
echo "Deleted pull subscription $SUBSCRIPTION."
