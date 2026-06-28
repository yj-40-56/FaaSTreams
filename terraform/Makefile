ENV ?= baseline
IMAGE_REPO = europe-west3-docker.pkg.dev/faastreams/worker-images

TTY_FLAG = $(shell [ -t 0 ] && echo '-it' || echo '-i')
TF = docker run --rm $(TTY_FLAG) \
	-v "$(shell pwd)/..:/workspace" \
	-v "$(HOME)/.config/gcloud:/root/.config/gcloud:ro" \
	-e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
	-w /workspace/terraform \
	hashicorp/terraform:1.9

# Build the worker Docker image and push to Artifact Registry.
# Run this before make apply whenever the worker source or Dockerfile changes.
build-worker:
	gcloud artifacts repositories create worker-images \
		--repository-format=docker --location=europe-west3 \
		--project=faastreams 2>/dev/null || true
	gcloud builds submit .. \
		--project=faastreams \
		--region=europe-west3 \
		--config=docker/worker.cloudbuild.yaml \
		--substitutions=_IMAGE=$(IMAGE_REPO)/worker-$(ENV)

# One-time: create the GCS bucket that stores Terraform state.
bucket-init:
	gcloud storage buckets create gs://faastreams-terraform-state \
		--project=faastreams --location=europe-west3 \
		--uniform-bucket-level-access 2>/dev/null || true

# Delete orphaned GCP resources for ENV that exist outside Terraform state.
# Run before make apply if you hit "resource already exists" errors.
purge-env:
	gcloud storage rm -r gs://faastreams-functions-source-$(ENV) --project=faastreams 2>/dev/null || true
	gcloud pubsub subscriptions delete ais-stream-$(ENV)-sub --project=faastreams 2>/dev/null || true
	gcloud pubsub topics delete ais-stream-$(ENV) --project=faastreams 2>/dev/null || true
	gcloud functions delete coordinator-$(ENV) --region=europe-west3 --gen2 --project=faastreams 2>/dev/null || true
	gcloud run services delete worker-$(ENV) --region=europe-west3 --project=faastreams --quiet 2>/dev/null || true
	gcloud functions delete data-sink-$(ENV) --region=europe-west3 --gen2 --project=faastreams 2>/dev/null || true

# Flush all coordinator state from Redis before a benchmark run.
# Required when re-running after a previous session: stale window_end keys cause
# the coordinator to catch-up through thousands of empty historical windows.
flush-redis:
	$(eval CODE := $(shell base64 -w 0 scripts/redis_flush.py))
	gcloud run jobs create redis-flush-$(ENV) \
		--image=$(IMAGE_REPO)/worker-$(ENV) \
		--region=europe-west3 --project=faastreams \
		--vpc-connector=projects/faastreams/locations/europe-west3/connectors/redis-eu-west3-connector \
		--set-env-vars="CODE=$(CODE),REDIS_HOST=10.101.64.19,REDIS_PORT=6379,ENV=$(ENV)" \
		--command=/bin/sh \
		--args='-c,echo $$CODE | base64 -d | python3' 2>/dev/null || true
	gcloud run jobs execute redis-flush-$(ENV) --region=europe-west3 --project=faastreams --wait
	gcloud run jobs delete redis-flush-$(ENV) --region=europe-west3 --project=faastreams --quiet 2>/dev/null || true

init:
	$(TF) init -reconfigure

validate:
	$(TF) workspace select -or-create $(ENV)
	$(TF) validate

plan:
	$(TF) workspace select -or-create $(ENV)
	$(TF) plan -var-file="environments/$(ENV).tfvars"

apply:
	$(TF) workspace select -or-create $(ENV)
	$(TF) apply -var-file="environments/$(ENV).tfvars"

destroy:
	$(TF) workspace select -or-create $(ENV)
	$(TF) destroy -var-file="environments/$(ENV).tfvars"

.PHONY: build-worker bucket-init purge-env init validate plan apply destroy
