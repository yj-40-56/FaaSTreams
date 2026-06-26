ENV ?= baseline
IMAGE_REPO = europe-west3-docker.pkg.dev/faastreams/worker-images

TF = docker run --rm -it \
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
	gcloud functions delete worker-$(ENV) --region=europe-west3 --gen2 --project=faastreams 2>/dev/null || true
	gcloud functions delete data-sink-$(ENV) --region=europe-west3 --gen2 --project=faastreams 2>/dev/null || true

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
