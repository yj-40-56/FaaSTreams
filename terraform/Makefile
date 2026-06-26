ENV ?= baseline

TF = docker run --rm -it \
	-v "$(shell pwd)/..:/workspace" \
	-v "$(HOME)/.config/gcloud:/root/.config/gcloud:ro" \
	-e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
	-w /workspace/terraform \
	hashicorp/terraform:1.9

# One-time: create the GCS bucket that stores Terraform state.
# Run this once on Cloud Shell before anything else.
bucket-init:
	gcloud storage buckets create gs://faastreams-terraform-state \
		--project=faastreams --location=europe-west3 \
		--uniform-bucket-level-access 2>/dev/null || true

init:
	$(TF) init

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

.PHONY: bucket-init init validate plan apply destroy
