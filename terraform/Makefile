ENV ?= baseline

TF = docker run --rm -it \
	-v "$(shell pwd)/..:/workspace" \
	-v "$(HOME)/.config/gcloud:/root/.config/gcloud:ro" \
	-e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
	-w /workspace/terraform \
	hashicorp/terraform:1.9

# One-time: create the GCS bucket that stores Terraform state.
# Run once before make init.
bucket-init:
	docker run --rm \
		-v "$(HOME)/.config/gcloud:/root/.config/gcloud:ro" \
		-e CLOUDSDK_CORE_PROJECT=faastreams \
		gcr.io/google.com/cloudsdktool/google-cloud-cli:slim \
		gcloud storage buckets create gs://faastreams-terraform-state \
			--project=faastreams --location=europe-west3 \
			--uniform-bucket-level-access 2>/dev/null || true

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

.PHONY: bucket-init init validate plan apply destroy
