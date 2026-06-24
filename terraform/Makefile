ENV ?= baseline

TF = docker run --rm \
	-v "$(shell pwd)/..:/workspace" \
	-v "$(HOME)/.config/gcloud:/root/.config/gcloud:ro" \
	-e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
	-w /workspace/terraform \
	hashicorp/terraform:1.9

init:
	$(TF) init

validate:
	$(TF) validate

plan:
	$(TF) plan -var-file="environments/$(ENV).tfvars"

apply:
	$(TF) apply -auto-approve -var-file="environments/$(ENV).tfvars"

destroy:
	$(TF) destroy -auto-approve -var-file="environments/$(ENV).tfvars"

.PHONY: init validate plan apply destroy
