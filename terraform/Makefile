# PROJECT picks environments/<PROJECT>.{tfvars,backend.hcl}: one stack and one
# state per project, the state in a bucket inside that project. Credentials are
# the gcloud application-default ones, so they must be for PROJECT's account.
PROJECT ?= faas-pj
REGION  ?= europe-west3

TF_LOCAL := $(shell command -v terraform 2>/dev/null)
ifdef TF_LOCAL
TF = terraform
else
TTY_FLAG = $(shell [ -t 0 ] && echo '-it' || echo '-i')
TF = docker run --rm $(TTY_FLAG) \
	-v "$(shell pwd)/..:/workspace" \
	-v "$(HOME)/.config/gcloud:/root/.config/gcloud:ro" \
	-e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
	-w /workspace/terraform \
	hashicorp/terraform:latest
endif

VARS = -var-file="environments/$(PROJECT).tfvars"
JOBS = $$($(TF) output -json scheduler_jobs | tr -d '[]"' | tr ',' ' ')
STATE_BUCKET = $(shell sed -nE 's/^bucket *= *"([^"]+)".*/\1/p' environments/$(PROJECT).backend.hcl)

# One-time per project: the bucket holding this project's state.
bucket-init:
	gcloud storage buckets create gs://$(STATE_BUCKET) --project=$(PROJECT) \
		--location=$(REGION) --uniform-bucket-level-access
	gcloud storage buckets update gs://$(STATE_BUCKET) --versioning

init:
	$(TF) init -reconfigure -backend-config="environments/$(PROJECT).backend.hcl"

validate:
	$(TF) validate

plan:
	$(TF) plan $(VARS)

# Exit 2 when deployed infrastructure differs from the config.
plan-check:
	$(TF) plan $(VARS) -detailed-exitcode -input=false -lock=false

apply:
	$(TF) apply $(VARS)

# Everything, Redis data included. Asks for PROJECT by name first.
destroy:
	@read -p "Destroy every pipeline resource in $(PROJECT)? Type the project ID: " p && [ "$$p" = "$(PROJECT)" ]
	$(TF) destroy $(VARS)

ARGS ?= list
state:
	$(TF) state $(ARGS)

output:
	$(TF) output

# Pause state is not declarable in the provider, so it lives here.
scheduler-pause:
	for job in $(JOBS); do \
		gcloud scheduler jobs pause $$job --location=$(REGION) --project=$(PROJECT); \
	done

scheduler-resume:
	for job in $(JOBS); do \
		gcloud scheduler jobs resume $$job --location=$(REGION) --project=$(PROJECT); \
	done

.PHONY: bucket-init init validate plan plan-check apply destroy state output scheduler-pause scheduler-resume
