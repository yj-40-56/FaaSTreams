.DEFAULT_GOAL := plan

# One stack and GCS backend per explicit PROJECT. Operational targets verify
# the initialized backend before reading state or making changes.
export PROJECT
STATE_BUCKET_LOCATION ?= europe-west3

export TF_VAR_project_id = $(PROJECT)

TF_LOCAL := $(shell command -v terraform 2>/dev/null)
ifdef TF_LOCAL
TF = terraform
else
TTY_FLAG = $(shell [ -t 0 ] && echo '-it' || echo '-i')
TF = docker run --rm $(TTY_FLAG) \
	-v "$(shell pwd)/..:/workspace" \
	-v "$(HOME)/.config/gcloud:/root/.config/gcloud:ro" \
	-e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
	-e TF_VAR_project_id \
	-e TF_DATA_DIR \
	-e TF_WORKSPACE \
	-w /workspace/terraform \
	hashicorp/terraform:latest
endif

# Optional: only for settings that differ from the defaults in variables.tf.
VARS = $(if $(wildcard environments/$(PROJECT).tfvars),-var-file="environments/$(PROJECT).tfvars")
STATE_BUCKET = $(PROJECT)-terraform-state
# Explicit CLI variable takes precedence over any accidental tfvars project_id.
PROJECT_VAR = -var="project_id=$(PROJECT)"

check-project:
	@python3 check-context.py --project-only

check-context: check-project
	@python3 check-context.py

# One-time per project: the bucket holding this project's state.
bucket-init: check-project
	gcloud storage buckets create gs://$(STATE_BUCKET) --project=$(PROJECT) \
		--location=$(STATE_BUCKET_LOCATION) --uniform-bucket-level-access
	gcloud storage buckets update gs://$(STATE_BUCKET) --versioning

# A backend block can't read variables, so the bucket comes in at init time.
init: check-project
	$(TF) init -reconfigure \
		-backend-config="bucket=$(STATE_BUCKET)" \
		-backend-config="prefix=terraform/state"

validate:
	$(TF) validate

plan: check-context
	$(TF) plan $(VARS) $(PROJECT_VAR)

# Exit 2 when deployed infrastructure differs from the config.
plan-check: check-context
	$(TF) plan $(VARS) $(PROJECT_VAR) -detailed-exitcode -input=false -lock=false

apply: check-context
	$(TF) apply $(VARS) $(PROJECT_VAR)

# Everything, Redis data included. Asks for PROJECT by name first.
destroy: check-context
	@read -p "Destroy every pipeline resource in $(PROJECT)? Type the project ID: " p && [ "$$p" = "$(PROJECT)" ]
	$(TF) destroy $(VARS) $(PROJECT_VAR)

ARGS ?= list
state: check-context
	$(TF) state $(ARGS)

output: check-context
	$(TF) output $(OUTPUT_ARGS)

# New jobs are paused. Their subsequent pause/resume state is managed here.
scheduler-pause scheduler-resume: check-context
	@set -eu; \
	region=$$($(TF) output -raw region); \
	jobs_json=$$($(TF) output -json scheduler_jobs); \
	jobs=$$(printf '%s' "$$jobs_json" | python3 -c 'import json,sys; print(" ".join(json.load(sys.stdin)))'); \
	for job in $$jobs; do \
		gcloud scheduler jobs $(patsubst scheduler-%,%,$@) "$$job" --location="$$region" --project="$(PROJECT)"; \
	done

.PHONY: check-project check-context bucket-init init validate plan plan-check apply destroy state output scheduler-pause scheduler-resume
