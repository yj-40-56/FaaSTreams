# PROJECT is the only thing a deployment needs: one stack and one state per
# project, the state in a bucket inside that project. It sets project_id and
# names the state bucket, so the two can't drift apart. environments/ holds
# only a project's non-default settings, and most projects need none.
# Credentials are the gcloud application-default ones, so they must be for
# PROJECT's account.
PROJECT ?= faas-pj
REGION  ?= europe-west3

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
	-w /workspace/terraform \
	hashicorp/terraform:latest
endif

# Optional: only for settings that differ from the defaults in variables.tf.
VARS = $(if $(wildcard environments/$(PROJECT).tfvars),-var-file="environments/$(PROJECT).tfvars")
JOBS = $$($(TF) output -json scheduler_jobs | tr -d '[]"' | tr ',' ' ')
STATE_BUCKET = $(PROJECT)-terraform-state

# One-time per project: the bucket holding this project's state.
bucket-init:
	gcloud storage buckets create gs://$(STATE_BUCKET) --project=$(PROJECT) \
		--location=$(REGION) --uniform-bucket-level-access
	gcloud storage buckets update gs://$(STATE_BUCKET) --versioning

# A backend block can't read variables, so the bucket comes in at init time.
init:
	$(TF) init -reconfigure \
		-backend-config="bucket=$(STATE_BUCKET)" \
		-backend-config="prefix=terraform/state"

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
