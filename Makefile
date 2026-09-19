# Thin wrappers around terraform/Makefile, which has the full target set.

PROJECT ?= faas-pj
ARGS    ?= list

TF_MAKE = $(MAKE) -C terraform PROJECT=$(PROJECT)

.PHONY: help terraform-init terraform-plan terraform-apply terraform-state terraform-output \
        scheduler-pause scheduler-resume

help:
	@echo "make terraform-init      - init against PROJECT's state bucket (PROJECT=$(PROJECT))"
	@echo "make terraform-plan      - preview infra changes, safe anytime"
	@echo "make terraform-apply     - apply infra changes, interactive confirm"
	@echo "make terraform-state     - terraform state list (ARGS=\"show <addr>\" for others)"
	@echo "make terraform-output    - service URLs, Redis host, scheduler jobs"
	@echo "make scheduler-pause     - pause every ingestor tick"
	@echo "make scheduler-resume    - resume every ingestor tick"

terraform-init:
	$(TF_MAKE) init

terraform-plan:
	$(TF_MAKE) plan

terraform-apply:
	$(TF_MAKE) apply

terraform-state:
	$(TF_MAKE) state ARGS="$(ARGS)"

terraform-output:
	$(TF_MAKE) output

scheduler-pause:
	$(TF_MAKE) scheduler-pause

scheduler-resume:
	$(TF_MAKE) scheduler-resume
