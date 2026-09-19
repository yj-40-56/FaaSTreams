# One-command provisioning for the FaaSTreams pipeline.
#
# This wraps terraform/Makefile (via `$(MAKE) -C terraform`, not `include` — that
# Makefile's Docker volume mount assumes its own directory as cwd). Every target
# here is a thin delegation; terraform/Makefile has the full set and remains
# directly runnable on its own.
#
# IMPORTANT: push and pull ingestors must never run simultaneously — they write the
# same Redis keys. This repo's live pipeline uses the pull ingestor (ingestor-pull);
# see terraform/README.md and README.md.

ENV ?= live
# Subcommand for `make terraform-state`, e.g. ARGS="show module.worker.google_cloudfunctions2_function.this"
ARGS ?= list

.PHONY: help terraform-plan terraform-apply terraform-import-live terraform-state terraform-show \
        scheduler-pause scheduler-resume

help:
	@echo "make terraform-plan          - preview infra changes (ENV=$(ENV)), safe anytime"
	@echo "make terraform-apply         - apply infra changes (ENV=$(ENV)), interactive confirm"
	@echo "make terraform-import-live   - one-time: import existing live GCP resources into state"
	@echo "make terraform-state         - terraform state list (ARGS=\"show <addr>\" for other subcommands)"
	@echo "make terraform-show          - terraform show (full current state, human-readable)"
	@echo "make scheduler-pause         - pause both live Cloud Scheduler jobs"
	@echo "make scheduler-resume        - resume coordinator-5sec-trigger (JOBS=... to override)"
	@echo ""
	@echo "Individual scripts (scripts/*.sh) remain directly runnable."
	@echo "terraform/Makefile has the full terraform target set (plan/apply/destroy/import-live/...)."

terraform-plan:
	$(MAKE) -C terraform plan ENV=$(ENV)

terraform-apply:
ifeq ($(ENV),live)
	@echo "*** Applying against LIVE resources: ingestor-pull, windower, worker, data-sink,"
	@echo "*** ais-stream-pull, scheduler jobs. This affects the real running pipeline."
	@echo "*** Review the plan output above carefully before confirming."
endif
	$(MAKE) -C terraform apply ENV=$(ENV)

terraform-import-live:
	$(MAKE) -C terraform import-live

terraform-state:
	$(MAKE) -C terraform state ENV=$(ENV) ARGS="$(ARGS)"

terraform-show:
	$(MAKE) -C terraform show ENV=$(ENV)

scheduler-pause:
	$(MAKE) -C terraform scheduler-pause

scheduler-resume:
	$(MAKE) -C terraform scheduler-resume
