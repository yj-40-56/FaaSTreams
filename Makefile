# One-command provisioning + benchmarking for the FaaSTreams pipeline.
#
# This wraps terraform/Makefile (via `$(MAKE) -C terraform`, not `include` — that
# Makefile's Docker volume mount assumes its own directory as cwd) and the existing
# scripts/ deploy/benchmark tools. Every individual script remains directly runnable
# on its own for targeted testing; this Makefile only composes them.
#
# IMPORTANT: push and pull ingestors must never run simultaneously — they write the
# same Redis keys. This repo's live pipeline uses the pull ingestor (ingestor-pull);
# see terraform/README.md and README.md.

ENV ?= live
# Data source both benchmark targets validate. Defaults to the T-Drive taxi replay
# path (tdrive_data_v1) — override with SOURCE=ais_data_v1 to exercise the AIS
# vessel pipeline instead (scripts/run-simulator.sh, unaffected by this variable,
# remains directly runnable for that path).
SOURCE ?= tdrive_data_v1

.PHONY: help terraform-plan terraform-apply terraform-import-live \
        scheduler-pause scheduler-resume benchmark benchmark-full

help:
	@echo "make terraform-plan          - preview infra changes (ENV=$(ENV)), safe anytime"
	@echo "make terraform-apply         - apply infra changes (ENV=$(ENV)), interactive confirm"
	@echo "make terraform-import-live   - one-time: import existing live GCP resources into state"
	@echo "make scheduler-pause         - pause both live Cloud Scheduler jobs"
	@echo "make scheduler-resume        - resume coordinator-5sec-trigger (JOBS=... to override)"
	@echo "make benchmark               - short (~2 min) taxi-burst end-to-end benchmark, pass/fail per stage"
	@echo "make benchmark-full          - full 96-minute train-day taxi replay benchmark"
	@echo "                                (SOURCE=ais_data_v1 to run either against the AIS pipeline instead)"
	@echo ""
	@echo "Individual scripts (scripts/*.sh, terraform/scripts/*.py) remain directly runnable."
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

scheduler-pause:
	$(MAKE) -C terraform scheduler-pause

scheduler-resume:
	$(MAKE) -C terraform scheduler-resume

# Short, cheap benchmark: ~2 minutes of real traffic (taxi records sliced from the
# trainday workload by default — SOURCE=ais_data_v1 for the AIS vessel simulator
# instead). Checks infra isn't drifted (aborts on unexpected drift — pass
# SKIP_INFRA_CHECK=1 to bypass), pauses the fan-out scheduler for a clean run,
# resets Redis/Pub/Sub state, replays traffic, triggers ingestor-pull directly
# (never touches windower-tick's live paused state), and reports PASS/FAIL per
# pipeline stage. Always resumes the scheduler after, even on failure.
benchmark:
	@if [ "$${SKIP_INFRA_CHECK:-}" != "1" ]; then \
		make -C terraform plan-check ENV=$(ENV) || \
		{ echo ""; echo "terraform plan shows drift from applied state — review with 'make terraform-plan' before benchmarking, or set SKIP_INFRA_CHECK=1 to bypass."; exit 1; }; \
	fi
	$(MAKE) scheduler-pause
	@trap 'make -C terraform scheduler-resume' EXIT; \
	  bash scripts/reset-pipeline.sh --no-wait --source $(SOURCE) && \
	  if [ "$(SOURCE)" = "tdrive_data_v1" ]; then bash scripts/run-taxi-burst.sh; else bash scripts/run-simulator.sh; fi && \
	  bash scripts/benchmark-check.sh --source $(SOURCE)

# Full 96-minute train-day replay. Requires the T-Drive workload CSV, which is a
# one-time manual data-prep step (raw T-Drive taxi dataset + the
# scripts/local-data-analysis/tdrive_*.py pipeline) — not something `make` can
# generate on its own. This target checks for its presence and fails with clear
# instructions rather than silently no-op'ing.
benchmark-full:
	@test -f data/tdrive_workload_trainday.csv || { \
		echo "data/tdrive_workload_trainday.csv not found."; \
		echo "Generate it first via scripts/local-data-analysis/tdrive_build_trainday.py"; \
		echo "and tdrive_build_workload.py (see train_day.py and scripts/run-tdrive-replay.sh)."; \
		exit 1; \
	}
	@if [ "$${SKIP_INFRA_CHECK:-}" != "1" ]; then \
		make -C terraform plan-check ENV=$(ENV) || \
		{ echo ""; echo "terraform plan shows drift from applied state — review with 'make terraform-plan' before benchmarking, or set SKIP_INFRA_CHECK=1 to bypass."; exit 1; }; \
	fi
	$(MAKE) scheduler-pause
	@trap 'make -C terraform scheduler-resume' EXIT; \
	  bash scripts/reset-pipeline.sh --no-wait --source $(SOURCE) && \
	  INGESTOR_PULL_URL=$$(gcloud functions describe ingestor-pull --gen2 --region europe-west3 --project faastreams --format='value(serviceConfig.uri)') && \
	  bash scripts/run-tdrive-replay.sh "$$INGESTOR_PULL_URL" trainday && \
	  bash scripts/benchmark-check.sh --full --source $(SOURCE)

# No terraform-destroy wrapper here on purpose — ENV defaults to live, and destroy
# should require deliberately typing `make -C terraform destroy ENV=...`.
