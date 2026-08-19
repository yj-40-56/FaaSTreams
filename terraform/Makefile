ENV ?= live

TTY_FLAG = $(shell [ -t 0 ] && echo '-it' || echo '-i')
TF = docker run --rm $(TTY_FLAG) \
	-v "$(shell pwd)/..:/workspace" \
	-v "$(HOME)/.config/gcloud:/root/.config/gcloud:ro" \
	-e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
	-w /workspace/terraform \
	hashicorp/terraform:1.9

# One-time: create the GCS bucket that stores Terraform state.
bucket-init:
	gcloud storage buckets create gs://faastreams-terraform-state \
		--project=faastreams --location=europe-west3 \
		--uniform-bucket-level-access 2>/dev/null || true

# Delete orphaned GCP resources for ENV that exist outside Terraform state.
# Run before make apply if you hit "resource already exists" errors.
# NEVER run this with ENV=live — it deletes the real pipeline's resources.
purge-env:
	@if [ "$(ENV)" = "live" ]; then echo "Refusing to purge-env against ENV=live."; exit 1; fi
	gcloud storage rm -r gs://faastreams-tf-functions-source-$(ENV) --project=faastreams 2>/dev/null || true
	gcloud pubsub subscriptions delete ais-stream-pull-$(ENV) --project=faastreams 2>/dev/null || true
	gcloud functions delete ingestor-pull-$(ENV) --region=europe-west3 --gen2 --project=faastreams 2>/dev/null || true
	gcloud functions delete windower-$(ENV) --region=europe-west3 --gen2 --project=faastreams 2>/dev/null || true
	gcloud functions delete worker-$(ENV) --region=europe-west3 --gen2 --project=faastreams 2>/dev/null || true
	gcloud functions delete data-sink-$(ENV) --region=europe-west3 --gen2 --project=faastreams 2>/dev/null || true
	gcloud functions delete scheduler-task-queue-$(ENV) --region=europe-west3 --gen2 --project=faastreams 2>/dev/null || true
	gcloud scheduler jobs delete coordinator-5sec-trigger-$(ENV) --location=europe-west3 --project=faastreams --quiet 2>/dev/null || true
	gcloud scheduler jobs delete windower-tick-$(ENV) --location=europe-west3 --project=faastreams --quiet 2>/dev/null || true

# Save the most recent window results from Cloud Logging to results/.
# Run immediately after a benchmark run finishes.
save-results:
	python3 scripts/save_results.py --env=$(ENV) --project=faastreams

# google_cloud_scheduler_job has no pause/enabled field in the provider — pausing is
# an imperative gcloud action, not something `terraform apply` can express.
# scheduler-pause pauses both live jobs unconditionally (clean state before a benchmark run).
scheduler-pause:
	gcloud scheduler jobs pause coordinator-5sec-trigger --location=europe-west3 --project=faastreams 2>/dev/null || true
	gcloud scheduler jobs pause windower-tick --location=europe-west3 --project=faastreams 2>/dev/null || true

# scheduler-resume only resumes coordinator-5sec-trigger (today's live baseline).
# windower-tick is paused live today for reasons this change doesn't know — resuming
# it is a deliberate call, not a side effect of running a benchmark. Pass
# JOBS="coordinator-5sec-trigger windower-tick" to resume both explicitly.
JOBS ?= coordinator-5sec-trigger
scheduler-resume:
	for job in $(JOBS); do \
		gcloud scheduler jobs resume $$job --location=europe-west3 --project=faastreams 2>/dev/null || true; \
	done

init:
	$(TF) init -reconfigure

validate:
	$(TF) workspace select -or-create $(ENV)
	$(TF) validate

plan:
	$(TF) workspace select -or-create $(ENV)
	$(TF) plan -var-file="environments/$(ENV).tfvars"

# Drift check for `make benchmark`: fails (exit 2 from terraform, non-zero here) if
# applied infra differs from what's declared. Does not print full plan output.
plan-check:
	$(TF) workspace select -or-create $(ENV)
	$(TF) plan -var-file="environments/$(ENV).tfvars" -detailed-exitcode -input=false -lock=false

apply:
	$(TF) workspace select -or-create $(ENV)
	$(TF) apply -var-file="environments/$(ENV).tfvars"

# Read-only inspection. `make state` lists all resources; pass ARGS for other
# subcommands, e.g. `make state ARGS="show module.worker.google_cloudfunctions2_function.this"`.
ARGS ?= list
state:
	$(TF) workspace select -or-create $(ENV)
	$(TF) state $(ARGS)

show:
	$(TF) workspace select -or-create $(ENV)
	$(TF) show

# No default here for ENV=live safety — see the repo-root Makefile, which
# intentionally does NOT wrap this target. Run explicitly: make -C terraform destroy ENV=<env>
destroy:
	$(TF) workspace select -or-create $(ENV)
	$(TF) destroy -var-file="environments/$(ENV).tfvars"

# Runs the full terraform import list documented in terraform/README.md, one resource
# at a time, workspace `live`. Review `terraform plan` after this before ever applying.
import-live:
	$(TF) workspace select -or-create live
	$(TF) import -var-file="environments/live.tfvars" module.pubsub.google_pubsub_topic.ais_stream projects/faastreams/topics/ais-stream
	$(TF) import -var-file="environments/live.tfvars" module.pubsub.google_pubsub_subscription.ais_stream_pull projects/faastreams/subscriptions/ais-stream-pull
	$(TF) import -var-file="environments/live.tfvars" module.ingestor_pull.google_cloudfunctions2_function.this projects/faastreams/locations/europe-west3/functions/ingestor-pull
	$(TF) import -var-file="environments/live.tfvars" module.windower.google_cloudfunctions2_function.this projects/faastreams/locations/europe-west3/functions/windower
	$(TF) import -var-file="environments/live.tfvars" module.worker.google_cloudfunctions2_function.this projects/faastreams/locations/europe-west3/functions/worker
	$(TF) import -var-file="environments/live.tfvars" module.data_sink.google_cloudfunctions2_function.this projects/faastreams/locations/europe-west3/functions/data-sink
	$(TF) import -var-file="environments/live.tfvars" module.scheduler_task_queue.google_cloudfunctions2_function.this projects/faastreams/locations/europe-west3/functions/scheduler-task-queue
	$(TF) import -var-file="environments/live.tfvars" module.ingestor_pull.google_cloud_run_v2_service_iam_member.public_invoker "projects/faastreams/locations/europe-west3/services/ingestor-pull roles/run.invoker allUsers"
	$(TF) import -var-file="environments/live.tfvars" module.windower.google_cloud_run_v2_service_iam_member.public_invoker "projects/faastreams/locations/europe-west3/services/windower roles/run.invoker allUsers"
	$(TF) import -var-file="environments/live.tfvars" module.worker.google_cloud_run_v2_service_iam_member.public_invoker "projects/faastreams/locations/europe-west3/services/worker roles/run.invoker allUsers"
	$(TF) import -var-file="environments/live.tfvars" module.data_sink.google_cloud_run_v2_service_iam_member.public_invoker "projects/faastreams/locations/europe-west3/services/data-sink roles/run.invoker allUsers"
	$(TF) import -var-file="environments/live.tfvars" module.scheduler_task_queue.google_cloud_run_v2_service_iam_member.public_invoker "projects/faastreams/locations/europe-west3/services/scheduler-task-queue roles/run.invoker allUsers"
	$(TF) import -var-file="environments/live.tfvars" module.scheduler.google_cloud_scheduler_job.fanout_trigger projects/faastreams/locations/europe-west3/jobs/coordinator-5sec-trigger
	$(TF) import -var-file="environments/live.tfvars" module.scheduler.google_cloud_scheduler_job.ingestor_pull_trigger projects/faastreams/locations/europe-west3/jobs/windower-tick
	$(TF) import -var-file="environments/live.tfvars" google_cloud_tasks_queue.faastreams_queue projects/faastreams/locations/europe-west3/queues/faastreams-queue
	@echo ""
	@echo "Import batch complete. Run 'make plan ENV=live' now and review the diff"
	@echo "carefully before ever running apply — it must show only the expected"
	@echo "net-new additions (functions_source bucket + objects), not changes to"
	@echo "anything just imported."

.PHONY: bucket-init purge-env save-results scheduler-pause scheduler-resume init validate plan plan-check apply destroy import-live state show
