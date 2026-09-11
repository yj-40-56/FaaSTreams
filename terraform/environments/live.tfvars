# The real, live pipeline.

env_name = "live"

# REQUIRED — Terraform manages the shared Memorystore instance, so this must be the
# EXISTING instance's ID, not a new name. A wrong value here creates a second,
# billable Redis and strands the pipeline's data on the old one. Confirm it with:
#
#   gcloud redis instances list --project=faastreams --region=europe-west3
#
# then uncomment the line below and run `make import-redis` before any apply.
# Left unset deliberately: it could not be confirmed when this was written (the
# project's billing is disabled), and an unset required variable fails `plan` loudly
# instead of letting a guess reach `apply`.
#
# redis_instance_name = "<real-instance-id>"

# Every other value this env needs is already the default in variables.tf.
