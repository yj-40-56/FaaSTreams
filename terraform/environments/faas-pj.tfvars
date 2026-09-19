# Personal sandbox. Everything here was measured before it was in Terraform:
# see FaaStreams deployment.md in the notes vault.

project_id = "faas-pj"

# deploy-sandbox.sh --clean flushes Redis over plain `gcloud compute ssh`.
bastion_allow_external_ssh = true
