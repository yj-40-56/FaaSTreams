# The real, live pipeline. Every value below is a default already declared in
# variables.tf — this file exists mainly so `make terraform-plan`/`terraform-apply`
# always has an explicit -var-file to point at, and so future overrides for the
# live environment have an obvious home.

env_name = "live"
