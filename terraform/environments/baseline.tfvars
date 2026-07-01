# Baseline environment — mirrors the existing demo deployment.
# Use this as the control for benchmarks.

env_name   = "baseline"
window_size = 60

worker_memory        = "2048Mi"
worker_max_instances = 10
worker_timeout       = 540

coordinator_memory        = "1024Mi"
coordinator_cpu           = "1"
coordinator_concurrency   = 100
coordinator_max_instances = 10

data_sink_memory        = "256Mi"
data_sink_max_instances = 10

vpc_connector_name = "redis-eu-west3-connector"
