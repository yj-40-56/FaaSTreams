# Benchmark: High-memory worker configuration.
# Tests whether more memory (4 GB) reduces query latency for large windows
# or high-cardinality spatial queries. Also bumps coordinator concurrency.
# Use a different vpc_connector_cidr if running alongside another environment.

env_name    = "bench-highmem"
window_size = 60

worker_memory        = "4096Mi"
worker_max_instances = 5
worker_timeout       = 540

coordinator_memory        = "2048Mi"
coordinator_cpu           = "2"
coordinator_concurrency   = 200
coordinator_max_instances = 5

data_sink_memory        = "512Mi"
data_sink_max_instances = 5

vpc_connector_name = "redis-eu-west3-connector"
