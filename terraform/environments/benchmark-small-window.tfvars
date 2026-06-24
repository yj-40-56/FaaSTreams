# Benchmark: 30-second tumbling windows.
# Tests whether shorter windows increase coordinator overhead or query latency.
# Use a different vpc_connector_cidr if running alongside another environment.

env_name    = "bench-30s"
window_size = 30

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
