# Google Cloud Overview

## Project

| Field      | Value      |
| ---------- | ---------- |
| Name       | FaaSTreams |
| Project ID | faastreams |

## Region & Zone

| Field        | Value                    |
| ------------ | ------------------------ |
| Default zone | europe-west3 (Frankfurt) |

## Compute Instances

### redis-bastion

| Field        | Value          |
| ------------ | -------------- |
| Zone         | europe-west3-a |
| Machine type | e2-micro       |
| Image        | debian-12      |
| Internal IP  | 10.156.0.2     |
| External IP  | 34.107.121.225 |

## Services

### Redis (Memorystore)

| Field    | Value                        |
| -------- | ---------------------------- |
| Tier     | Basic                        |
| Topology | Standalone (single instance) |
| IP       | 10.101.64.19                 |

10.101.64.19:6379> get coordinator:window_end
"1780875288"
