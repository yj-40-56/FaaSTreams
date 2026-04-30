# (Advanced) Distributed Systems Prototyping - FaaS Data Stream Processing

## Overview

This repository contains the codebase for our FaaS (Function-as-a-Service) based data stream processing system.

In the final production architecture, live data will be pushed from external sources through a data queue and ingested into Redis, which serves as our temporary data storage. FaaS workers will then process this streaming data.

---

## Phase 1: Worker Development Setup

Currently, this repository contains the foundational infrastructure needed to begin developing the **worker side** of the application.

Since the live external data ingestion queue is not yet integrated, we have set up a mock infrastructure to simulate the data stream. This allows us to start writing and testing the FaaS workers immediately.

To enable this worker development, a Docker Compose stack is used to run a Redis instance pre-loaded with mock AIS (Automatic Identification System) vessel tracking data. On startup, a one-shot import container reads `data/ais.csv` (19,999 rows) and writes every record as a Redis hash, simulating the state of the database after data ingestion.

### Stack

| Service        | Role                                                                                                                                                                                                                                                  |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `redis`        | Redis server with persistence disabled (`--save "" --appendonly no`) for in-memory-only, fast access. Exposes port `6379`.                                                                                                                            |
| `redis-import` | Custom Python image (`docker/import/Dockerfile`) that reads `data/ais.csv` and imports each row as a hash under the key `ais:<index>` (e.g. `ais:0`, `ais:1`, …). Also sets `ais:total` to the total number of imported records. Runs once and exits. |

### Starting the Mock Infrastructure

```bash
# from the docker/ directory
cd docker

# start in detached mode — Redis starts first, import runs after Redis is healthy, then exits
docker compose up -d

# stop and remove containers
docker compose down
```

_Note: The import container exits on its own once the mock data is loaded. Redis keeps running with the data in memory, ready for your local FaaS workers to interact with it._

### Accessing Redis CLI

You can interact with the mock data to test queries your workers will need to perform:

```bash
# open an interactive Redis CLI session inside the running container
docker compose exec redis redis-cli
```

#### Useful commands

```text
# total number of imported records
GET ais:total

# get all fields of a single record
HGETALL ais:0

# get a specific field from a record
HGET ais:42 latitude

# count keys
DBSIZE
```

### Mock Data Fields

Each `ais:<index>` hash contains the following fields, representing the payload FaaS workers will process:

| Field                    | Description                          |
| ------------------------ | ------------------------------------ |
| `timestamp`              | UTC timestamp of the position report |
| `mmsi`                   | Maritime Mobile Service Identity     |
| `name`                   | Vessel name                          |
| `shipType`               | Type of ship                         |
| `latitude` / `longitude` | Position                             |
| `sog`                    | Speed over ground (knots)            |
| `cog`                    | Course over ground (degrees)         |
| `heading`                | True heading (degrees)               |
| `navigationalStatus`     | e.g. Under way, At anchor            |
| `imo`                    | IMO number                           |
| `callsign`               | Radio callsign                       |
| `destination`            | Reported destination port            |
| `eta`                    | Estimated time of arrival            |
| `draught`                | Current draught (metres)             |
| `length` / `width`       | Vessel dimensions (metres)           |
| `cargoType`              | Cargo category                       |
| `typeOfMobile`           | Class of AIS transponder             |
| `positionFixingDevice`   | GPS, GLONASS, etc.                   |
| `dataSourceType`         | AIS message source                   |
| `rot`                    | Rate of turn                         |
| `a` / `b` / `c` / `d`    | Antenna offset dimensions            |
