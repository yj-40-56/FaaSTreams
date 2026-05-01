# FaaSTreams Local Coordinator Start

This project starts multiple local coordinator containers using Go.  
Each container runs the `faastreams_coordinator` image and connects to Redis via a shared Docker network.

---

## Requirements

- Docker Desktop running
- Go installed

---

## Project Structure

- FaaSTreams/
    - localCoordinator/
        - faasTreamsCoordinator
        - startingPosition


---

## 1. Build Coordinator Image

The Docker image must be built from the `faastreams_coordinator` folder.

```bash
cd localCoordinator/faastreams_coordinator
docker build -t faastreams_coordinator .