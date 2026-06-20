# Worker Version 2 Status
Fetch logic, etc. is copied from /worker.

- Map domain specific record field names to general field names --> done and working
- Implement 8 spatial, 8 temporal, 8 spatio-temporal queries --> to be done ( so far only 3 )


### for local test:
cd /src/worker-version-2

### (for mac:) create one time: python3 -m venv .venv
### activate:

source .venv/bin/activate
pip install -r requirements.txt

### then:
docker run -d --name faastreams-redis -p 6379:6379 redis:7

### test
python test_local.py