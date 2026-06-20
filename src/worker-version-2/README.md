# for local test:
cd /src/worker-version-2
# create one time: python3 -m venv .venv
# activate:
source .venv/bin/activate
pip install -r requirements.txt

# then:
docker run -d --name faastreams-redis -p 6379:6379 redis:7

# test
python test_local.py