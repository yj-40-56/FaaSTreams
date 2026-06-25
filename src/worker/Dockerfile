FROM python:3.11-slim

WORKDIR /app

COPY src/worker/requirements.txt .
RUN pip install -r requirements.txt

RUN python -c "import duckdb; conn = duckdb.connect(); conn.execute('INSTALL spatial')"

COPY src/worker/ .

CMD ["functions-framework", "--target=handler", "--source=handler.py", "--port=8080"]