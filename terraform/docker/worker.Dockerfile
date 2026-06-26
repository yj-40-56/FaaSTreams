FROM python:3.12-slim

WORKDIR /workspace

COPY requirements.txt .
RUN pip install -r requirements.txt

# Pre-install DuckDB spatial to a stable image path (not /tmp, which Cloud Run
# mounts as an empty tmpfs at startup, wiping anything written during build).
RUN python -c "
import duckdb
conn = duckdb.connect()
conn.execute(\"SET extension_directory='/duckdb_ext'\")
conn.execute(\"INSTALL spatial\")
conn.close()
"

COPY . .

ENV PORT=8080

# Copy the pre-installed extension into /tmp at container startup so that
# analytics.py's `SET extension_directory='/tmp'` + `INSTALL spatial` finds it
# already there and skips the download (which is what causes the SIGABRT crash).
CMD cp -r /duckdb_ext/. /tmp/ && exec functions-framework --target=handler --port=$PORT
