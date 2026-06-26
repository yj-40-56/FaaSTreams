FROM python:3.12-slim

WORKDIR /workspace

COPY requirements.txt .
RUN pip install -r requirements.txt

# Pre-install DuckDB spatial extension at image build time so Cloud Run
# instances never need to download it at request time.
RUN python -c "\
import duckdb; \
conn = duckdb.connect(); \
conn.execute(\"SET extension_directory='/tmp'\"); \
conn.execute(\"INSTALL spatial\"); \
conn.close()"

COPY . .

ENV PORT=8080
CMD exec functions-framework --target=handler --port=$PORT
