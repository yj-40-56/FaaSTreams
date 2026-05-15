FROM python:3.12-slim
WORKDIR /app
# Install system dependencies for DuckDB spatial
RUN apt-get update && apt-get install -y libgomp1 && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install functions-framework
COPY . .
EXPOSE 8080
CMD ["functions-framework", "--target=handler", "--source=handler.py", "--port=8080"]