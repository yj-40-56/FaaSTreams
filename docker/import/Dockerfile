FROM python:3.12-slim
RUN pip install --no-cache-dir redis
WORKDIR /app
COPY docker/import/import.py .
CMD ["python", "import.py"]
