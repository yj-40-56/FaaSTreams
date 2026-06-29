FROM python:3.11-slim

WORKDIR /app

COPY src/data-sink/requirements.txt .
RUN pip install -r requirements.txt

COPY src/data-sink/ .

CMD ["functions-framework", "--target=handler", "--source=handler.py", "--port=8080"]
