FROM python:3.12-slim

WORKDIR /workspace

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

ENV PORT=8080

CMD exec functions-framework --target=handler --port=$PORT
