# Development Dockerfile
FROM golang:1.26-alpine

RUN apk add --no-cache \
    ca-certificates \
    git

WORKDIR /app

COPY src/ingestor/go.mod src/ingestor/go.sum ./

RUN go mod download

COPY src/ingestor/ ./

RUN go build -o ingestor ./cmd/local

EXPOSE 8080
#ENV PUBSUB_EMULATOR_HOST=pubsub:8085

CMD ["./ingestor"]
