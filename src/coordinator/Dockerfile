# Development Dockerfile
FROM golang:1.26-alpine

RUN apk add --no-cache \
    ca-certificates \
    git

WORKDIR /app

COPY src/coordinator/go.mod src/coordinator/go.sum ./

RUN go mod download

COPY src/coordinator/ ./

RUN go build -o coordinator ./cmd/local

EXPOSE 8080
#ENV PUBSUB_EMULATOR_HOST=pubsub:8085

CMD ["./coordinator"]

