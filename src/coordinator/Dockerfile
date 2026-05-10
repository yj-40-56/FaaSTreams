# Development Dockerfile
FROM golang:1.26-alpine

RUN apk add --no-cache \
    ca-certificates \
    git

WORKDIR /app

COPY src/coordinator/go.mod src/coordinator/go.sum ./

RUN go mod download

COPY src/coordinator/*.go ./

EXPOSE 8000
ENV PUBSUB_EMULATOR_HOST=pubsub:8085

CMD ["go", "run", "."]

