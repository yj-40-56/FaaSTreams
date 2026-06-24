# Development Dockerfile
FROM golang:1.26-alpine

RUN apk add --no-cache \
    ca-certificates \
    git

WORKDIR /app

COPY src/windower/go.mod src/windower/go.sum ./

RUN go mod download

COPY src/windower/ ./

RUN go build -o windower ./cmd/local

EXPOSE 8080

CMD ["./windower"]
