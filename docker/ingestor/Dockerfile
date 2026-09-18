# Development Dockerfile. Runs the same package the deployed Cloud Function runs,
# under the Go functions framework, with the local entry-point wrapper copied in.
FROM golang:1.26-alpine

RUN apk add --no-cache ca-certificates git

WORKDIR /app

COPY src/ingestor/go.mod src/ingestor/go.sum ./
RUN go mod download

COPY src/ingestor/ ./
COPY docker/ingestor/localmain/main.go ./cmd/local/main.go

# The checked-in go.sum covers only what the deployed function imports; Google's
# buildpack supplies the funcframework server itself. Resolve its sums here so
# the committed go.mod/go.sum stay byte-identical to what Terraform deploys.
RUN go mod tidy

RUN go build -o ingestor ./cmd/local

EXPOSE 8080

CMD ["./ingestor"]
