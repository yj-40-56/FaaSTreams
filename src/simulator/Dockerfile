FROM golang:1.26-alpine

WORKDIR /app

COPY src/simulator/go.mod src/simulator/go.sum ./
RUN go mod download

COPY src/simulator/ .

RUN go build -o simulator .

CMD ["./simulator"]