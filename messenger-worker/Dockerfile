# ---- builder ----
FROM golang:1.23 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/worker ./cmd/worker

# ---- runtime ----
FROM alpine:3.20
WORKDIR /app
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/worker /app/worker
COPY config/config.json /app/config/config.json

ENV KAFKA_BROKERS=redpanda:9092 \
    WORKER_TOPIC=sms.normal.v1 WORKER_GROUP=masanger-normal \
    TOPIC_STATUS=sms.status.v1 \
    OPERATOR=mock WORKER_NAME=w1 \
    ACCEPT_LATENCY_MS=50 DELIVERY_MIN_MS=300 DELIVERY_MAX_MS=1500 FAIL_RATIO_PCT=10

ENTRYPOINT ["/app/worker"]
