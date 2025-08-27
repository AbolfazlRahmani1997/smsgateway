# ---- builder ----
FROM golang:1.23 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/server ./cmd/server

# ---- runtime ----
FROM alpine:3.20
WORKDIR /app
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/server /app/server
COPY config/config.json /app/config/config.json

ENV PORT=8080 \
    DATABASE_URL="root:password@tcp(mysql-mm:3306)/mm?charset=utf8mb4&parseTime=True&loc=Local" \
    CLIENT_MANAGER_GRPC_ADDR=client-manager:9091 \
    KAFKA_BROKERS=redpanda:9092 \
    TOPIC_NORMAL=sms.normal.v1 TOPIC_PRIORITY=sms.otp.v1 \
    TOPIC_STATUS=sms.status.v1 GROUP_STATUS=message-manager-status \
    PRICE_NORMAL=1 PRICE_PRIORITY=2

EXPOSE 8080
ENTRYPOINT ["/app/server"]
