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
# برای HTTPS نیاز داری:
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/server /app/server
COPY config/config.json /app/config/config.json

ENV GRPC_PORT=9091 \
    DATABASE_URL="root:password@tcp(mysql-cm:3306)/cm?charset=utf8mb4&parseTime=True&loc=Local" \
    SEED_DEMO=1 DEMO_CLIENT_ID=demo DEMO_BALANCE=1000 DEMO_NORMAL_PRICE=1 DEMO_PRIORITY_PRICE=2

EXPOSE 9091
ENTRYPOINT ["/app/server"]
