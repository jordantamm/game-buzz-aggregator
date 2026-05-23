FROM golang:1.23-alpine AS builder
WORKDIR /build

COPY go.work go.work.sum* ./
COPY gen/go/go.mod gen/go/go.mod
COPY pkg/go.mod pkg/go.mod
COPY services/api-gateway/go.mod services/api-gateway/go.mod

RUN go work sync 2>/dev/null || true

COPY gen/go/ gen/go/
COPY pkg/ pkg/
COPY services/api-gateway/ services/api-gateway/

WORKDIR /build/services/api-gateway
RUN go build -o /api-gateway ./cmd/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /api-gateway /api-gateway
EXPOSE 8080
ENTRYPOINT ["/api-gateway"]
