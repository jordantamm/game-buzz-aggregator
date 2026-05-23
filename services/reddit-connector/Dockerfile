FROM golang:1.23-alpine AS builder
WORKDIR /build

# Copy workspace and module files first for layer caching
COPY go.work go.work.sum* ./
COPY gen/go/go.mod gen/go/go.mod
COPY pkg/go.mod pkg/go.mod
COPY services/reddit-connector/go.mod services/reddit-connector/go.mod

# Fake go.sum files so workspace downloads work
RUN go work sync 2>/dev/null || true

# Copy source
COPY gen/go/ gen/go/
COPY pkg/ pkg/
COPY services/reddit-connector/ services/reddit-connector/

WORKDIR /build/services/reddit-connector
RUN go build -o /reddit-connector ./cmd/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /reddit-connector /reddit-connector
ENTRYPOINT ["/reddit-connector"]
