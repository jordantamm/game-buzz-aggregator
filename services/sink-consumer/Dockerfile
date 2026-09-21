FROM golang:1.23-alpine AS builder
WORKDIR /build

COPY go.work.sum* ./
COPY gen/go/go.mod gen/go/go.mod
COPY pkg/go.mod pkg/go.mod
COPY services/sink-consumer/go.mod services/sink-consumer/go.mod

RUN go work init ./gen/go ./pkg ./services/sink-consumer

COPY gen/go/ gen/go/
COPY pkg/ pkg/
COPY services/sink-consumer/ services/sink-consumer/

WORKDIR /build/services/sink-consumer
RUN go build -o /sink-consumer ./cmd/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /sink-consumer /sink-consumer
ENTRYPOINT ["/sink-consumer"]
