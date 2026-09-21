FROM golang:1.23-alpine AS builder
WORKDIR /build

COPY go.work.sum* ./
COPY gen/go/go.mod gen/go/go.mod
COPY pkg/go.mod pkg/go.mod
COPY services/mcp-server/go.mod services/mcp-server/go.mod

RUN go work init ./gen/go ./pkg ./services/mcp-server

COPY gen/go/ gen/go/
COPY pkg/ pkg/
COPY services/mcp-server/ services/mcp-server/

WORKDIR /build/services/mcp-server
RUN go build -o /mcp-server ./cmd/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /mcp-server /mcp-server
EXPOSE 8081
ENTRYPOINT ["/mcp-server", "--http"]
