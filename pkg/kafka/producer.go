package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Producer wraps a franz-go client with OTel trace injection.
type Producer struct {
	client *kgo.Client
}

// NewProducer creates a new producer connected to the given brokers.
func NewProducer(brokers []string, opts ...kgo.Opt) (*Producer, error) {
	base := []kgo.Opt{kgo.SeedBrokers(brokers...)}
	client, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &Producer{client: client}, nil
}

// Produce sends a record, injecting the current span's trace context into headers.
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) error {
	headers := make([]kgo.RecordHeader, 0, 3)
	headers = append(headers, kgo.RecordHeader{Key: "content-type", Value: []byte("application/protobuf")})

	// Inject W3C traceparent into record headers.
	carrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for k, v := range carrier {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}

	rec := &kgo.Record{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: headers,
	}

	results := p.client.ProduceSync(ctx, rec)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}
	return nil
}

// Close shuts down the producer gracefully.
func (p *Producer) Close() {
	p.client.Close()
}
