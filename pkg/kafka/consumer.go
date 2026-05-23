package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Consumer wraps a franz-go client for consuming with OTel trace extraction.
type Consumer struct {
	client *kgo.Client
}

// NewConsumer creates a consumer joined to the given group.
func NewConsumer(brokers []string, group, topic string, opts ...kgo.Opt) (*Consumer, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	}
	client, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Consumer{client: client}, nil
}

// Poll fetches the next batch of records.
func (c *Consumer) Poll(ctx context.Context) kgo.Fetches {
	return c.client.PollFetches(ctx)
}

// CommitOffsets commits processed offsets back to the broker.
func (c *Consumer) CommitOffsets(ctx context.Context) error {
	return c.client.CommitUncommittedOffsets(ctx)
}

// Close shuts down the consumer.
func (c *Consumer) Close() {
	c.client.Close()
}

// ExtractSpanContext pulls the W3C traceparent from record headers.
func ExtractSpanContext(ctx context.Context, rec *kgo.Record) (context.Context, trace.Span) {
	carrier := make(propagation.MapCarrier)
	for _, h := range rec.Headers {
		carrier[h.Key] = string(h.Value)
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	tracer := otel.Tracer("kafka-consumer")
	return tracer.Start(ctx, "kafka.consume")
}
