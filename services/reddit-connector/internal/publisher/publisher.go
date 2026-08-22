package publisher

import (
	"context"
	"fmt"

	gkafka "github.com/jordantamm/game-buzz-aggregator/pkg/kafka"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Publisher serializes proto messages and publishes them to a topic.
type Publisher struct {
	producer *gkafka.Producer
	topic    string
	log      *zap.Logger
}

func New(producer *gkafka.Producer, topic string, log *zap.Logger) *Publisher {
	return &Publisher{producer: producer, topic: topic, log: log}
}

func (p *Publisher) Publish(ctx context.Context, key string, msg proto.Message) error {
	b, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal proto: %w", err)
	}

	if err := p.producer.Produce(ctx, p.topic, []byte(key), b); err != nil {
		return err
	}

	// Debug, not Info: this fires once per mention, and at production ingest
	// rates an Info line per message drowns out everything worth reading.
	p.log.Debug("published mention",
		zap.String("topic", p.topic),
		zap.String("key", key),
		zap.Int("bytes", len(b)),
	)
	return nil
}
