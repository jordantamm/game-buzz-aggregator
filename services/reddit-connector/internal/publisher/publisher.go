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

	p.log.Info("publishing message to redpanda",
		zap.String("topic", p.topic),
		zap.String("key", key),
		zap.Int("bytes", len(b)),
	)

	if err := p.producer.Produce(ctx, p.topic, []byte(key), b); err != nil {
		return err
	}

	p.log.Info("message published successfully",
		zap.String("topic", p.topic),
		zap.String("key", key),
	)
	return nil
}
