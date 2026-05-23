package publisher

import (
	"context"
	"fmt"

	gkafka "github.com/jordantamm/game-buzz-aggregator/pkg/kafka"
	"google.golang.org/protobuf/proto"
)

// Publisher serializes proto messages and publishes them to a topic.
type Publisher struct {
	producer *gkafka.Producer
	topic    string
}

func New(producer *gkafka.Producer, topic string) *Publisher {
	return &Publisher{producer: producer, topic: topic}
}

func (p *Publisher) Publish(ctx context.Context, key string, msg proto.Message) error {
	b, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal proto: %w", err)
	}
	return p.producer.Produce(ctx, p.topic, []byte(key), b)
}
