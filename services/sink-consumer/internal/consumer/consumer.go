package consumer

import (
	"context"
	"fmt"
	"time"

	gbav1 "github.com/jordantamm/game-buzz-aggregator/gen/go/gba/v1"
	gkafka "github.com/jordantamm/game-buzz-aggregator/pkg/kafka"
	"github.com/jordantamm/game-buzz-aggregator/pkg/sink"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Consumer reads from mentions.enriched and writes batches to Postgres.
type Consumer struct {
	kafka         *gkafka.Consumer
	sink          *sink.Sink
	batchSize     int
	flushInterval time.Duration
	log           *zap.Logger
	dlqTopic      string
	dlqProducer   *gkafka.Producer
}

func New(
	kafka *gkafka.Consumer,
	sink *sink.Sink,
	batchSize int,
	flushInterval time.Duration,
	dlqTopic string,
	dlqProducer *gkafka.Producer,
	log *zap.Logger,
) *Consumer {
	return &Consumer{
		kafka:         kafka,
		sink:          sink,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		dlqTopic:      dlqTopic,
		dlqProducer:   dlqProducer,
		log:           log,
	}
}

// Run consumes until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	batch := make([]*gbav1.EnrichedMention, 0, c.batchSize)
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ctx2, span := otel.Tracer("sink-consumer").Start(ctx, "flush")
		defer span.End()
		if err := c.sink.WriteBatch(ctx2, batch); err != nil {
			return fmt.Errorf("write batch: %w", err)
		}
		if err := c.kafka.CommitOffsets(ctx2); err != nil {
			c.log.Warn("offset commit failed", zap.Error(err))
		}
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return flush()
		case <-ticker.C:
			if err := flush(); err != nil {
				c.log.Error("flush error", zap.Error(err))
			}
		default:
		}

		fetches := c.kafka.Poll(ctx)
		if fetches.IsClientClosed() {
			return nil
		}

		fetches.EachError(func(_ string, _ int32, err error) {
			c.log.Error("fetch error", zap.Error(err))
		})

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()

			var em gbav1.EnrichedMention
			if err := proto.Unmarshal(rec.Value, &em); err != nil {
				c.log.Error("unmarshal failed, sending to DLQ",
					zap.String("topic", rec.Topic),
					zap.Error(err),
				)
				// Never discard a produce error silently: if the DLQ write
				// also fails, this payload is gone for good and that must be
				// loud in the logs.
				if dlqErr := c.dlqProducer.Produce(ctx, c.dlqTopic, rec.Key, rec.Value); dlqErr != nil {
					c.log.Error("DLQ produce failed; malformed payload is unrecoverable",
						zap.String("dlq_topic", c.dlqTopic),
						zap.Error(dlqErr),
					)
				}
				continue
			}
			batch = append(batch, &em)

			if len(batch) >= c.batchSize {
				if err := flush(); err != nil {
					c.log.Error("flush error", zap.Error(err))
				}
			}
		}
	}
}
