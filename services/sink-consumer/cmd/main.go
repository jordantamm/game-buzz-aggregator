package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	gkafka "github.com/jordantamm/game-buzz-aggregator/pkg/kafka"
	"github.com/jordantamm/game-buzz-aggregator/pkg/pg"
	"github.com/jordantamm/game-buzz-aggregator/pkg/telemetry"
	"github.com/jordantamm/game-buzz-aggregator/services/sink-consumer/internal/consumer"
	"github.com/jordantamm/game-buzz-aggregator/services/sink-consumer/internal/sink"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	viper.AutomaticEnv()
	viper.SetDefault("REDPANDA_BROKERS", "localhost:9092")
	viper.SetDefault("TOPIC_MENTIONS_ENRICHED", "mentions.enriched")
	viper.SetDefault("TOPIC_MENTIONS_DLQ", "mentions.dlq")
	viper.SetDefault("POSTGRES_DSN", "postgres://gba:gba_dev_password@localhost:5432/gba?sslmode=disable")
	viper.SetDefault("SINK_BATCH_SIZE", 100)
	viper.SetDefault("SINK_FLUSH_INTERVAL_MS", 500)
	viper.SetDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")

	log, _ := zap.NewProduction()
	defer log.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	shutdownOTel, err := telemetry.Init(ctx, "sink-consumer", viper.GetString("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if err != nil {
		log.Warn("otel init failed", zap.Error(err))
	} else {
		defer func() {
			ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel2()
			_ = shutdownOTel(ctx2)
		}()
	}

	pool, err := pg.Connect(ctx, viper.GetString("POSTGRES_DSN"))
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()

	brokers := strings.Split(viper.GetString("REDPANDA_BROKERS"), ",")

	cons, err := gkafka.NewConsumer(
		brokers,
		"sink-v1",
		viper.GetString("TOPIC_MENTIONS_ENRICHED"),
	)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer cons.Close()

	dlqProd, err := gkafka.NewProducer(brokers)
	if err != nil {
		return fmt.Errorf("dlq producer: %w", err)
	}
	defer dlqProd.Close()

	s := sink.New(pool, log)
	c := consumer.New(
		cons, s,
		viper.GetInt("SINK_BATCH_SIZE"),
		time.Duration(viper.GetInt("SINK_FLUSH_INTERVAL_MS"))*time.Millisecond,
		viper.GetString("TOPIC_MENTIONS_DLQ"),
		dlqProd,
		log,
	)

	log.Info("sink-consumer started")
	return c.Run(ctx)
}
