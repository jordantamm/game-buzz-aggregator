package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jordantamm/game-buzz-aggregator/pkg/kafka"
	"github.com/jordantamm/game-buzz-aggregator/pkg/telemetry"
	"github.com/jordantamm/game-buzz-aggregator/services/reddit-connector/internal/publisher"
	"github.com/jordantamm/game-buzz-aggregator/services/reddit-connector/internal/ratelimit"
	"github.com/jordantamm/game-buzz-aggregator/services/reddit-connector/internal/reddit"
	"github.com/jordantamm/game-buzz-aggregator/services/reddit-connector/internal/state"
	"github.com/redis/go-redis/v9"
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
	mock := flag.Bool("mock", false, "Use synthetic Reddit data instead of real API")
	flag.Parse()

	// Config
	viper.AutomaticEnv()
	viper.SetDefault("REDDIT_SUBREDDITS", "games,gaming,patientgamers,pcgaming,IndieGaming")
	viper.SetDefault("REDDIT_POLL_INTERVAL_SECONDS", 30)
	viper.SetDefault("REDDIT_POLL_MAX_INTERVAL_SECONDS", 300)
	viper.SetDefault("REDPANDA_BROKERS", "localhost:19092")
	viper.SetDefault("TOPIC_MENTIONS_RAW", "mentions.raw")
	viper.SetDefault("REDIS_ADDR", "localhost:6379")
	viper.SetDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")

	// Logger
	log, _ := zap.NewProduction()
	defer log.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// OTel
	shutdownOTel, err := telemetry.Init(ctx, "reddit-connector", viper.GetString("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if err != nil {
		log.Warn("otel init failed, traces disabled", zap.Error(err))
	} else {
		defer func() {
			ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel2()
			_ = shutdownOTel(ctx2)
		}()
	}

	// Redis
	rdb := redis.NewClient(&redis.Options{Addr: viper.GetString("REDIS_ADDR")})
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}

	// Kafka producer
	brokers := strings.Split(viper.GetString("REDPANDA_BROKERS"), ",")
	prod, err := kafka.NewProducer(brokers)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer prod.Close()
	log.Info("kafka producer created (connection is lazy — established on first produce)",
		zap.Strings("brokers", brokers),
		zap.String("topic", viper.GetString("TOPIC_MENTIONS_RAW")),
	)

	pub := publisher.New(prod, viper.GetString("TOPIC_MENTIONS_RAW"), log)
	cursor := state.New(rdb)
	bucket := ratelimit.New(rdb)

	// Fetcher: real or mock
	var fetcher reddit.Fetcher
	if *mock {
		log.Info("running in mock mode — synthetic Reddit data")
		fetcher = &reddit.MockClient{}
	} else {
		fetcher = reddit.NewClient(
			viper.GetString("REDDIT_CLIENT_ID"),
			viper.GetString("REDDIT_CLIENT_SECRET"),
			viper.GetString("REDDIT_USER_AGENT"),
		)
	}

	subreddits := strings.Split(viper.GetString("REDDIT_SUBREDDITS"), ",")
	baseInterval := time.Duration(viper.GetInt("REDDIT_POLL_INTERVAL_SECONDS")) * time.Second
	maxInterval := time.Duration(viper.GetInt("REDDIT_POLL_MAX_INTERVAL_SECONDS")) * time.Second

	errCh := make(chan error, len(subreddits))
	for _, sub := range subreddits {
		sub := strings.TrimSpace(sub)
		p := reddit.NewPoller(sub, baseInterval, maxInterval, fetcher, cursor, bucket, pub, log)
		go func() {
			errCh <- p.Run(ctx)
		}()
	}

	log.Info("reddit-connector test started", zap.Strings("subreddits", subreddits), zap.Bool("mock", *mock))

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-errCh:
		return err
	}
	return nil
}
