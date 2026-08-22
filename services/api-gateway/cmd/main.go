package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jordantamm/game-buzz-aggregator/pkg/embed"
	"github.com/jordantamm/game-buzz-aggregator/pkg/pg"
	"github.com/jordantamm/game-buzz-aggregator/pkg/telemetry"
	"github.com/jordantamm/game-buzz-aggregator/services/api-gateway/internal/handler"
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
	viper.SetDefault("POSTGRES_DSN", "postgres://gba:gba_dev_password@localhost:5432/gba?sslmode=disable")
	viper.SetDefault("API_GATEWAY_PORT", "8080")
	viper.SetDefault("EMBEDDER_URL", "http://enricher:8000")
	viper.SetDefault("EMBEDDER_TIMEOUT_MS", 5000)
	viper.SetDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")

	log, _ := zap.NewProduction()
	defer log.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	shutdownOTel, err := telemetry.Init(ctx, "api-gateway", viper.GetString("OTEL_EXPORTER_OTLP_ENDPOINT"))
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

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Optional: if the embedder is unreachable, /v1/search degrades to
	// keyword-only retrieval instead of erroring.
	embedder := embed.NewClient(
		viper.GetString("EMBEDDER_URL"),
		time.Duration(viper.GetInt("EMBEDDER_TIMEOUT_MS"))*time.Millisecond,
	)

	h := handler.New(pool, embedder, log)
	h.Routes(r)

	addr := ":" + viper.GetString("API_GATEWAY_PORT")
	srv := &http.Server{Addr: addr, Handler: r}

	go func() {
		<-ctx.Done()
		ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel2()
		_ = srv.Shutdown(ctx2)
	}()

	log.Info("api-gateway listening", zap.String("addr", addr))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
