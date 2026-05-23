package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jordantamm/game-buzz-aggregator/pkg/pg"
	"github.com/jordantamm/game-buzz-aggregator/pkg/telemetry"
	"github.com/jordantamm/game-buzz-aggregator/services/mcp-server/internal/tools"
	"github.com/mark3labs/mcp-go/server"
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
	httpMode := flag.Bool("http", false, "Run as SSE HTTP server instead of stdio")
	flag.Parse()

	viper.AutomaticEnv()
	viper.SetDefault("POSTGRES_DSN", "postgres://gba:gba_dev_password@localhost:5432/gba?sslmode=disable")
	viper.SetDefault("MCP_SERVER_HTTP_PORT", "8081")
	viper.SetDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")

	log, _ := zap.NewProduction()
	defer log.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	shutdownOTel, err := telemetry.Init(ctx, "mcp-server", viper.GetString("OTEL_EXPORTER_OTLP_ENDPOINT"))
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

	mcpServer := server.NewMCPServer(
		"game-buzz-aggregator",
		"0.1.0",
		server.WithToolCapabilities(true),
	)

	tools.Register(mcpServer, pool)

	if *httpMode {
		addr := ":" + viper.GetString("MCP_SERVER_HTTP_PORT")
		log.Info("mcp-server SSE HTTP mode", zap.String("addr", addr))
		sse := server.NewSSEServer(mcpServer, server.WithBaseURL("http://localhost"+addr))
		go func() {
			<-ctx.Done()
			shutCtx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel2()
			_ = sse.Shutdown(shutCtx)
		}()
		return sse.Start(addr)
	}

	log.Info("mcp-server stdio mode")
	return server.ServeStdio(mcpServer)
}
