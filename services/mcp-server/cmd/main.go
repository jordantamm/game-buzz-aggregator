package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jordantamm/game-buzz-aggregator/pkg/embed"
	"github.com/jordantamm/game-buzz-aggregator/pkg/pg"
	"github.com/jordantamm/game-buzz-aggregator/pkg/telemetry"
	"github.com/jordantamm/game-buzz-aggregator/services/mcp-server/internal/tools"
	"github.com/mark3labs/mcp-go/mcp"
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
	// Must match the host:port clients actually dial, not where the process
	// happens to run: the SSE client rejects a message-endpoint URL whose
	// origin differs from the URL it connected to. Inside docker-compose
	// that's the service name ("mcp-server"), not "localhost".
	viper.SetDefault("MCP_SERVER_BASE_URL", "http://localhost:8081")
	viper.SetDefault("EMBEDDER_URL", "http://enricher:8000")
	viper.SetDefault("EMBEDDER_TIMEOUT_MS", 5000)
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

	// Log every tool call the server receives: the caller (analyst-agent, MCP
	// Inspector, Claude Desktop, ...), exact arguments, and either the result
	// payload or the error. This is the only place all three call sites funnel
	// through, so it is the one place to log to be sure nothing is missed.
	hooks := &server.Hooks{}
	hooks.AddBeforeCallTool(func(id any, msg *mcp.CallToolRequest) {
		args, _ := json.Marshal(msg.Params.Arguments)
		log.Info("tool call received",
			zap.Any("request_id", id),
			zap.String("tool", msg.Params.Name),
			zap.String("arguments", string(args)),
		)
	})
	hooks.AddOnSuccess(func(id any, method mcp.MCPMethod, msg any, result any) {
		if method != mcp.MethodToolsCall {
			return
		}
		res, _ := json.Marshal(result)
		log.Info("tool call succeeded",
			zap.Any("request_id", id),
			zap.String("result", truncate(string(res), 2000)),
		)
	})
	hooks.AddOnError(func(id any, method mcp.MCPMethod, msg any, err error) {
		if method != mcp.MethodToolsCall {
			return
		}
		log.Error("tool call failed",
			zap.Any("request_id", id),
			zap.Error(err),
		)
	})

	mcpServer := server.NewMCPServer(
		"game-buzz-aggregator",
		"0.1.0",
		server.WithToolCapabilities(true),
		server.WithHooks(hooks),
	)

	// The embedder is optional: if it is unreachable, search_mentions degrades
	// to keyword-only retrieval rather than failing outright.
	embedder := embed.NewClient(
		viper.GetString("EMBEDDER_URL"),
		time.Duration(viper.GetInt("EMBEDDER_TIMEOUT_MS"))*time.Millisecond,
	)

	tools.Register(mcpServer, tools.Deps{Pool: pool, Embedder: embedder})

	if *httpMode {
		addr := ":" + viper.GetString("MCP_SERVER_HTTP_PORT")
		baseURL := viper.GetString("MCP_SERVER_BASE_URL")
		log.Info("mcp-server SSE HTTP mode", zap.String("addr", addr), zap.String("base_url", baseURL))
		sse := server.NewSSEServer(mcpServer, server.WithBaseURL(baseURL))
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}
