module github.com/jordantamm/game-buzz-aggregator/services/api-gateway

go 1.23

require (
	github.com/jordantamm/game-buzz-aggregator/pkg v0.0.0-00010101000000-000000000000
	github.com/go-chi/chi/v5 v5.1.0
	github.com/jackc/pgx/v5 v5.7.1
	github.com/spf13/viper v1.19.0
	go.opentelemetry.io/otel v1.31.0
	go.opentelemetry.io/otel/trace v1.31.0
	go.uber.org/zap v1.27.0
)

replace github.com/jordantamm/game-buzz-aggregator/pkg => ../../pkg
