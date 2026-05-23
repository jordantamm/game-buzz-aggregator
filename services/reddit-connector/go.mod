module github.com/jordantamm/game-buzz-aggregator/services/reddit-connector

go 1.23

require (
	github.com/jordantamm/game-buzz-aggregator/gen/go v0.0.0-00010101000000-000000000000
	github.com/jordantamm/game-buzz-aggregator/pkg v0.0.0-00010101000000-000000000000
	github.com/minio/minio-go/v7 v7.0.80
	github.com/oklog/ulid/v2 v2.1.0
	github.com/redis/go-redis/v9 v9.7.0
	github.com/spf13/viper v1.19.0
	github.com/twmb/franz-go v1.18.0
	go.opentelemetry.io/otel v1.31.0
	go.opentelemetry.io/otel/trace v1.31.0
	go.uber.org/zap v1.27.0
	google.golang.org/protobuf v1.35.1
)

replace (
	github.com/jordantamm/game-buzz-aggregator/gen/go => ../../gen/go
	github.com/jordantamm/game-buzz-aggregator/pkg => ../../pkg
)
