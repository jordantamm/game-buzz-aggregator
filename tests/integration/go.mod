module github.com/jordantamm/game-buzz-aggregator/tests/integration

go 1.23

require (
	github.com/jordantamm/game-buzz-aggregator/gen/go v0.0.0-00010101000000-000000000000
	github.com/jackc/pgx/v5 v5.7.1
	github.com/testcontainers/testcontainers-go v0.35.0
	github.com/testcontainers/testcontainers-go/modules/postgres v0.35.0
	github.com/testcontainers/testcontainers-go/modules/redpanda v0.35.0
	github.com/twmb/franz-go v1.18.0
	google.golang.org/protobuf v1.35.1
)

replace github.com/jordantamm/game-buzz-aggregator/gen/go => ../../gen/go
