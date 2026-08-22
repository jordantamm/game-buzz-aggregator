// Package archive stores raw Reddit API payloads in MinIO (S3).
//
// The Kafka message carries only the normalized `Mention` plus an object key.
// Keeping the untouched upstream JSON out of the event stream keeps messages
// small, and keeping it *somewhere* means the pipeline can be replayed against
// a changed normalization without re-fetching from Reddit — which is impossible
// after the fact, since /new only serves a rolling window.
package archive

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

// Archiver writes raw payloads to an object store.
type Archiver struct {
	client *minio.Client
	bucket string
	log    *zap.Logger
}

// Config holds MinIO connection settings.
type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
}

// New connects to MinIO and ensures the bucket exists.
func New(ctx context.Context, cfg Config, log *zap.Logger) (*Archiver, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}

	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket %q: %w", cfg.Bucket, err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("create bucket %q: %w", cfg.Bucket, err)
		}
		log.Info("created minio bucket", zap.String("bucket", cfg.Bucket))
	}

	return &Archiver{client: client, bucket: cfg.Bucket, log: log}, nil
}

// Key builds the object key for a payload: reddit/{yyyy}/{mm}/{dd}/{native_id}.json
//
// Date-prefixed so a day's raw data can be listed or expired as a unit.
func Key(createdAt time.Time, nativeID string) string {
	utc := createdAt.UTC()
	return fmt.Sprintf("reddit/%04d/%02d/%02d/%s.json",
		utc.Year(), utc.Month(), utc.Day(), nativeID)
}

// Put stores a raw payload and returns its key.
func (a *Archiver) Put(ctx context.Context, key string, payload []byte) (string, error) {
	ctx, span := otel.Tracer("reddit-connector").Start(ctx, "archive.put")
	defer span.End()
	span.SetAttributes(
		attribute.String("archive.bucket", a.bucket),
		attribute.String("archive.key", key),
		attribute.Int("archive.bytes", len(payload)),
	)

	_, err := a.client.PutObject(ctx, a.bucket, key,
		bytes.NewReader(payload), int64(len(payload)),
		minio.PutObjectOptions{ContentType: "application/json"},
	)
	if err != nil {
		return "", fmt.Errorf("put object %q: %w", key, err)
	}
	return key, nil
}
