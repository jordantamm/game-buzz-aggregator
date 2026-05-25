package reddit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"time"

	gbav1 "github.com/jordantamm/game-buzz-aggregator/gen/go/gba/v1"
	"github.com/jordantamm/game-buzz-aggregator/services/reddit-connector/internal/publisher"
	"github.com/jordantamm/game-buzz-aggregator/services/reddit-connector/internal/ratelimit"
	"github.com/jordantamm/game-buzz-aggregator/services/reddit-connector/internal/state"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Fetcher abstracts the Reddit API so both real and mock clients satisfy it.
type Fetcher interface {
	FetchNew(ctx context.Context, subreddit, after string) ([]postData, string, error)
}

// Poller runs one subreddit polling loop.
type Poller struct {
	subreddit   string
	baseInterval time.Duration
	maxInterval  time.Duration
	fetcher     Fetcher
	cursor      *state.Cursor
	bucket      *ratelimit.Bucket
	pub         *publisher.Publisher
	log         *zap.Logger
}

func NewPoller(
	subreddit string,
	baseInterval, maxInterval time.Duration,
	fetcher Fetcher,
	cursor *state.Cursor,
	bucket *ratelimit.Bucket,
	pub *publisher.Publisher,
	log *zap.Logger,
) *Poller {
	return &Poller{
		subreddit:    subreddit,
		baseInterval: baseInterval,
		maxInterval:  maxInterval,
		fetcher:      fetcher,
		cursor:       cursor,
		bucket:       bucket,
		pub:          pub,
		log:          log.With(zap.String("subreddit", subreddit)),
	}
}

// Run loops until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) error {
	interval := p.baseInterval

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}

		if err := p.bucket.Take(ctx); err != nil {
			return fmt.Errorf("rate limiter: %w", err)
		}

		count, err := p.poll(ctx)
		if err != nil {
			p.log.Warn("poll error", zap.Error(err))
			interval = min(time.Duration(float64(interval)*1.5), p.maxInterval)
			continue
		}

		if count == 0 {
			interval = min(time.Duration(float64(interval)*1.5), p.maxInterval)
		} else {
			interval = p.baseInterval
		}
	}
}

func (p *Poller) poll(ctx context.Context) (int, error) {
	ctx, span := otel.Tracer("reddit-poller").Start(ctx, "poll")
	defer span.End()
	span.SetAttributes(attribute.String("subreddit", p.subreddit))

	after, err := p.cursor.Get(ctx, p.subreddit)
	if err != nil {
		return 0, err
	}

	posts, nextAfter, err := p.fetcher.FetchNew(ctx, p.subreddit, after)
	if err != nil {
		return 0, err
	}

	p.log.Info("fetched posts from reddit", zap.Int("count", len(posts)), zap.String("after", after))

	published := 0
	for _, post := range posts {
		if err := p.publishPost(ctx, post); err != nil {
			p.log.Error("publish failed", zap.String("native_id", post.Name), zap.Error(err))
			continue
		}
		published++
	}
	p.log.Info("poll complete", zap.Int("fetched", len(posts)), zap.Int("published", published))

	if nextAfter != "" {
		if err := p.cursor.Set(ctx, p.subreddit, nextAfter); err != nil {
			p.log.Warn("cursor update failed", zap.Error(err))
		}
	}

	return published, nil
}

func (p *Poller) publishPost(ctx context.Context, post postData) error {
	mentionID := deterministicULID("reddit", post.Name)
	authorHash := sha256hex(post.Author)
	createdAt := time.Unix(int64(post.CreatedUTC), 0)

	mention := &gbav1.Mention{
		MentionId:  mentionID,
		Source:     gbav1.Source_SOURCE_REDDIT,
		NativeId:   post.Name,
		AuthorHash: authorHash,
		Text:       post.text(),
		CreatedAt:  timestamppb.New(createdAt),
		Engagement: &gbav1.Engagement{
			Score:       post.Score,
			ReplyCount:  post.NumComments,
		},
		ParentId: post.ParentID,
		SourceMetadata: map[string]string{
			"subreddit": p.subreddit,
		},
	}

	key := fmt.Sprintf("reddit:%s", p.subreddit)

	if jsonBytes, err := protojson.Marshal(mention); err == nil {
		p.log.Info("mention payload", zap.String("native_id", post.Name), zap.String("mention_id", mentionID), zap.String("key", key), zap.Any("mention", json.RawMessage(jsonBytes)))
	}

	return p.pub.Publish(ctx, key, mention)
}

func deterministicULID(source, nativeID string) string {
	h := sha256.Sum256([]byte(source + nativeID))
	// Use first 10 bytes as ULID entropy (deterministic).
	ms := ulid.Timestamp(time.Now())
	id, _ := ulid.New(ms, bytes.NewReader(h[:10]))
	return id.String()
}

func sha256hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// minFloat used for backoff cap
var _ = math.MaxFloat64
