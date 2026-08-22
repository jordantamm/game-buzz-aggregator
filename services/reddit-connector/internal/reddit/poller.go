package reddit

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	gbav1 "github.com/jordantamm/game-buzz-aggregator/gen/go/gba/v1"
	"github.com/jordantamm/game-buzz-aggregator/services/reddit-connector/internal/archive"
	"github.com/jordantamm/game-buzz-aggregator/services/reddit-connector/internal/publisher"
	"github.com/jordantamm/game-buzz-aggregator/services/reddit-connector/internal/ratelimit"
	"github.com/jordantamm/game-buzz-aggregator/services/reddit-connector/internal/state"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// backoffFactor is applied to the poll interval after an empty or failed poll.
const backoffFactor = 1.5

// Fetcher abstracts the Reddit API so both real and mock clients satisfy it.
type Fetcher interface {
	FetchNew(ctx context.Context, subreddit, after string) ([]postData, string, error)
}

// Archiver stores raw payloads. Optional: nil disables archival.
type Archiver interface {
	Put(ctx context.Context, key string, payload []byte) (string, error)
}

// Poller runs one subreddit polling loop.
type Poller struct {
	subreddit    string
	baseInterval time.Duration
	maxInterval  time.Duration
	fetcher      Fetcher
	cursor       *state.Cursor
	bucket       *ratelimit.Bucket
	pub          *publisher.Publisher
	archiver     Archiver
	log          *zap.Logger
}

func NewPoller(
	subreddit string,
	baseInterval, maxInterval time.Duration,
	fetcher Fetcher,
	cursor *state.Cursor,
	bucket *ratelimit.Bucket,
	pub *publisher.Publisher,
	archiver Archiver,
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
		archiver:     archiver,
		log:          log.With(zap.String("subreddit", subreddit)),
	}
}

// Run loops until ctx is cancelled.
//
// Adaptive cadence: an empty poll multiplies the interval by backoffFactor up
// to maxInterval, and any non-empty poll resets it to base. A quiet subreddit
// therefore stops burning the shared Reddit rate-limit budget that a busy one
// needs.
func (p *Poller) Run(ctx context.Context) error {
	interval := p.baseInterval

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}

		if err := p.bucket.Take(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("rate limiter: %w", err)
		}

		count, err := p.poll(ctx)
		switch {
		case err != nil:
			p.log.Warn("poll failed", zap.Error(err), zap.Duration("next_interval", interval))
			interval = backoff(interval, p.maxInterval)
		case count == 0:
			interval = backoff(interval, p.maxInterval)
		default:
			interval = p.baseInterval
		}
	}
}

func backoff(current, max time.Duration) time.Duration {
	next := time.Duration(float64(current) * backoffFactor)
	if next > max {
		return max
	}
	return next
}

func (p *Poller) poll(ctx context.Context) (int, error) {
	ctx, span := otel.Tracer("reddit-connector").Start(ctx, "reddit.poll")
	defer span.End()
	span.SetAttributes(attribute.String("reddit.subreddit", p.subreddit))

	after, err := p.cursor.Get(ctx, p.subreddit)
	if err != nil {
		// A lost cursor means re-reading the current page, which the
		// deterministic mention_id makes harmless. Better than halting.
		p.log.Warn("cursor read failed; polling from the head", zap.Error(err))
		after = ""
	}

	posts, nextAfter, err := p.fetcher.FetchNew(ctx, p.subreddit, after)
	if err != nil {
		span.RecordError(err)
		return 0, err
	}
	span.SetAttributes(attribute.Int("reddit.posts_fetched", len(posts)))

	published := 0
	for _, post := range posts {
		if err := p.publishPost(ctx, post); err != nil {
			// One bad post must not abort the page; the cursor still advances
			// and the deterministic ID makes a later replay safe.
			p.log.Error("publish failed",
				zap.String("native_id", post.Name), zap.Error(err))
			continue
		}
		published++
	}

	span.SetAttributes(attribute.Int("reddit.posts_published", published))
	p.log.Info("poll complete",
		zap.Int("fetched", len(posts)),
		zap.Int("published", published),
	)

	if nextAfter != "" {
		if err := p.cursor.Set(ctx, p.subreddit, nextAfter); err != nil {
			p.log.Warn("cursor update failed", zap.Error(err))
		}
	}
	return published, nil
}

func (p *Poller) publishPost(ctx context.Context, post postData) error {
	ctx, span := otel.Tracer("reddit-connector").Start(ctx, "reddit.publish_post")
	defer span.End()

	mentionID := DeterministicULID("reddit", post.Name)
	createdAt := time.Unix(int64(post.CreatedUTC), 0).UTC()
	span.SetAttributes(
		attribute.String("mention.id", mentionID),
		attribute.String("reddit.native_id", post.Name),
	)

	// Archive the raw payload first, so the Kafka event can carry its key.
	// Archival failure is logged but non-fatal: losing the raw copy is worse
	// than losing nothing, but far better than dropping the mention entirely.
	rawRef := ""
	if p.archiver != nil {
		if payload, err := json.Marshal(post); err != nil {
			p.log.Warn("could not marshal raw payload", zap.Error(err))
		} else {
			key := archive.Key(createdAt, post.Name)
			if stored, err := p.archiver.Put(ctx, key, payload); err != nil {
				p.log.Warn("raw payload archival failed",
					zap.String("key", key), zap.Error(err))
			} else {
				rawRef = stored
			}
		}
	}

	mention := &gbav1.Mention{
		MentionId:  mentionID,
		Source:     gbav1.Source_SOURCE_REDDIT,
		NativeId:   post.Name,
		AuthorHash: sha256hex(post.Author),
		Text:       post.text(),
		CreatedAt:  timestamppb.New(createdAt),
		Engagement: &gbav1.Engagement{
			Score:      post.Score,
			ReplyCount: post.NumComments,
		},
		RawPayloadRef: rawRef,
		ParentId:      post.ParentID,
		// Carry the trace across the async Kafka boundary in the payload as
		// well as in headers. Headers are the transport-level mechanism; this
		// field survives replay from the topic and re-publication by any future
		// connector, which is what the cross-service trace contract relies on.
		TraceId: traceparent(span.SpanContext()),
		SourceMetadata: map[string]string{
			"subreddit": p.subreddit,
			"kind":      post.kind(),
		},
	}

	return p.pub.Publish(ctx, fmt.Sprintf("reddit:%s", p.subreddit), mention)
}

// DeterministicULID derives a ULID purely from (source, native_id).
//
// Both halves of the ULID come from the hash — the 48-bit timestamp prefix as
// well as the 80-bit entropy. Seeding the timestamp from time.Now() instead
// (the obvious implementation) produces a different ID for the same post on
// every run, which silently breaks the end-to-end idempotency story: the whole
// point is that reprocessing the same Reddit post upserts the same row rather
// than inserting a duplicate.
//
// Consequence to be aware of: the timestamp prefix is not a real time, so
// ULIDs do not sort chronologically here. Nothing depends on that — ordering
// comes from the created_at column.
func DeterministicULID(source, nativeID string) string {
	sum := sha256.Sum256([]byte(source + "|" + nativeID))

	// ULID timestamps are 48 bits.
	ms := binary.BigEndian.Uint64(append([]byte{0, 0}, sum[0:6]...))

	var entropy [10]byte
	copy(entropy[:], sum[6:16])

	id := ulid.ULID{}
	if err := id.SetTime(ms); err != nil {
		// Only possible if ms exceeds the 48-bit max, which the mask prevents.
		ms &= (1 << 48) - 1
		_ = id.SetTime(ms)
	}
	_ = id.SetEntropy(entropy[:])
	return id.String()
}

// traceparent renders a span context as a W3C traceparent header value.
func traceparent(sc trace.SpanContext) string {
	if !sc.IsValid() {
		return ""
	}
	return fmt.Sprintf("00-%s-%s-%02x", sc.TraceID(), sc.SpanID(), sc.TraceFlags())
}

func sha256hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
