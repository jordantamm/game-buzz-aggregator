package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	tokenKey    = "reddit:tokens"
	maxTokens   = 100
	refillPer   = time.Minute
)

// Bucket is a Redis-backed token bucket for Reddit's 100 req/min quota.
// Shared across all connector replicas.
type Bucket struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Bucket {
	return &Bucket{rdb: rdb}
}

// Take blocks until a token is available, then consumes one.
func (b *Bucket) Take(ctx context.Context) error {
	script := redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local refill_ms = tonumber(ARGV[3])

local data = redis.call("HMGET", key, "tokens", "last_refill")
local tokens = tonumber(data[1]) or max
local last = tonumber(data[2]) or now

local elapsed = now - last
local add = math.floor(elapsed / refill_ms * max)
tokens = math.min(max, tokens + add)

if tokens < 1 then
  return -1
end

tokens = tokens - 1
redis.call("HMSET", key, "tokens", tokens, "last_refill", now)
redis.call("PEXPIRE", key, refill_ms * 2)
return tokens
`)

	for {
		nowMs := time.Now().UnixMilli()
		result, err := script.Run(ctx, b.rdb,
			[]string{tokenKey},
			nowMs, maxTokens, refillPer.Milliseconds(),
		).Int()
		if err != nil {
			return fmt.Errorf("rate limit check: %w", err)
		}
		if result >= 0 {
			return nil
		}
		// No tokens — wait a bit and retry.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}
