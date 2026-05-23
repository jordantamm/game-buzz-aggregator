package state

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const keyPrefix = "reddit:cursor:"

// Cursor persists the last-seen Reddit fullname per subreddit in Redis.
type Cursor struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Cursor {
	return &Cursor{rdb: rdb}
}

func (c *Cursor) Get(ctx context.Context, subreddit string) (string, error) {
	val, err := c.rdb.Get(ctx, keyPrefix+subreddit).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get cursor for %s: %w", subreddit, err)
	}
	return val, nil
}

func (c *Cursor) Set(ctx context.Context, subreddit, after string) error {
	if err := c.rdb.Set(ctx, keyPrefix+subreddit, after, 0).Err(); err != nil {
		return fmt.Errorf("set cursor for %s: %w", subreddit, err)
	}
	return nil
}
