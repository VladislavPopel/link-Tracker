package cache

import (
	"context"
	"errors"
	"time"
)

var ErrCacheMiss = errors.New("cache miss")

// Cache - абстракция над распределённым кэшем
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	Close()
}
