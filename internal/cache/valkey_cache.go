package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
)

// ValkeyCache реализует Cache
type ValkeyCache struct {
	client   valkey.Client
	localTTL time.Duration
}

// NewValkeyCache создаёт клиент, подключённый к указанным адресам
func NewValkeyCache(addrs []string, localTTL time.Duration) (*ValkeyCache, error) {
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: addrs,
	})
	if err != nil {
		return nil, fmt.Errorf("create valkey client: %w", err)
	}
	return &ValkeyCache{client: client, localTTL: localTTL}, nil
}

func (c *ValkeyCache) Get(ctx context.Context, key string) ([]byte, error) {
	result := c.client.DoCache(ctx,
		c.client.B().Get().Key(key).Cache(),
		c.localTTL,
	)
	if err := result.Error(); err != nil {
		if valkey.IsValkeyNil(err) {
			return nil, ErrCacheMiss
		}
		return nil, fmt.Errorf("valkey get %q: %w", key, err)
	}
	data, err := result.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("valkey get bytes %q: %w", key, err)
	}
	return data, nil
}

func (c *ValkeyCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	cmd := c.client.B().Set().Key(key).Value(string(value)).Ex(ttl).Build()
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("valkey set %q: %w", key, err)
	}
	return nil
}

func (c *ValkeyCache) Delete(ctx context.Context, keys ...string) error {
	cmd := c.client.B().Del().Key(keys...).Build()
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("valkey del %v: %w", keys, err)
	}
	return nil
}
func (c *ValkeyCache) Close() {
	c.client.Close()
}

// Key возвращает ключ кэша для списка ссылок чата
func Key(chatID int64) string {
	return fmt.Sprintf("links:%d", chatID)
}
