//go:build integration

package cache_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/cache"
)

func startValkey(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "valkey/valkey:7",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start valkey container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("get port: %v", err)
	}
	return fmt.Sprintf("%s:%s", host, port.Port())
}

func TestValkeyCache_SetAndGet(t *testing.T) {
	addr := startValkey(t)
	c, err := cache.NewValkeyCache([]string{addr}, 30*time.Second)
	if err != nil {
		t.Fatalf("create cache: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	key := "test-key"
	value := []byte(`{"links":[],"size":0}`)

	if err = c.Set(ctx, key, value, time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(value) {
		t.Errorf("want %q, got %q", value, got)
	}
}

func TestValkeyCache_Miss(t *testing.T) {
	addr := startValkey(t)
	c, err := cache.NewValkeyCache([]string{addr}, 30*time.Second)
	if err != nil {
		t.Fatalf("create cache: %v", err)
	}
	defer c.Close()

	_, err = c.Get(context.Background(), "nonexistent-key")
	if !errors.Is(err, cache.ErrCacheMiss) {
		t.Errorf("want ErrCacheMiss, got %v", err)
	}
}

func TestValkeyCache_Delete(t *testing.T) {
	addr := startValkey(t)
	c, err := cache.NewValkeyCache([]string{addr}, 30*time.Second)
	if err != nil {
		t.Fatalf("create cache: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	key := cache.Key(42)

	_ = c.Set(ctx, key, []byte(`{"links":[],"size":0}`), time.Minute)

	if err = c.Delete(ctx, key); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err = c.Get(ctx, key)
	if !errors.Is(err, cache.ErrCacheMiss) {
		t.Errorf("want ErrCacheMiss after delete, got %v", err)
	}
}

func TestValkeyCache_TTLExpiry(t *testing.T) {
	addr := startValkey(t)
	c, err := cache.NewValkeyCache([]string{addr}, 30*time.Second)
	if err != nil {
		t.Fatalf("create cache: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	key := "ttl-key"

	_ = c.Set(ctx, key, []byte("data"), 500*time.Millisecond)

	time.Sleep(700 * time.Millisecond)

	_, err = c.Get(ctx, key)
	if !errors.Is(err, cache.ErrCacheMiss) {
		t.Errorf("want ErrCacheMiss after TTL, got %v", err)
	}
}
