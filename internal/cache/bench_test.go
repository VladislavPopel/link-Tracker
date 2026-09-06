//go:build integration

package cache_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/cache"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper"
)

// fakeRepo возвращает фиксированный список ссылок - симулирует медленную БД
type fakeRepo struct {
	delay time.Duration
	links []*domain.Link
}

func (f *fakeRepo) GetLinks(_ context.Context, _ int64, _ domain.Page) ([]*domain.Link, error) {
	time.Sleep(f.delay)
	return f.links, nil
}

func (f *fakeRepo) AddChat(_ context.Context, _ int64) error                        { return nil }
func (f *fakeRepo) RemoveChat(_ context.Context, _ int64) error                     { return nil }
func (f *fakeRepo) AddLink(_ context.Context, l *domain.Link) (*domain.Link, error) { return l, nil }
func (f *fakeRepo) RemoveLink(_ context.Context, _ int64, _ string) (*domain.Link, error) {
	return &domain.Link{}, nil
}
func (f *fakeRepo) GetAllLinks(_ context.Context, _ domain.Page) ([]*domain.Link, error) {
	return nil, nil
}

func makeLinks(n int) []*domain.Link {
	links := make([]*domain.Link, n)
	for i := range links {
		links[i] = &domain.Link{
			ID:  int64(i),
			URL: fmt.Sprintf("https://github.com/user/repo-%d", i),
		}
	}
	return links
}

// BenchmarkGetLinks_NoCache - запросы напрямую в БД
func BenchmarkGetLinks_NoCache(b *testing.B) {
	repo := &fakeRepo{delay: 5 * time.Millisecond, links: makeLinks(100)}
	handler := scrapper.NewHandler(repo)
	srv := httptest.NewServer(handler.Router())
	defer srv.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := http.Get(srv.URL + "/links?tgChatId=1")
			if err != nil {
				b.Error(err)
				continue
			}
			_ = resp.Body.Close()
		}
	})
}

// BenchmarkGetLinks_WithCache - запросы через кэш
func BenchmarkGetLinks_WithCache(b *testing.B) {
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
		b.Fatalf("start valkey: %v", err)
	}
	defer container.Terminate(ctx)

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "6379")
	addr := fmt.Sprintf("%s:%s", host, port.Port())

	c, err := cache.NewValkeyCache([]string{addr}, 30*time.Second)
	if err != nil {
		b.Fatalf("create cache: %v", err)
	}
	defer c.Close()

	repo := &fakeRepo{delay: 5 * time.Millisecond, links: makeLinks(100)}
	handler := scrapper.NewHandlerWithCache(repo, c, time.Minute)
	srv := httptest.NewServer(handler.Router())
	defer srv.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, linksErr := http.Get(srv.URL + "/links?tgChatId=1")
			if linksErr != nil {
				b.Error(linksErr)
				continue
			}
			_ = resp.Body.Close()
		}
	})
}
