package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/cache"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/db"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/resilience"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper"
)

const (
	shutdownTimeout = 5 * time.Second
	outboxInterval  = 5 * time.Second
	outboxBatch     = 100
)

func main() {
	if err := run(); err != nil {
		slog.Error("scrapper failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	cfg, err := config.LoadScrapper()
	if err != nil {
		return fmt.Errorf("load scrapper config: %w", err)
	}
	// База данных
	linkRepo, sqlDB, err := initRepository(cfg) // возвращаем sqlDB
	if err != nil {
		return err
	}

	sched, sender, err := initScheduler(cfg, linkRepo, sqlDB)
	if err != nil {
		return err
	}

	// Создаём outbox-процессор и передаём ему тот же sender
	outboxRepo := repository.NewOutboxRepository(sqlDB)
	outboxProc := scrapper.NewOutboxProcessor(
		outboxRepo,
		sender,
		outboxInterval, // интервал поллинга
		outboxBatch,    // размер батча
	)

	valkeyCache, err := cache.NewValkeyCache(cfg.Valkey.Addrs, cfg.Valkey.LocalTTL)
	if err != nil {
		slog.Warn("valkey unavailable, caching disabled", "err", err)
	} else {
		slog.Info("valkey cache enabled",
			"addrs", cfg.Valkey.Addrs,
			"ttl", cfg.Valkey.TTL,
			"local_ttl", cfg.Valkey.LocalTTL,
		)
		defer valkeyCache.Close()
	}

	return startServer(cfg, linkRepo, sched, sender, outboxProc, valkeyCache)
}

func initRepository(cfg *config.ScrapperConfig) (domain.LinkRepository, *sql.DB, error) {
	sqlDB, err := db.Open(cfg.DatabaseDSN)
	if err != nil {
		return nil, nil, fmt.Errorf("open db: %w", err)
	}
	if err = db.Ping(context.Background(), sqlDB); err != nil {
		return nil, nil, fmt.Errorf("ping db: %w", err)
	}
	// Применяем миграции
	if err = db.RunMigrations(sqlDB); err != nil {
		return nil, nil, fmt.Errorf("run migrations: %w", err)
	}
	slog.Info("migrations applied")

	repo, err := repository.New(sqlDB, cfg.DatabaseDSN, repository.AccessType(cfg.DatabaseAccessType))
	if err != nil {
		return nil, nil, fmt.Errorf("create repository: %w", err)
	}
	slog.Info("repository initialized", "access-type", cfg.DatabaseAccessType)
	return repo, sqlDB, nil
}

func initScheduler(cfg *config.ScrapperConfig, repo domain.LinkRepository, sqlDB *sql.DB) (*scrapper.Scheduler, scrapper.MessageSender, error) {
	externalHTTPClient := resilience.FromConfig(cfg.Resilience, "external-api")

	checkers := []scrapper.LinkChecker{
		scrapper.NewGitHubCheckerWithHTTP(cfg.GitHubToken, externalHTTPClient),
		scrapper.NewStackOverflowCheckerWithHTTP(cfg.StackOverflowAPIKey, externalHTTPClient),
	}

	// если транспорт HTTP - оборачиваем в NewFallbackMessageSender
	var sender scrapper.MessageSender
	switch cfg.NotificationTransport {
	case config.TransportHTTP:
		sender = scrapper.NewFallbackMessageSender(cfg, externalHTTPClient)
		slog.Info("notification transport: http with kafka fallback")
	case config.TransportKafka:
		sender = scrapper.NewKafkaMessageSender(cfg.Kafka.Brokers, cfg.Kafka.Topic)
		slog.Info("notification transport: kafka", "transport", cfg.NotificationTransport)
	default:
		sender = scrapper.NewKafkaMessageSender(cfg.Kafka.Brokers, cfg.Kafka.Topic)
		slog.Info("notification transport: kafka", "transport", cfg.NotificationTransport)
	}
	outboxRepo := repository.NewOutboxRepository(sqlDB)

	interval := time.Duration(cfg.ScheduleIntervalSec) * time.Second

	sched, err := scrapper.NewScheduler(
		outboxRepo,
		repo,
		checkers,
		sender,
		interval,
		cfg.BatchSize,
		cfg.WorkerCount,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create scheduler: %w", err)
	}
	return sched, sender, nil
}

func startServer(
	cfg *config.ScrapperConfig,
	repo domain.LinkRepository,
	sched *scrapper.Scheduler,
	sender scrapper.MessageSender,
	outboxProc *scrapper.OutboxProcessor,
	valkeyCache cache.Cache,
) error {
	var handler *scrapper.Handler
	if valkeyCache != nil {
		handler = scrapper.NewHandlerWithCache(repo, valkeyCache, cfg.Valkey.TTL)
	} else {
		handler = scrapper.NewHandler(repo)
	}

	rateLimiter := resilience.NewIPRateLimiter(cfg.Resilience.RateLimitRPS, cfg.Resilience.RateLimitBurst)
	rateLimitMW := resilience.RateLimitMiddleware(rateLimiter)

	httpServer := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: rateLimitMW(handler.Router()),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Запускаем outbox-процессор
	go outboxProc.Run(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.Info("received signal, shutting down", "signal", sig)
		cancel()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer shutdownCancel()

		if shutdownErr := httpServer.Shutdown(shutdownCtx); shutdownErr != nil {
			slog.Error("http server shutdown error", "err", shutdownErr)
		}
		if stopErr := sched.Stop(); stopErr != nil {
			slog.Error("scheduler stop error", "err", stopErr)
		}
		if ks, ok := sender.(*scrapper.KafkaMessageSender); ok {
			if closeErr := ks.Close(); closeErr != nil {
				slog.Error("kafka sender close error", "err", closeErr)
			}
		}
	}()

	sched.Start()
	slog.Info("scrapper listening",
		"addr", cfg.ListenAddr,
		"transport", cfg.NotificationTransport,
		"rate_limit_rps", cfg.Resilience.RateLimitRPS,
	)

	if listenErr := httpServer.ListenAndServe(); !errors.Is(listenErr, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", listenErr)
	}

	<-ctx.Done()
	return nil
}
