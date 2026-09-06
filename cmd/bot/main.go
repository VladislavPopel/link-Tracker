package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/client"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/resilience"
)

const (
	shutdownTimeout = 5 * time.Second
)

func main() {
	if err := run(); err != nil {
		slog.Error("bot failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.LoadBot() // BotConfig
	if err != nil {
		slog.Error("failed to load config", "err", err)
		return fmt.Errorf("load bot config: %w", err)
	}

	tgClient, err := client.NewTelegramClient(cfg.TelegramToken, cfg.UpdatesTimeout, cfg.Debug)
	if err != nil {
		slog.Error("failed to create telegram client", "err", err)
		return fmt.Errorf("create telegram client: %w", err)
	}

	commander, err := initCommander(cfg, tgClient)
	if err != nil {
		slog.Error("failed to init commander", "err", err)
		return fmt.Errorf("init commander: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer := startKafkaConsumer(ctx, cfg, tgClient)
	defer func() {
		if closeErr := consumer.Close(); closeErr != nil {
			slog.Error("kafka consumer close error", "err", closeErr)
		}
	}()

	httpServer := initHTTPServer(cfg, tgClient)

	go func() {
		slog.Info("bot update server listening", "addr", cfg.BotListenAddr)
		if serveErr := httpServer.ListenAndServe(); !errors.Is(serveErr, http.ErrServerClosed) {
			slog.Error("bot http server error", "err", serveErr)
		}
	}()
	go handleShutdown(ctx, cancel, httpServer)
	/*sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		slog.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer shutdownCancel()
		if shutdownErr := httpServer.Shutdown(shutdownCtx); shutdownErr != nil {
			slog.Error("http server shutdown error", "err", shutdownErr)
		}
	}()*/

	slog.Info("bot started", "username", tgClient.Username())
	updates := tgClient.Updates(ctx)

	for {
		select {
		case <-ctx.Done():
			slog.Info("context cancelled, stopping bot")
			return nil
		case update, ok := <-updates:
			if !ok {
				return nil
			}
			commander.Handle(ctx, update)
		}
	}
}

func initCommander(cfg *config.BotConfig, tgClient *client.TelegramClient) (*application.Commander, error) {
	resilientClient := buildResilientClient(cfg.Resilience, "scrapper")
	scrapperClient := client.NewScrapperHTTPClientWithHTTP(cfg.ScrapperURL, resilientClient)
	userRepo := infrastructure.NewInMemoryUserRepo()

	commander, err := application.NewCommander(tgClient, userRepo, scrapperClient)
	if err != nil {
		slog.Error("failed to create commander", "err", err)
		return nil, fmt.Errorf("create commander: %w", err)
	}
	if err = commander.Init(); err != nil {
		return nil, fmt.Errorf("init commander: %w", err)
	}
	return commander, nil
}

func initHTTPServer(cfg *config.BotConfig, sender bot.MessageSender) *http.Server {
	updateServer := bot.NewUpdateServer(sender)

	limiter := resilience.NewIPRateLimiter(cfg.Resilience.RateLimitRPS, cfg.Resilience.RateLimitBurst)

	return &http.Server{
		Addr:    cfg.BotListenAddr,
		Handler: resilience.RateLimitMiddleware(limiter)(updateServer.Handler()),
	}
}

func handleShutdown(ctx context.Context, cancel context.CancelFunc, httpServer *http.Server) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		slog.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	if shutdownErr := httpServer.Shutdown(shutdownCtx); shutdownErr != nil {
		slog.Error("http server shutdown error", "err", shutdownErr)
	}
}

func startKafkaConsumer(ctx context.Context, cfg *config.BotConfig, tgClient *client.TelegramClient) *bot.LinkUpdateConsumer {
	consumer := bot.NewLinkUpdateConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topic,
		cfg.Kafka.GroupID,
		cfg.Kafka.DLQTopic,
		cfg.Kafka.MaxRetries,
		tgClient,
	)

	go func() {
		if runErr := consumer.Run(ctx); runErr != nil {
			// Run возвращает nil при штатной отмене контекста
			// Ненулевая ошибка = фатальный сбой - завершаем весь процесс
			slog.Error("kafka consumer fatal error", "err", runErr)
		}
	}()

	return consumer
}

func buildResilientClient(cfg config.ResilienceConfig, cbName string) *http.Client {
	return resilience.NewHTTPClient(
		cfg.HTTPTimeout,
		resilience.RetryConfig{
			MaxAttempts: cfg.RetryMaxAttempts,
			Delay:       cfg.RetryDelay,
		},
		resilience.CircuitBreakerConfig{
			Name:        cbName,
			MaxRequests: cfg.CBMaxRequests,
			Interval:    cfg.CBInterval,
			Timeout:     cfg.CBTimeout,
			ReadyToTrip: resilience.DefaultReadyToTrip(cfg.CBFailureRatio, cfg.CBMinRequests),
		},
	)
}
