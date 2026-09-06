package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent"
)

func main() {
	if err := run(); err != nil {
		slog.Error("agent failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "configs/agent.yaml", "path to agent config file")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	cfg, err := agent.LoadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	slog.Info("agent config loaded",
		"input_topic", cfg.Kafka.InputTopic,
		"output_topic", cfg.Kafka.OutputTopic,
		"min_length", cfg.Filtering.MinLength,
		"summarization_threshold", cfg.Summarization.Threshold,
	)

	// собираем зависимости
	filter := agent.NewFilter(cfg.Filtering)
	summarizer := agent.NewSummarizer(cfg.Summarization)

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Kafka.Brokers...),
		Topic:        cfg.Kafka.OutputTopic,
		Balancer:     &kafka.RoundRobin{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil {
			slog.Error("kafka writer close error", "err", closeErr)
		}
	}()

	processor := agent.NewProcessor(filter, summarizer, writer, cfg.Summarization.Threshold)
	consumer := agent.NewConsumer(cfg.Kafka, processor)
	defer func() {
		if closeErr := consumer.Close(); closeErr != nil {
			slog.Error("kafka consumer close error", "err", closeErr)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	slog.Info("AI Agent started",
		"brokers", cfg.Kafka.Brokers,
		"ai_enabled", cfg.Summarization.AIEnabled,
	)

	if err = consumer.Run(ctx); err != nil {
		return fmt.Errorf("consumer run: %w", err)
	}

	return nil
}
