package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// NotificationTransport определяет способ отправки уведомлений scrapper - bot
type NotificationTransport string

const (
	TransportKafka NotificationTransport = "kafka"
	TransportHTTP  NotificationTransport = "http"
)

// KafkaConfig содержит настройки подключения к кластеру Kafka
type KafkaConfig struct {
	// Brokers - список брокеров через запятую, например: kafka1:9092,kafka2:9094,kafka3:9095
	Brokers []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:9092"`

	// Topic - топик для отправки уведомлений об обновлениях ссылок
	Topic string `env:"KAFKA_TOPIC" envDefault:"link-updates"`

	// DLQTopic - топик DLQ для сообщений, которые не удалось обработать
	DLQTopic string `env:"KAFKA_DLQ_TOPIC" envDefault:"link-updates-dlq"`

	// GroupID - consumer group ID
	GroupID string `env:"KAFKA_GROUP_ID" envDefault:"bot-consumer"`

	// MaxRetries - максимальное количество повторных попыток обработки сообщения перед отправкой в DLQ
	MaxRetries int `env:"KAFKA_MAX_RETRIES" envDefault:"3"`
}

type BotConfig struct {
	TelegramToken  string `env:"TELEGRAM_APITOKEN,required"`
	UpdatesTimeout int    `env:"UPDATES_TIMEOUT"           envDefault:"60"`
	Debug          bool   `env:"BOT_DEBUG"                 envDefault:"false"`
	ScrapperURL    string `env:"SCRAPPER_URL" envDefault:"http://localhost:8081"`
	BotListenAddr  string `env:"BOT_LISTEN_ADDR" envDefault:":8080"`

	Kafka KafkaConfig

	Resilience ResilienceConfig
}

type ScrapperConfig struct {
	ListenAddr          string `env:"SCRAPPER_LISTEN_ADDR" envDefault:":8081"`
	BotURL              string `env:"BOT_URL" envDefault:"http://localhost:8080"`
	ScheduleIntervalSec int    `env:"SCRAPPER_SCHEDULE_INTERVAL_SEC" envDefault:"60"`

	GitHubToken         string `env:"GITHUB_TOKEN"`
	StackOverflowAPIKey string `env:"STACKOVERFLOW_API_KEY"`

	// База данных
	DatabaseDSN string `env:"DATABASE_DSN,required"` // 5432
	// Способ доступа к БД: SQL или ORM
	DatabaseAccessType string `env:"DATABASE_ACCESS_TYPE" envDefault:"SQL"`

	BatchSize int `env:"SCRAPPER_BATCH_SIZE" envDefault:"100"`

	WorkerCount int `env:"SCRAPPER_WORKER_COUNT" envDefault:"4"`

	// Kafka
	NotificationTransport NotificationTransport `env:"NOTIFICATION_TRANSPORT" envDefault:"kafka"`

	// Kafka - настройки для продюсера уведомлений
	Kafka KafkaConfig

	Valkey ValkeyConfig

	Resilience ResilienceConfig
}

// ValkeyConfig содержит настройки подключения к кластеру Valkey
type ValkeyConfig struct {
	Addrs    []string      `env:"VALKEY_ADDRS" envSeparator:"," envDefault:"localhost:6379"` // список адресов через запятую
	TTL      time.Duration `env:"VALKEY_TTL"   envDefault:"5m"`
	LocalTTL time.Duration `env:"VALKEY_LOCAL_TTL" envDefault:"30s"`
}

// LoadBot загружает конфигурацию Bot из переменных окружения
func LoadBot() (*BotConfig, error) {
	cfg := &BotConfig{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parse bot env config: %w", err)
	}
	/*if cfg.TelegramToken == "" {
		return nil, errors.New("TELEGRAM_APITOKEN is empty")
	}*/
	if cfg.UpdatesTimeout <= 0 {
		return nil, fmt.Errorf("UPDATES_TIMEOUT must be positive, got %d", cfg.UpdatesTimeout)
	}
	return cfg, nil
}

// LoadScrapper загружает конфигурацию Scrapper
func LoadScrapper() (*ScrapperConfig, error) {
	cfg := &ScrapperConfig{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parse scrapper env config: %w", err)
	}
	if cfg.ScheduleIntervalSec <= 0 {
		return nil, fmt.Errorf("SCRAPPER_SCHEDULE_INTERVAL_SEC must be positive, got %d", cfg.ScheduleIntervalSec)
	}
	if cfg.BatchSize < 50 || cfg.BatchSize > 500 {
		return nil, fmt.Errorf("SCRAPPER_BATCH_SIZE must be between 50 and 500, got %d", cfg.BatchSize)
	}
	if cfg.WorkerCount < 1 {
		return nil, fmt.Errorf("SCRAPPER_WORKER_COUNT must be >= 1, got %d", cfg.WorkerCount)
	}
	if cfg.NotificationTransport != TransportKafka && cfg.NotificationTransport != TransportHTTP {
		return nil, fmt.Errorf("NOTIFICATION_TRANSPORT must be 'kafka' or 'http', got %q", cfg.NotificationTransport)
	}
	return cfg, nil
}
