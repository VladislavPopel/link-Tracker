package config

import "time"

// ResilienceConfig содержит настройки паттернов надежности
type ResilienceConfig struct {
	HTTPTimeout time.Duration `env:"HTTP_TIMEOUT" envDefault:"10s"`

	RetryMaxAttempts uint          `env:"RETRY_MAX_ATTEMPTS" envDefault:"3"`
	RetryDelay       time.Duration `env:"RETRY_DELAY"        envDefault:"500ms"`

	CBMaxRequests  uint32        `env:"CB_MAX_REQUESTS"   envDefault:"5"`
	CBInterval     time.Duration `env:"CB_INTERVAL"       envDefault:"60s"` // интервал сброса статистики
	CBTimeout      time.Duration `env:"CB_TIMEOUT"        envDefault:"30s"`
	CBFailureRatio float64       `env:"CB_FAILURE_RATIO"  envDefault:"0.6"`
	CBMinRequests  uint32        `env:"CB_MIN_REQUESTS"   envDefault:"5"` // минимальный счетчик для подсчета ratio

	// Rate Limiter
	RateLimitRPS   float64 `env:"RATE_LIMIT_RPS"   envDefault:"10"` // запросов в секунду с одного IP
	RateLimitBurst int     `env:"RATE_LIMIT_BURST" envDefault:"20"` // burst-размер
}
