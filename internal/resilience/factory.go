package resilience

import (
	"net/http"
	"time"

	"github.com/sony/gobreaker/v2"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
)

const (
	defaultHTTPTimeout = 10 * time.Second
)

// FromConfig создает *http.Client с timeout, retry и circuit breaker
func FromConfig(cfg config.ResilienceConfig, cbName string) *http.Client {
	retryCfg := RetryConfig{
		MaxAttempts: cfg.RetryMaxAttempts,
		Delay:       cfg.RetryDelay,
	}

	cbCfg := CircuitBreakerConfig{
		Name:        cbName,
		MaxRequests: cfg.CBMaxRequests,
		Interval:    cfg.CBInterval,
		Timeout:     cfg.CBTimeout,
		ReadyToTrip: DefaultReadyToTrip(cfg.CBFailureRatio, cfg.CBMinRequests),
	}

	base := &http.Transport{}

	withCB := WithCircuitBreaker(base, cbCfg)
	withRetry := WithRetry(withCB, retryCfg)
	withTimeout := WithTimeout(withRetry, cfg.HTTPTimeout)

	return &http.Client{
		Timeout:   cfg.HTTPTimeout,
		Transport: withTimeout,
	}
}

// MustFromConfig аналогичен FromConfig, паникует при нулевом конфиге
func MustFromConfig(cfg config.ResilienceConfig, cbName string) *http.Client {
	if cfg.HTTPTimeout == 0 {
		cfg.HTTPTimeout = defaultHTTPTimeout
	}
	return FromConfig(cfg, cbName)
}

// NewCircuitBreakerFromConfig создаёт gobreaker.CircuitBreaker из ResilienceConfig
func NewCircuitBreakerFromConfig[T any](cfg config.ResilienceConfig, name string) *gobreaker.CircuitBreaker[T] {
	settings := gobreaker.Settings{
		Name:        name,
		MaxRequests: cfg.CBMaxRequests,
		Interval:    cfg.CBInterval,
		Timeout:     cfg.CBTimeout,
		ReadyToTrip: DefaultReadyToTrip(cfg.CBFailureRatio, cfg.CBMinRequests),
	}
	return gobreaker.NewCircuitBreaker[T](settings)
}
