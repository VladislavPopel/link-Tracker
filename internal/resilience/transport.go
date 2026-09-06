package resilience

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	retry "github.com/avast/retry-go/v4"
	"github.com/sony/gobreaker/v2"
)

// RetryConfig задает параметры повторных попыток
type RetryConfig struct {
	MaxAttempts uint
	Delay       time.Duration
}

// CircuitBreakerConfig задает параметры circuit breaker
type CircuitBreakerConfig struct {
	Name        string
	MaxRequests uint32        // кол-во пробных запросов в HALF-OPEN
	Interval    time.Duration // интервал сброса счетчика
	Timeout     time.Duration // время пребывания в OPEN перед переходом в HALF-OPEN

	ReadyToTrip func(counts gobreaker.Counts) bool // вызывается при каждой ошибке
}

type RoundTripperFunc func(*http.Request) (*http.Response, error)

func (f RoundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// isRetryable возвращает true, если ответ означает временный сбой.
func isRetryable(resp *http.Response, err error) bool {
	if err != nil {
		return true
	}
	return resp != nil && resp.StatusCode >= http.StatusInternalServerError
}

// WithRetry оборачивает транспорт логикой retry с constant backoff
func WithRetry(next http.RoundTripper, cfg RetryConfig) http.RoundTripper {
	return RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		var (
			resp    *http.Response
			lastErr error
		)

		err := retry.Do(
			func() error {
				var doErr error
				//nolint:bodyclose // body закрывается ниже при 5xx или возвращается caller'у при успехе
				resp, doErr = next.RoundTrip(req)
				if !isRetryable(resp, doErr) {
					if doErr != nil {
						return retry.Unrecoverable(doErr)
					}
					return nil
				}
				if doErr != nil {
					return fmt.Errorf("round trip failed: %w", doErr)
				}

				if resp != nil && resp.Body != nil {
					_ = resp.Body.Close()
				}
				lastErr = fmt.Errorf("server error: status %d", resp.StatusCode)
				return lastErr
			},
			retry.Attempts(cfg.MaxAttempts),
			retry.Delay(cfg.Delay),
			retry.DelayType(retry.FixedDelay), // constant backoff
			retry.LastErrorOnly(true),
			retry.Context(req.Context()),
		)

		if err != nil {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			if lastErr != nil {
				return nil, fmt.Errorf("retry failed: %w", lastErr)
			}
			return nil, fmt.Errorf("retry failed: %w", err)
		}
		return resp, nil
	})
}

// WithCircuitBreaker оборачивает транспорт circuit breakerом
func WithCircuitBreaker(next http.RoundTripper, cfg CircuitBreakerConfig) http.RoundTripper {
	settings := gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: cfg.MaxRequests,
		Interval:    cfg.Interval,
		Timeout:     cfg.Timeout,
		ReadyToTrip: cfg.ReadyToTrip,
	}

	//nolint:bodyclose // body закрывается ниже при 5xx или возвращается caller'у при успехе
	cb := gobreaker.NewCircuitBreaker[*http.Response](settings)

	return RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := cb.Execute(func() (*http.Response, error) {
			resp, doErr := next.RoundTrip(req)
			if doErr != nil {
				return nil, fmt.Errorf("round trip failed: %w", doErr)
			}
			if resp.StatusCode >= http.StatusInternalServerError {
				_ = resp.Body.Close()
				return nil, fmt.Errorf("server error: status %d", resp.StatusCode)
			}
			return resp, nil
		})

		if err != nil {
			if errors.Is(err, gobreaker.ErrOpenState) {
				return nil, fmt.Errorf("circuit breaker open: %w", err)
			}
			if errors.Is(err, gobreaker.ErrTooManyRequests) {
				return nil, fmt.Errorf("circuit breaker half-open limit: %w", err)
			}
			return nil, fmt.Errorf("circuit breaker execution failed: %w", err)
		}
		return resp, nil
	})
}

// NewResilientTransport создает цепочку: Retry - CircuitBreaker - base
func NewResilientTransport(
	base http.RoundTripper,
	retryCfg RetryConfig,
	cbCfg CircuitBreakerConfig,
) http.RoundTripper {
	withCB := WithCircuitBreaker(base, cbCfg)
	return WithRetry(withCB, retryCfg)
}

// NewHTTPClient создает *http.Client с timeout, retry и circuit breaker.
func NewHTTPClient(
	timeout time.Duration,
	retryCfg RetryConfig,
	cbCfg CircuitBreakerConfig,
) *http.Client {
	base := &http.Transport{}
	transport := NewResilientTransport(base, retryCfg, cbCfg)
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}
}

// DefaultReadyToTrip - стандартная функция перехода в OPEN
func DefaultReadyToTrip(failureRatio float64, minRequests uint32) func(gobreaker.Counts) bool {
	return func(counts gobreaker.Counts) bool {
		total := counts.TotalSuccesses + counts.TotalFailures
		if total < minRequests {
			return false
		}
		ratio := float64(counts.TotalFailures) / float64(total)
		return ratio >= failureRatio
	}
}

// WithTimeout добавляет deadline к запросу, если у контекста еще нет deadline
// Используется как middleware поверх любого транспорта
func WithTimeout(next http.RoundTripper, timeout time.Duration) http.RoundTripper {
	return RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if _, ok := req.Context().Deadline(); ok {
			return next.RoundTrip(req)
		}
		ctx, cancel := context.WithTimeout(req.Context(), timeout)
		defer cancel()
		return next.RoundTrip(req.WithContext(ctx))
	})
}
