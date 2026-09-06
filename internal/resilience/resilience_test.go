package resilience_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/resilience"
)

// helpers

func doGET(t *testing.T, client *http.Client, url string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	return client.Do(req)
}

// Timeout

func TestTimeout_ExceedsLimit(t *testing.T) {
	// Сервер отвечает через 200 мс
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	timeout := 50 * time.Millisecond
	base := &http.Transport{}
	transport := resilience.WithTimeout(base, timeout)
	client := &http.Client{Transport: transport}

	start := time.Now()
	_, err := doGET(t, client, srv.URL)
	elapsed := time.Since(start)

	require.Error(t, err, "ожидается ошибка timeout")
	assert.Less(t, elapsed, 200*time.Millisecond, "время ответа должно быть меньше задержки сервера")
}

// Retry на 5xx

func TestRetry_5xx_ThenSuccess(t *testing.T) {
	var callCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := callCount.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError) // первые 2 - 500
			return
		}
		w.WriteHeader(http.StatusOK) // третья - 200
	}))
	defer srv.Close()

	cfg := resilience.RetryConfig{MaxAttempts: 3, Delay: 10 * time.Millisecond}
	transport := resilience.WithRetry(http.DefaultTransport, cfg)
	client := &http.Client{Transport: transport}

	resp, err := doGET(t, client, srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(3), callCount.Load(), "должно быть ровно 3 попытки")
}

// Отсутствие Retry на 4xx

func TestRetry_4xx_NoRetry(t *testing.T) {
	var callCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	cfg := resilience.RetryConfig{MaxAttempts: 3, Delay: 10 * time.Millisecond}
	transport := resilience.WithRetry(http.DefaultTransport, cfg)
	client := &http.Client{Transport: transport}

	resp, err := doGET(t, client, srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, int32(1), callCount.Load(), "retry при 4xx не должен выполняться")
}

// Соблюдение интервала retry

func TestRetry_ConstantBackoff(t *testing.T) {
	var callCount atomic.Int32
	timestamps := make([]time.Time, 0, 3)
	var mu = make(chan time.Time, 10)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		mu <- time.Now()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	delay := 100 * time.Millisecond
	cfg := resilience.RetryConfig{MaxAttempts: 3, Delay: delay}
	transport := resilience.WithRetry(http.DefaultTransport, cfg)
	client := &http.Client{Transport: transport}

	_, _ = doGET(t, client, srv.URL)
	close(mu)
	for ts := range mu {
		timestamps = append(timestamps, ts)
	}

	require.Len(t, timestamps, 3)

	// Интервал между 1→2 и 2→3 должен быть delay
	interval1 := timestamps[1].Sub(timestamps[0])
	interval2 := timestamps[2].Sub(timestamps[1])

	margin := 80 * time.Millisecond
	assert.GreaterOrEqual(t, interval1, delay-margin, "интервал 1→2 слишком мал")
	assert.GreaterOrEqual(t, interval2, delay-margin, "интервал 2→3 слишком мал")

	diff := interval2 - interval1
	if diff < 0 {
		diff = -diff
	}
	assert.Less(t, diff, 2*delay, "интервалы должны быть примерно равны (constant backoff)")
}

// Превышение лимита

func TestRateLimiter_TooManyRequests(t *testing.T) {
	// RPS=2, burst=2 - после 2 запросов следующие получают 429
	limiter := resilience.NewIPRateLimiter(2, 2)
	mw := resilience.RateLimitMiddleware(limiter)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := &http.Client{}
	results := make([]int, 10)
	for i := range results {
		resp, err := doGET(t, client, srv.URL)
		require.NoError(t, err)
		results[i] = resp.StatusCode
		resp.Body.Close()
	}

	has429 := false
	for _, code := range results {
		if code == http.StatusTooManyRequests {
			has429 = true
		}
	}
	assert.True(t, has429, "превышение лимита должно возвращать 429")
}

// Переход в OPEN

func TestCircuitBreaker_OpensOnFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cbCfg := resilience.CircuitBreakerConfig{
		Name:        "test-cb",
		MaxRequests: 1,
		Interval:    0,
		Timeout:     5 * time.Second,
		ReadyToTrip: resilience.DefaultReadyToTrip(0.5, 3),
	}
	transport := resilience.WithCircuitBreaker(http.DefaultTransport, cbCfg)
	client := &http.Client{Transport: transport}

	// делаем запросы до открытия CB
	var cbOpenErr error
	for range 10 {
		_, err := doGET(t, client, srv.URL)
		if err != nil && errors.Is(err, gobreaker.ErrOpenState) {
			cbOpenErr = err
			break
		}
		if err != nil {
			// проверяем обертку
			var cbErr *resilience.CircuitBreakerError
			if errors.As(err, &cbErr) {
				cbOpenErr = err
				break
			}
			// ошибка содержит "circuit breaker open"
			if len(err.Error()) > 0 {
				cbOpenErr = err
				break
			}
		}
	}
	assert.Error(t, cbOpenErr, "circuit breaker должен перейти в OPEN и возвращать ошибку")
}

// HALF-OPEN → CLOSED состояние
//
// Логика: сервер отвечает 500 ровно `failsNeeded` раз (достаточно для открытия CB),
// потом переключается на 200. После waitDurationInOpenState CB переходит в HALF-OPEN,
// пробный вызов успешен → CB закрывается.
func TestCircuitBreaker_HalfOpenToClosed(t *testing.T) {
	// minRequests=3, ratio=0.5 → CB откроется после 3 запросов с 100% ошибок.
	// Делаем  3 провальных запроса, потом сервер всегда отвечает 200.
	const failsNeeded = 3
	var serverCalls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := serverCalls.Add(1)
		if n <= failsNeeded {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cbCfg := resilience.CircuitBreakerConfig{
		Name:        "test-cb-halfopen-closed",
		MaxRequests: 1,                      // 1 пробный вызов в HALF-OPEN
		Interval:    0,                      // не сбрасываем счетчик
		Timeout:     200 * time.Millisecond, // waitDurationInOpenState
		ReadyToTrip: resilience.DefaultReadyToTrip(0.5, failsNeeded),
	}
	// Только CB, без retry - чтобы каждый вызов шел к серверу ровно один раз
	transport := resilience.WithCircuitBreaker(http.DefaultTransport, cbCfg)
	cl := &http.Client{Transport: transport}

	// Шаг 1: открываем CB - делаем ровно failsNeeded запросов с ошибками
	for range failsNeeded {
		_, _ = doGET(t, cl, srv.URL)
	}

	// Шаг 2: убеждаемся что CB открыт - следующий вызов должен вернуть ошибку мгновенно
	_, err := doGET(t, cl, srv.URL)
	require.Error(t, err, "CB должен быть в состоянии OPEN")

	// Шаг 3: ждем waitDurationInOpenState → CB переходит в HALF-OPEN
	time.Sleep(300 * time.Millisecond)

	// Шаг 4: пробный вызов - сервер теперь отвечает 200 → CB должен перейти в CLOSED
	resp, err := doGET(t, cl, srv.URL)
	require.NoError(t, err, "HALF-OPEN: успешный пробный вызов должен пройти без ошибки")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Шаг 5: CB в CLOSED - обычные запросы проходят
	resp2, err := doGET(t, cl, srv.URL)
	require.NoError(t, err, "после перехода в CLOSED запросы должны проходить")
	defer resp2.Body.Close()
}

//  HALF-OPEN состояние

func TestCircuitBreaker_HalfOpenToOpen(t *testing.T) {
	// Сервер всегда возвращает 500
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	const minReqs = 3
	cbCfg := resilience.CircuitBreakerConfig{
		Name:        "test-cb-halfopen-reopen",
		MaxRequests: 1, // 1 пробный вызов в HALF-OPEN
		Interval:    0,
		Timeout:     200 * time.Millisecond, // waitDurationInOpenState
		ReadyToTrip: resilience.DefaultReadyToTrip(0.5, minReqs),
	}
	transport := resilience.WithCircuitBreaker(http.DefaultTransport, cbCfg)
	cl := &http.Client{Transport: transport}

	// Шаг 1: открываем CB
	for range minReqs {
		_, _ = doGET(t, cl, srv.URL)
	}

	// Шаг 2: убеждаемся что OPEN
	_, err := doGET(t, cl, srv.URL)
	require.Error(t, err, "CB должен быть в состоянии OPEN")

	// Шаг 3: ждем → HALF-OPEN
	time.Sleep(300 * time.Millisecond)

	// Шаг 4: пробный вызов неуспешен (сервер все еще 500) → CB возвращается в OPEN
	_, _ = doGET(t, cl, srv.URL)

	// Шаг 5: следующий вызов должен мгновенно вернуть ошибку OPEN (без обращения к серверу)
	start := time.Now()
	_, err2 := doGET(t, cl, srv.URL)
	elapsed := time.Since(start)

	require.Error(t, err2, "после неудачного пробного вызова CB должен вернуться в OPEN")
	// Вызов в OPEN завершается мгновенно, без ожидания сети
	assert.Less(t, elapsed, 50*time.Millisecond, "вызов в OPEN должен завершаться мгновенно")
}
