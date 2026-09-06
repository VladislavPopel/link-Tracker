package resilience

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	defaultTTL     = 10 * time.Minute
	clenupInterval = 5 * time.Minute
)

// ipLimiter хранит rate.Limiter и время последнего обращения для одного IP
type ipLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// IPRateLimiter ограничивает частоту запросов с одного IP-адреса
type IPRateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*ipLimiter
	rps      rate.Limit
	burst    int
	ttl      time.Duration
}

func NewIPRateLimiter(rps float64, burst int) *IPRateLimiter {
	rl := &IPRateLimiter{
		limiters: make(map[string]*ipLimiter),
		rps:      rate.Limit(rps),
		burst:    burst,
		ttl:      defaultTTL,
	}
	go rl.cleanup()
	return rl
}

// Allow возвращает true, если запрос с данного IP разрешен
func (rl *IPRateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	l, ok := rl.limiters[ip]
	if !ok {
		l = &ipLimiter{limiter: rate.NewLimiter(rl.rps, rl.burst)}
		rl.limiters[ip] = l
	}
	l.lastSeen = time.Now()
	return l.limiter.Allow()
}

// cleanup периодически удаляет устаревшие записи, чтобы не копить память
func (rl *IPRateLimiter) cleanup() {
	ticker := time.NewTicker(clenupInterval)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		for ip, l := range rl.limiters {
			if time.Since(l.lastSeen) > rl.ttl {
				delete(rl.limiters, ip)
			}
		}
		rl.mu.Unlock()
	}
}

// RateLimitMiddleware возвращает HTTP middleware, ограничивающий запросы по IP
// При превышении лимита отвечает 429
func RateLimitMiddleware(limiter *IPRateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := extractIP(r)
			if !limiter.Allow(ip) {
				http.Error(w, `{"description":"too many requests","code":"429"}`, http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// extractIP возвращает IP клиента, учитывая X-Forwarded-For и X-Real-IP.
func extractIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if idx := len(xff); idx > 0 {
			for i := range xff {
				if xff[i] == ',' {
					return xff[:i]
				}
			}
			return xff
		}
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
