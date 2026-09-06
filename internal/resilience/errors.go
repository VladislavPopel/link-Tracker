package resilience

import "fmt"

// CircuitBreakerError сигнализирует, что circuit breaker открыт
type CircuitBreakerError struct {
	Name string
	Msg  string
}

func (e *CircuitBreakerError) Error() string {
	return fmt.Sprintf("circuit breaker [%s] %s", e.Name, e.Msg)
}
