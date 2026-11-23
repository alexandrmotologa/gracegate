package proxy

import (
	"testing"
	"time"
)

func TestCircuitBreakerFlow(t *testing.T) {
	cfg := CircuitBreakerConfig{
		Enabled:             true,
		ConsecutiveFailures: 3,
		RecoverySuccesses:   2,
		CooldownDuration:    50 * time.Millisecond,
	}

	cb := NewCircuitBreaker(cfg)

	if cb.IsTripped() {
		t.Fatalf("expected circuit breaker to start closed")
	}

	// 2 errors: should still be closed
	cb.RecordResult(500)
	cb.RecordResult(502)
	if cb.IsTripped() {
		t.Fatalf("expected breaker to remain closed after 2 errors (threshold 3)")
	}

	// 1 success resets consecutive error count
	cb.RecordResult(200)
	cb.RecordResult(500)
	cb.RecordResult(500)
	if cb.IsTripped() {
		t.Fatalf("expected breaker still closed")
	}

	// 3rd consecutive error: trips to open
	cb.RecordResult(500)
	if !cb.IsTripped() {
		t.Fatalf("expected breaker to trip after 3 consecutive errors")
	}
	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen, got %s", cb.State())
	}

	// Wait for cooldown
	time.Sleep(60 * time.Millisecond)

	// First success transitions to half-open
	cb.RecordResult(200)
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected StateHalfOpen after cooldown + success, got %s", cb.State())
	}

	// Second success recovers back to closed
	cb.RecordResult(200)
	if cb.IsTripped() {
		t.Fatalf("expected breaker to recover and close after 2 successes")
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed, got %s", cb.State())
	}
}
