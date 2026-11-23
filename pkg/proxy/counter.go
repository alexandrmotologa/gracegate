package proxy

import (
	"sync"
	"sync/atomic"
	"time"
)

// Protocol identifies the application-layer protocol of a proxied stream.
type Protocol string

const (
	ProtocolHTTP1 Protocol = "HTTP/1.1"
	ProtocolHTTP2 Protocol = "HTTP/2"
	ProtocolGRPC  Protocol = "gRPC"
	ProtocolOther Protocol = "TCP"
)

// Snapshot represents an immutable point-in-time view of proxy counters.
type Snapshot struct {
	InFlightTotal int64            `json:"in_flight_total"`
	InFlightHTTP1 int64            `json:"in_flight_http1"`
	InFlightHTTP2 int64            `json:"in_flight_http2"`
	InFlightGRPC  int64            `json:"in_flight_grpc"`
	TotalRequests int64            `json:"total_requests"`
	TotalErrors   int64            `json:"total_errors"`
	ActiveClients int64            `json:"active_clients"`
	BytesReceived int64            `json:"bytes_received"`
	BytesSent     int64            `json:"bytes_sent"`
	AvgLatencyNs  int64            `json:"avg_latency_ns"`
	Timestamp     time.Time        `json:"timestamp"`
	ProtocolBreak map[string]int64 `json:"protocol_breakdown"`
}

// AtomicTracker maintains real-time in-flight metrics with thread-safe atomic primitives.
type AtomicTracker struct {
	inFlightHTTP1 atomic.Int64
	inFlightHTTP2 atomic.Int64
	inFlightGRPC  atomic.Int64
	inFlightTotal atomic.Int64

	totalRequests atomic.Int64
	totalErrors   atomic.Int64
	activeClients atomic.Int64
	bytesReceived atomic.Int64
	bytesSent     atomic.Int64

	totalLatencyNs atomic.Int64
	latencySamples atomic.Int64

	mu        sync.RWMutex
	drainChan chan struct{}
}

// NewAtomicTracker creates a tracker with initialized counters.
func NewAtomicTracker() *AtomicTracker {
	return &AtomicTracker{
		drainChan: make(chan struct{}, 1),
	}
}

// RequestStarted records the start of an in-flight request and returns a completion callback.
func (t *AtomicTracker) RequestStarted(proto Protocol) func(statusCode int, bytesIn, bytesOut int64, duration time.Duration) {
	t.inFlightTotal.Add(1)
	t.totalRequests.Add(1)

	switch proto {
	case ProtocolHTTP1:
		t.inFlightHTTP1.Add(1)
	case ProtocolHTTP2:
		t.inFlightHTTP2.Add(1)
	case ProtocolGRPC:
		t.inFlightGRPC.Add(1)
	}

	return func(statusCode int, bytesIn, bytesOut int64, duration time.Duration) {
		t.bytesReceived.Add(bytesIn)
		t.bytesSent.Add(bytesOut)

		if statusCode >= 500 {
			t.totalErrors.Add(1)
		}

		if durNs := duration.Nanoseconds(); durNs > 0 {
			t.totalLatencyNs.Add(durNs)
			t.latencySamples.Add(1)
		}

		switch proto {
		case ProtocolHTTP1:
			t.inFlightHTTP1.Add(-1)
		case ProtocolHTTP2:
			t.inFlightHTTP2.Add(-1)
		case ProtocolGRPC:
			t.inFlightGRPC.Add(-1)
		}

		remaining := t.inFlightTotal.Add(-1)
		if remaining <= 0 {
			select {
			case t.drainChan <- struct{}{}:
			default:
			}
		}
	}
}

// ClientConnected increments the active client connection gauge.
func (t *AtomicTracker) ClientConnected() {
	t.activeClients.Add(1)
}

// ClientDisconnected decrements the active client connection gauge.
func (t *AtomicTracker) ClientDisconnected() {
	t.activeClients.Add(-1)
}

// CurrentInFlight returns the instantaneous count of active in-flight requests.
func (t *AtomicTracker) CurrentInFlight() int64 {
	val := t.inFlightTotal.Load()
	if val < 0 {
		return 0
	}
	return val
}

// WaitForZeroInFlight blocks until in-flight requests reach zero or the timeout expires.
func (t *AtomicTracker) WaitForZeroInFlight(timeout time.Duration) bool {
	if t.CurrentInFlight() == 0 {
		return true
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-t.drainChan:
			if t.CurrentInFlight() == 0 {
				return true
			}
		case <-ticker.C:
			if t.CurrentInFlight() == 0 {
				return true
			}
		case <-timer.C:
			return t.CurrentInFlight() == 0
		}
	}
}

// Snapshot captures the current state for telemetry and metrics.
func (t *AtomicTracker) Snapshot() Snapshot {
	samples := t.latencySamples.Load()
	var avgLatency int64
	if samples > 0 {
		avgLatency = t.totalLatencyNs.Load() / samples
	}

	h1 := t.inFlightHTTP1.Load()
	h2 := t.inFlightHTTP2.Load()
	grpc := t.inFlightGRPC.Load()
	total := t.inFlightTotal.Load()

	return Snapshot{
		InFlightTotal: total,
		InFlightHTTP1: h1,
		InFlightHTTP2: h2,
		InFlightGRPC:  grpc,
		TotalRequests: t.totalRequests.Load(),
		TotalErrors:   t.totalErrors.Load(),
		ActiveClients: t.activeClients.Load(),
		BytesReceived: t.bytesReceived.Load(),
		BytesSent:     t.bytesSent.Load(),
		AvgLatencyNs:  avgLatency,
		Timestamp:     time.Now().UTC(),
		ProtocolBreak: map[string]int64{
			string(ProtocolHTTP1): h1,
			string(ProtocolHTTP2): h2,
			string(ProtocolGRPC):  grpc,
		},
	}
}
