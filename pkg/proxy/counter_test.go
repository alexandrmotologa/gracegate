package proxy

import (
	"sync"
	"testing"
	"time"
)

func TestAtomicTrackerFlow(t *testing.T) {
	tracker := NewAtomicTracker()

	if inFlight := tracker.CurrentInFlight(); inFlight != 0 {
		t.Fatalf("expected initial in-flight 0, got %d", inFlight)
	}

	done1 := tracker.RequestStarted(ProtocolHTTP1)
	done2 := tracker.RequestStarted(ProtocolHTTP2)
	done3 := tracker.RequestStarted(ProtocolGRPC)

	if inFlight := tracker.CurrentInFlight(); inFlight != 3 {
		t.Fatalf("expected in-flight 3, got %d", inFlight)
	}

	done1(200, 100, 200, 5*time.Millisecond)
	if inFlight := tracker.CurrentInFlight(); inFlight != 2 {
		t.Fatalf("expected in-flight 2, got %d", inFlight)
	}

	done2(200, 150, 300, 10*time.Millisecond)
	done3(500, 50, 80, 2*time.Millisecond)

	if inFlight := tracker.CurrentInFlight(); inFlight != 0 {
		t.Fatalf("expected in-flight 0, got %d", inFlight)
	}

	snap := tracker.Snapshot()
	if snap.TotalRequests != 3 {
		t.Fatalf("expected 3 total requests, got %d", snap.TotalRequests)
	}
	if snap.TotalErrors != 1 {
		t.Fatalf("expected 1 error, got %d", snap.TotalErrors)
	}
	if snap.BytesReceived != 300 {
		t.Fatalf("expected 300 bytes received, got %d", snap.BytesReceived)
	}
	if snap.BytesSent != 580 {
		t.Fatalf("expected 580 bytes sent, got %d", snap.BytesSent)
	}
}

func TestAtomicTrackerConcurrency(t *testing.T) {
	tracker := NewAtomicTracker()
	const workers = 50
	const requestsPerWorker = 200

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			proto := ProtocolHTTP1
			if id%3 == 1 {
				proto = ProtocolHTTP2
			} else if id%3 == 2 {
				proto = ProtocolGRPC
			}

			for j := 0; j < requestsPerWorker; j++ {
				done := tracker.RequestStarted(proto)
				time.Sleep(10 * time.Microsecond)
				done(200, 10, 20, 15*time.Microsecond)
			}
		}(i)
	}

	wg.Wait()

	drained := tracker.WaitForZeroInFlight(1 * time.Second)
	if !drained {
		t.Fatalf("failed to drain all requests")
	}

	snap := tracker.Snapshot()
	expectedTotal := int64(workers * requestsPerWorker)
	if snap.TotalRequests != expectedTotal {
		t.Fatalf("expected %d total requests, got %d", expectedTotal, snap.TotalRequests)
	}
	if snap.InFlightTotal != 0 {
		t.Fatalf("expected 0 in-flight total, got %d", snap.InFlightTotal)
	}
}
