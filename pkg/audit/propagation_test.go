package audit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAuditorRun(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("X-Pod-Name", "order-service-pod-xyz")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	drainTriggered := false
	drainServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		drainTriggered = true
		w.WriteHeader(http.StatusAccepted)
	}))
	defer drainServer.Close()

	cfg := AuditConfig{
		TargetURL:    server.URL,
		DrainTrigger: drainServer.URL,
		ProbeCount:   15,
		Interval:     10 * time.Millisecond,
		Timeout:      500 * time.Millisecond,
	}

	auditor := NewAuditor(cfg)
	report, err := auditor.Run(context.Background())
	if err != nil {
		t.Fatalf("auditor failed: %v", err)
	}

	if report.TotalProbes != 15 {
		t.Fatalf("expected 15 total probes, got %d", report.TotalProbes)
	}
	if report.SuccessfulProbes != 15 {
		t.Fatalf("expected 15 successful probes, got %d", report.SuccessfulProbes)
	}
	if !report.ZeroLossMaintained {
		t.Fatalf("expected zero loss maintained")
	}
	if !drainTriggered {
		t.Fatalf("expected drain trigger to be called")
	}
	if report.AvgLatencyMs <= 0 {
		t.Fatalf("expected positive average latency, got %f", report.AvgLatencyMs)
	}
}
