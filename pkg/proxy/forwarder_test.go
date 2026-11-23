package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type mockDrainState struct {
	draining    bool
	quorum      bool
	closeHeader bool
	packets     int
}

func (m *mockDrainState) IsDraining() bool                { return m.draining }
func (m *mockDrainState) IsQuorumPhase() bool            { return m.quorum }
func (m *mockDrainState) IsCloseConnectionRequired() bool { return m.closeHeader }
func (m *mockDrainState) RecordIngressPacket()           { m.packets++ }

func TestForwarderProxyFlow(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		w.Header().Set("X-Backend-Handled", "true")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("backend payload response"))
	}))
	defer backend.Close()

	tracker := NewAtomicTracker()
	drainState := &mockDrainState{}

	cfg := Config{
		ListenAddr:    "127.0.0.1:0",
		TargetAddr:    backend.URL,
		DrainTimeout:  5 * time.Second,
		StateProvider: drainState,
	}

	fwd, err := NewForwarder(cfg, tracker)
	if err != nil {
		t.Fatalf("failed to create forwarder: %v", err)
	}

	proxyServer := httptest.NewServer(fwd)
	defer proxyServer.Close()

	client := proxyServer.Client()
	resp, err := client.Get(proxyServer.URL + "/test-endpoint")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	if string(body) != "backend payload response" {
		t.Fatalf("unexpected body: %s", string(body))
	}
	if resp.Header.Get("X-Backend-Handled") != "true" {
		t.Fatalf("missing backend header")
	}

	snap := tracker.Snapshot()
	if snap.TotalRequests != 1 {
		t.Fatalf("expected 1 total request, got %d", snap.TotalRequests)
	}
	if snap.InFlightTotal != 0 {
		t.Fatalf("expected 0 in-flight after completion, got %d", snap.InFlightTotal)
	}

	// Test drain header injection
	drainState.draining = true
	drainState.closeHeader = true

	resp2, err := client.Get(proxyServer.URL + "/draining-endpoint")
	if err != nil {
		t.Fatalf("request during drain failed: %v", err)
	}
	defer resp2.Body.Close()

	if !resp2.Close && resp2.Header.Get("Connection") != "close" {
		t.Fatalf("expected Connection: close during drain, got resp.Close=%v header=%s", resp2.Close, resp2.Header.Get("Connection"))
	}
	if drainState.packets != 1 {
		t.Fatalf("expected 1 recorded ingress packet during drain, got %d", drainState.packets)
	}
}

func BenchmarkForwarderThroughput(b *testing.B) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	tracker := NewAtomicTracker()
	cfg := Config{
		ListenAddr:   "127.0.0.1:0",
		TargetAddr:   backend.URL,
		DrainTimeout: 5 * time.Second,
	}

	fwd, err := NewForwarder(cfg, tracker)
	if err != nil {
		b.Fatalf("failed to create forwarder: %v", err)
	}

	proxyServer := httptest.NewServer(fwd)
	defer proxyServer.Close()

	client := proxyServer.Client()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(proxyServer.URL + "/bench")
		if err != nil {
			b.Fatalf("failed request: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}
