package lifecycle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alexandrmotologa/gracegate/pkg/proxy"
)

func TestFSMTransitions(t *testing.T) {
	tracker := proxy.NewAtomicTracker()
	cfg := Config{
		QuorumDuration: 50 * time.Millisecond,
		DrainTimeout:   500 * time.Millisecond,
	}

	fsm := NewFSM(cfg, tracker)
	if phase := fsm.CurrentPhase(); phase != PhaseRunning {
		t.Fatalf("expected initial phase RUNNING, got %s", phase)
	}

	// Start 2 in-flight requests
	done1 := tracker.RequestStarted(proxy.ProtocolHTTP1)
	done2 := tracker.RequestStarted(proxy.ProtocolHTTP2)

	phaseChan := make(chan Phase, 4)
	fsm.Subscribe(func(p Phase) {
		phaseChan <- p
	})

	// Trigger drain
	fsm.TriggerDrain(ReasonSIGTERM)

	// Immediately, state should be DRAINING_QUORUM
	if !fsm.IsDraining() {
		t.Fatalf("expected IsDraining() == true")
	}

	p1 := <-phaseChan
	if p1 != PhaseDrainingQuorum {
		t.Fatalf("expected DRAINING_QUORUM, got %s", p1)
	}

	// After quorum wait (50ms), should transition to DRAINING_INFLIGHT
	p2 := <-phaseChan
	if p2 != PhaseDrainingInflight {
		t.Fatalf("expected DRAINING_INFLIGHT, got %s", p2)
	}

	if !fsm.IsCloseConnectionRequired() {
		t.Fatalf("expected IsCloseConnectionRequired() == true in DRAINING_INFLIGHT")
	}

	// Complete the in-flight requests
	done1(200, 10, 20, time.Millisecond)
	done2(200, 10, 20, time.Millisecond)

	// Should transition to TERMINATED
	p3 := <-phaseChan
	if p3 != PhaseTerminated {
		t.Fatalf("expected TERMINATED, got %s", p3)
	}

	timeline := fsm.Timeline()
	if len(timeline.Milestones) < 4 {
		t.Fatalf("expected at least 4 milestones, got %d", len(timeline.Milestones))
	}
	if timeline.QuorumDurationMs <= 0 {
		t.Fatalf("expected positive QuorumDurationMs, got %d", timeline.QuorumDurationMs)
	}
}

func TestHealthEndpoints(t *testing.T) {
	tracker := proxy.NewAtomicTracker()
	cfg := Config{
		QuorumDuration: 50 * time.Millisecond,
		DrainTimeout:   500 * time.Millisecond,
	}

	fsm := NewFSM(cfg, tracker)
	hs := NewHealthServer("127.0.0.1:0", fsm, tracker)

	ts := httptest.NewServer(hs.server.Handler)
	defer ts.Close()

	client := ts.Client()

	// 1. Initial readiness should be 200 OK
	resp, err := client.Get(ts.URL + "/gracegate/healthz")
	if err != nil {
		t.Fatalf("healthz request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 2. Initial liveness should be 200 OK
	respLive, err := client.Get(ts.URL + "/gracegate/livez")
	if err != nil {
		t.Fatalf("livez request failed: %v", err)
	}
	if respLive.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", respLive.StatusCode)
	}
	_ = respLive.Body.Close()

	// 3. Trigger drain via preStop HTTP endpoint
	respDrain, err := client.Get(ts.URL + "/gracegate/drain")
	if err != nil {
		t.Fatalf("drain request failed: %v", err)
	}
	if respDrain.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", respDrain.StatusCode)
	}
	_ = respDrain.Body.Close()

	// 4. Readiness must now immediately return 503 Service Unavailable
	respDown, err := client.Get(ts.URL + "/gracegate/healthz")
	if err != nil {
		t.Fatalf("healthz after drain failed: %v", err)
	}
	if respDown.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d", respDown.StatusCode)
	}
	_ = respDown.Body.Close()

	// 5. Check timeline endpoint
	respTl, err := client.Get(ts.URL + "/gracegate/timeline")
	if err != nil {
		t.Fatalf("timeline request failed: %v", err)
	}
	if respTl.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for timeline, got %d", respTl.StatusCode)
	}
	var tl DrainTimeline
	if err := json.NewDecoder(respTl.Body).Decode(&tl); err != nil {
		t.Fatalf("failed to decode timeline: %v", err)
	}
	_ = respTl.Body.Close()

	if len(tl.Milestones) == 0 {
		t.Fatalf("expected recorded milestones in timeline")
	}
}

func TestSyncDrainTrigger(t *testing.T) {
	tracker := proxy.NewAtomicTracker()
	cfg := Config{
		QuorumDuration: 20 * time.Millisecond,
		DrainTimeout:   500 * time.Millisecond,
	}

	fsm := NewFSM(cfg, tracker)
	hs := NewHealthServer("127.0.0.1:0", fsm, tracker)
	ts := httptest.NewServer(hs.server.Handler)
	defer ts.Close()

	client := ts.Client()

	done := tracker.RequestStarted(proxy.ProtocolHTTP1)

	// Release request in background after 50ms
	go func() {
		time.Sleep(50 * time.Millisecond)
		done(200, 10, 20, 10*time.Millisecond)
	}()

	start := time.Now()
	resp, err := client.Get(ts.URL + "/gracegate/drain?sync=true")
	if err != nil {
		t.Fatalf("sync drain request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from sync drain, got %d", resp.StatusCode)
	}

	dur := time.Since(start)
	if dur < 50*time.Millisecond {
		t.Fatalf("expected sync drain to wait at least 50ms, took %v", dur)
	}
	if fsm.CurrentPhase() != PhaseTerminated {
		t.Fatalf("expected phase TERMINATED, got %s", fsm.CurrentPhase())
	}
}
