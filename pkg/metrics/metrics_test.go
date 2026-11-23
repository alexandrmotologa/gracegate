package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alexandrmotologa/gracegate/pkg/proxy"
)

type dummyPhaseProvider struct {
	phase string
}

func (d *dummyPhaseProvider) CurrentPhase() string {
	return d.phase
}

func TestMetricsHandler(t *testing.T) {
	tracker := proxy.NewAtomicTracker()
	phaseProvider := &dummyPhaseProvider{phase: "RUNNING"}

	done := tracker.RequestStarted(proxy.ProtocolHTTP1)
	done(200, 100, 200, 5*time.Millisecond)

	handler := Handler(tracker, phaseProvider)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("metrics request failed: %v", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	body := string(bodyBytes)

	if !strings.Contains(body, "gracegate_inflight_requests") {
		t.Fatalf("missing gracegate_inflight_requests metric")
	}
	if !strings.Contains(body, "gracegate_requests_total 1") {
		t.Fatalf("expected gracegate_requests_total 1, got:\n%s", body)
	}
	if !strings.Contains(body, `gracegate_drain_phase{phase="RUNNING"} 1`) {
		t.Fatalf("expected phase RUNNING 1, got:\n%s", body)
	}
}
