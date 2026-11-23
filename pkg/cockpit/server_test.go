package cockpit

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alexandrmotologa/gracegate/pkg/lifecycle"
	"github.com/alexandrmotologa/gracegate/pkg/proxy"
)

func TestCockpitServer(t *testing.T) {
	tracker := proxy.NewAtomicTracker()
	fsm := lifecycle.NewFSM(lifecycle.Config{
		QuorumDuration: 50 * time.Millisecond,
		DrainTimeout:   500 * time.Millisecond,
	}, tracker)

	server := NewServer(0, tracker, fsm)
	ts := httptest.NewServer(server.server.Handler)
	defer ts.Close()

	client := ts.Client()

	// 1. Static UI serving
	resp, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("failed to fetch index.html: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for index.html, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "gracegate Cockpit") {
		t.Fatalf("index.html missing title")
	}

	// 2. Status API
	respStatus, err := client.Get(ts.URL + "/api/status")
	if err != nil {
		t.Fatalf("failed to fetch status API: %v", err)
	}
	defer respStatus.Body.Close()

	var payload StatusPayload
	if err := json.NewDecoder(respStatus.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode status payload: %v", err)
	}

	if payload.Phase != lifecycle.PhaseRunning {
		t.Fatalf("expected phase RUNNING, got %s", payload.Phase)
	}

	// 3. Trigger drain from Cockpit
	respDrain, err := client.Post(ts.URL+"/api/drain", "application/json", nil)
	if err != nil {
		t.Fatalf("failed to post drain: %v", err)
	}
	defer respDrain.Body.Close()

	if respDrain.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", respDrain.StatusCode)
	}

	if !fsm.IsDraining() {
		t.Fatalf("expected FSM to be draining")
	}
}
