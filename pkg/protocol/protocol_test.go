package protocol

import (
	"bytes"
	"net/http"
	"testing"
	"time"
)

func TestHTTP1Drainer(t *testing.T) {
	d := NewHTTP1Drainer()

	h := http.Header{}
	h.Set("Connection", "keep-alive")
	h.Set("Keep-Alive", "timeout=5, max=1000")

	d.MutateHeadersForDrain(h)

	if h.Get("Connection") != "close" {
		t.Fatalf("expected Connection: close, got %s", h.Get("Connection"))
	}
	if h.Get("Keep-Alive") != "" {
		t.Fatalf("expected Keep-Alive to be deleted")
	}

	raw := []byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nOK")
	modified, err := d.InjectConnectionCloseRaw(raw)
	if err != nil {
		t.Fatalf("failed to mutate raw response: %v", err)
	}

	if !bytes.Contains(modified, []byte("Connection: close")) {
		t.Fatalf("expected raw output to contain Connection: close, got:\n%s", string(modified))
	}
}

func TestHTTP2DrainerFrames(t *testing.T) {
	d := NewHTTP2Drainer()

	// 1. Advisory Frame (Step 1)
	advisory := d.BuildAdvisoryGoAwayFrame()
	if len(advisory) < 9 {
		t.Fatalf("frame too short: %d bytes", len(advisory))
	}

	lastStream, errCode, debugData, err := d.ParseGoAwayFrame(advisory)
	if err != nil {
		t.Fatalf("failed to parse advisory GOAWAY: %v", err)
	}

	if lastStream != MaxStreamID {
		t.Fatalf("expected lastStreamID %d, got %d", MaxStreamID, lastStream)
	}
	if errCode != ErrCodeNoError {
		t.Fatalf("expected NO_ERROR (0), got %v", errCode)
	}
	if string(debugData) != "gracegate: draining quorum" {
		t.Fatalf("unexpected debug data: %s", string(debugData))
	}

	// 2. Final Frame (Step 2)
	final := d.BuildFinalGoAwayFrame(42)
	lastStreamFinal, _, debugFinal, err := d.ParseGoAwayFrame(final)
	if err != nil {
		t.Fatalf("failed to parse final GOAWAY: %v", err)
	}
	if lastStreamFinal != 42 {
		t.Fatalf("expected stream ID 42, got %d", lastStreamFinal)
	}
	if string(debugFinal) != "gracegate: drain complete" {
		t.Fatalf("unexpected final debug data: %s", string(debugFinal))
	}

	// 3. Raw Frame Direct Builder
	rawDirect := d.BuildRawFrameDirect(101, 0, []byte("direct"))
	lastStreamDirect, _, debugDirect, err := d.ParseGoAwayFrame(rawDirect)
	if err != nil {
		t.Fatalf("failed to parse raw direct frame: %v", err)
	}
	if lastStreamDirect != 101 {
		t.Fatalf("expected stream ID 101, got %d", lastStreamDirect)
	}
	if string(debugDirect) != "direct" {
		t.Fatalf("unexpected debug data: %s", string(debugDirect))
	}
}

func TestGRPCDrainer(t *testing.T) {
	g := NewGRPCDrainer()

	req, _ := http.NewRequest("POST", "http://localhost/proto.OrderService/CreateOrder", nil)
	req.Header.Set("Content-Type", "application/grpc")

	if !g.IsGRPCRequest(req) {
		t.Fatalf("expected request to be recognized as gRPC")
	}

	header := http.Header{}
	trailer := http.Header{}
	trailer.Set("grpc-status", "0")
	trailer.Set("grpc-message", "success")

	code, msg := g.ExtractGRPCStatus(header, trailer)
	if code != GRPCStatusOk {
		t.Fatalf("expected status OK, got %v", code)
	}
	if msg != "success" {
		t.Fatalf("expected message 'success', got %q", msg)
	}
	if !g.IsZeroLossSafe(code) {
		t.Fatalf("status OK should be zero-loss safe")
	}

	// Test Unavailable status
	trailer.Set("grpc-status", "14")
	codeUnavail, _ := g.ExtractGRPCStatus(header, trailer)
	if g.IsZeroLossSafe(codeUnavail) {
		t.Fatalf("status Unavailable should not be considered zero-loss safe")
	}
}

func TestEWMAEstimator(t *testing.T) {
	cfg := EstimatorConfig{
		Alpha:   0.3,
		MinWait: 1 * time.Second,
		MaxWait: 10 * time.Second,
	}

	est := NewEWMAEstimator(cfg)

	// Feed samples in milliseconds: 1500, 2000, 1800, 2200, 1900
	samples := []time.Duration{
		1500 * time.Millisecond,
		2000 * time.Millisecond,
		1800 * time.Millisecond,
		2200 * time.Millisecond,
		1900 * time.Millisecond,
	}

	for _, s := range samples {
		est.RecordObservation(s)
	}

	stats := est.Stats()
	if stats.Count != 5 {
		t.Fatalf("expected count 5, got %d", stats.Count)
	}
	if stats.MinMs != 1500 || stats.MaxMs != 2200 {
		t.Fatalf("unexpected min/max: min=%d max=%d", stats.MinMs, stats.MaxMs)
	}
	if stats.RecommendedQuorumMs < 1000 || stats.RecommendedQuorumMs > 10000 {
		t.Fatalf("recommended quorum out of bounds: %dms", stats.RecommendedQuorumMs)
	}
}
