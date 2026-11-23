package metrics

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/alexandrmotologa/gracegate/pkg/proxy"
)

// PhaseProvider returns the current phase string.
type PhaseProvider interface {
	CurrentPhase() string
}

// Handler returns an HTTP handler serving Prometheus format metrics without heavy external dependencies.
func Handler(tracker *proxy.AtomicTracker, phaseProvider PhaseProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snap := tracker.Snapshot()
		var phase string
		if phaseProvider != nil {
			phase = phaseProvider.CurrentPhase()
		} else {
			phase = "UNKNOWN"
		}

		var b strings.Builder

		// Header & In-Flight gauge
		b.WriteString("# HELP gracegate_inflight_requests Current number of active in-flight requests.\n")
		b.WriteString("# TYPE gracegate_inflight_requests gauge\n")
		fmt.Fprintf(&b, "gracegate_inflight_requests{protocol=\"HTTP/1.1\"} %d\n", snap.InFlightHTTP1)
		fmt.Fprintf(&b, "gracegate_inflight_requests{protocol=\"HTTP/2\"} %d\n", snap.InFlightHTTP2)
		fmt.Fprintf(&b, "gracegate_inflight_requests{protocol=\"gRPC\"} %d\n", snap.InFlightGRPC)
		fmt.Fprintf(&b, "gracegate_inflight_requests{protocol=\"total\"} %d\n", snap.InFlightTotal)

		// Active Clients
		b.WriteString("\n# HELP gracegate_active_clients Current number of active TCP connections.\n")
		b.WriteString("# TYPE gracegate_active_clients gauge\n")
		fmt.Fprintf(&b, "gracegate_active_clients %d\n", snap.ActiveClients)

		// Total Requests counter
		b.WriteString("\n# HELP gracegate_requests_total Total number of processed requests.\n")
		b.WriteString("# TYPE gracegate_requests_total counter\n")
		fmt.Fprintf(&b, "gracegate_requests_total %d\n", snap.TotalRequests)

		// Total Errors counter
		b.WriteString("\n# HELP gracegate_errors_total Total number of server 5xx errors.\n")
		b.WriteString("# TYPE gracegate_errors_total counter\n")
		fmt.Fprintf(&b, "gracegate_errors_total %d\n", snap.TotalErrors)

		// Bytes metrics
		b.WriteString("\n# HELP gracegate_bytes_received_total Total inbound bytes proxied.\n")
		b.WriteString("# TYPE gracegate_bytes_received_total counter\n")
		fmt.Fprintf(&b, "gracegate_bytes_received_total %d\n", snap.BytesReceived)

		b.WriteString("\n# HELP gracegate_bytes_sent_total Total outbound bytes proxied.\n")
		b.WriteString("# TYPE gracegate_bytes_sent_total counter\n")
		fmt.Fprintf(&b, "gracegate_bytes_sent_total %d\n", snap.BytesSent)

		// Phase gauge (1 for active phase, 0 for others)
		b.WriteString("\n# HELP gracegate_drain_phase Current lifecycle draining phase.\n")
		b.WriteString("# TYPE gracegate_drain_phase gauge\n")
		phases := []string{"RUNNING", "DRAINING_QUORUM", "DRAINING_INFLIGHT", "TERMINATED"}
		for _, p := range phases {
			val := 0
			if p == phase {
				val = 1
			}
			fmt.Fprintf(&b, "gracegate_drain_phase{phase=\"%s\"} %d\n", p, val)
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(b.String()))
	}
}
