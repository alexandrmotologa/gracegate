package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/alexandrmotologa/gracegate/pkg/proxy"
)

// HealthServer manages Kubernetes probe endpoints and lifecycle triggers.
type HealthServer struct {
	listenAddr string
	fsm        *FSM
	tracker    *proxy.AtomicTracker
	server     *http.Server
	mux        *http.ServeMux
	mu         sync.Mutex
}

// NewHealthServer creates a health probe HTTP server.
func NewHealthServer(listenAddr string, fsm *FSM, tracker *proxy.AtomicTracker) *HealthServer {
	mux := http.NewServeMux()
	hs := &HealthServer{
		listenAddr: listenAddr,
		fsm:        fsm,
		tracker:    tracker,
		mux:        mux,
	}

	mux.HandleFunc("/gracegate/healthz", hs.handleReadiness)
	mux.HandleFunc("/gracegate/livez", hs.handleLiveness)
	mux.HandleFunc("/gracegate/drain", hs.handleDrainTrigger)
	mux.HandleFunc("/gracegate/timeline", hs.handleTimeline)
	mux.HandleFunc("/gracegate/status", hs.handleStatus)

	hs.server = &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return hs
}

// MountHandler registers an additional HTTP handler on the health server.
func (h *HealthServer) MountHandler(pattern string, handler http.Handler) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mux.Handle(pattern, handler)
}

// Start runs the health server in the background.
func (h *HealthServer) Start() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	ln, err := net.Listen("tcp", h.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to bind health server on %s: %w", h.listenAddr, err)
	}

	go func() {
		if err := h.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			// server stopped
		}
	}()

	return nil
}

// Stop terminates the health server.
func (h *HealthServer) Stop(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.server != nil {
		return h.server.Shutdown(ctx)
	}
	return nil
}

func (h *HealthServer) handleReadiness(w http.ResponseWriter, r *http.Request) {
	phase := h.fsm.CurrentPhase()
	w.Header().Set("Content-Type", "application/json")

	if phase == PhaseRunning {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP","phase":"RUNNING","message":"Pod accepting traffic"}`))
		return
	}

	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprintf(w, `{"status":"DOWN","phase":%q,"message":"Pod draining in-flight traffic"}`, phase)
}

func (h *HealthServer) handleLiveness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"UP","message":"gracegate alive"}`))
}

func (h *HealthServer) handleDrainTrigger(w http.ResponseWriter, r *http.Request) {
	isSync := r.URL.Query().Get("sync") == "true"

	if isSync {
		ctx, cancel := context.WithTimeout(r.Context(), h.fsm.config.DrainTimeout)
		defer cancel()

		if err := h.fsm.ExecuteDrainSync(ctx, ReasonPreStop); err != nil {
			w.WriteHeader(http.StatusGatewayTimeout)
			_, _ = fmt.Fprintf(w, `{"status":"TIMEOUT","error":%q}`, err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"DRAINED","message":"All in-flight requests drained cleanly"}`))
		return
	}

	h.fsm.TriggerDrain(ReasonPreStop)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"DRAIN_INITIATED","phase":"DRAINING_QUORUM"}`))
}

func (h *HealthServer) handleTimeline(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	timeline := h.fsm.Timeline()
	_ = json.NewEncoder(w).Encode(timeline)
}

func (h *HealthServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	phase := h.fsm.CurrentPhase()
	snap := h.tracker.Snapshot()
	timeline := h.fsm.Timeline()

	status := struct {
		Phase      Phase             `json:"phase"`
		InFlight   int64             `json:"in_flight"`
		Snapshot   proxy.Snapshot    `json:"metrics"`
		Milestones []TimelineMilestone `json:"milestones"`
	}{
		Phase:      phase,
		InFlight:   h.tracker.CurrentInFlight(),
		Snapshot:   snap,
		Milestones: timeline.Milestones,
	}

	_ = json.NewEncoder(w).Encode(status)
}
