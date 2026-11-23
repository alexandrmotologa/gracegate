package cockpit

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/alexandrmotologa/gracegate/pkg/lifecycle"
	"github.com/alexandrmotologa/gracegate/pkg/proxy"
)

//go:embed web/*
var embeddedWeb embed.FS

// Server hosts the embedded Cockpit web diagnostics dashboard.
type Server struct {
	port       int
	listenAddr string
	tracker    *proxy.AtomicTracker
	fsm        *lifecycle.FSM
	server     *http.Server
	mu         sync.Mutex
}

// NewServer creates a Cockpit server instance.
func NewServer(port int, tracker *proxy.AtomicTracker, fsm *lifecycle.FSM) *Server {
	if port <= 0 {
		port = 8282
	}

	cs := &Server{
		port:       port,
		listenAddr: fmt.Sprintf(":%d", port),
		tracker:    tracker,
		fsm:        fsm,
	}

	mux := http.NewServeMux()

	// Sub-filesystem for embedded assets
	subFS, err := fs.Sub(embeddedWeb, "web")
	if err == nil {
		mux.Handle("/", http.FileServer(http.FS(subFS)))
	}

	mux.HandleFunc("/api/status", cs.handleStatus)
	mux.HandleFunc("/api/events", cs.handleSSE)
	mux.HandleFunc("/api/drain", cs.handleDrainTrigger)

	cs.server = &http.Server{
		Addr:              cs.listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return cs
}

// Start launches the Cockpit web server in the background.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to bind Cockpit dashboard on %s: %w", s.listenAddr, err)
	}

	go func() {
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			// Cockpit server stopped
		}
	}()

	return nil
}

// Stop gracefully shuts down the Cockpit server.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// StatusPayload defines the payload sent to Cockpit clients.
type StatusPayload struct {
	Phase      lifecycle.Phase              `json:"phase"`
	InFlight   int64                        `json:"in_flight"`
	Metrics    proxy.Snapshot               `json:"metrics"`
	Timeline   lifecycle.DrainTimeline      `json:"timeline"`
	Milestones []lifecycle.TimelineMilestone `json:"milestones"`
}

func (s *Server) getPayload() StatusPayload {
	var phase lifecycle.Phase = lifecycle.PhaseRunning
	var tl lifecycle.DrainTimeline
	var milestones []lifecycle.TimelineMilestone

	if s.fsm != nil {
		phase = s.fsm.CurrentPhase()
		tl = s.fsm.Timeline()
		milestones = tl.Milestones
	}

	var snap proxy.Snapshot
	var inFlight int64
	if s.tracker != nil {
		snap = s.tracker.Snapshot()
		inFlight = s.tracker.CurrentInFlight()
	}

	return StatusPayload{
		Phase:      phase,
		InFlight:   inFlight,
		Metrics:    snap,
		Timeline:   tl,
		Milestones: milestones,
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	payload := s.getPayload()
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *Server) handleDrainTrigger(w http.ResponseWriter, r *http.Request) {
	if s.fsm != nil {
		s.fsm.TriggerDrain(lifecycle.ReasonManual)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"DRAIN_INITIATED","phase":"DRAINING_QUORUM"}`))
		return
	}

	w.WriteHeader(http.StatusBadRequest)
	_, _ = w.Write([]byte(`{"error":"Lifecycle FSM not configured"}`))
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			payload := s.getPayload()
			data, err := json.Marshal(payload)
			if err != nil {
				continue
			}
			_, writeErr := fmt.Fprintf(w, "data: %s\n\n", data)
			if writeErr != nil {
				return
			}
			flusher.Flush()
		}
	}
}
