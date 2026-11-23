package lifecycle

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alexandrmotologa/gracegate/pkg/proxy"
)

// Phase represents the current stage in the pod draining lifecycle.
type Phase string

const (
	PhaseRunning          Phase = "RUNNING"
	PhaseDrainingQuorum   Phase = "DRAINING_QUORUM"
	PhaseDrainingInflight Phase = "DRAINING_INFLIGHT"
	PhaseTerminated       Phase = "TERMINATED"
)

// DrainReason describes what triggered the shutdown.
type DrainReason string

const (
	ReasonSIGTERM DrainReason = "SIGTERM"
	ReasonSIGINT  DrainReason = "SIGINT"
	ReasonPreStop DrainReason = "PRESTOP_HOOK"
	ReasonManual  DrainReason = "MANUAL_TRIGGER"
)

// TimelineMilestone records a single event during pod lifecycle.
type TimelineMilestone struct {
	Name        string    `json:"name"`
	Timestamp   time.Time `json:"timestamp"`
	OffsetMs    int64     `json:"offset_ms"`
	Description string    `json:"description"`
}

// DrainTimeline contains the exact sequence of events for SRE auditability.
type DrainTimeline struct {
	StartTime            time.Time           `json:"start_time"`
	DrainTriggerTime     time.Time           `json:"drain_trigger_time"`
	ReadinessFailTime    time.Time           `json:"readiness_fail_time"`
	LastPacketTime       time.Time           `json:"last_packet_time"`
	InflightZeroTime     time.Time           `json:"inflight_zero_time"`
	TerminationTime      time.Time           `json:"termination_time"`
	QuorumDurationMs     int64               `json:"quorum_duration_ms"`
	InflightDrainMs      int64               `json:"inflight_drain_ms"`
	TotalDrainMs         int64               `json:"total_drain_ms"`
	MeasuredIngressDelay int64               `json:"measured_ingress_delay_ms"`
	Milestones           []TimelineMilestone `json:"milestones"`
}

// Config specifies lifecycle and draining durations.
type Config struct {
	QuorumDuration time.Duration
	DrainTimeout   time.Duration
}

// FSM orchestrates the two-phase graceful draining finite state machine.
type FSM struct {
	config  Config
	tracker *proxy.AtomicTracker

	mu          sync.RWMutex
	current     Phase
	reason      DrainReason
	startedAt   time.Time
	triggeredAt time.Time

	timeline DrainTimeline
	notifyCh chan Phase

	lastIngressPacket atomic.Int64 // Unix nanoseconds
	drainInitiated    atomic.Bool
	closedConnections atomic.Bool
	listeners         []func(Phase)
}

// NewFSM initializes the lifecycle finite state machine.
func NewFSM(cfg Config, tracker *proxy.AtomicTracker) *FSM {
	if cfg.QuorumDuration <= 0 {
		cfg.QuorumDuration = 3 * time.Second
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 30 * time.Second
	}

	now := time.Now().UTC()
	fsm := &FSM{
		config:    cfg,
		tracker:   tracker,
		current:   PhaseRunning,
		startedAt: now,
		notifyCh:  make(chan Phase, 16),
		timeline: DrainTimeline{
			StartTime:  now,
			Milestones: make([]TimelineMilestone, 0, 16),
		},
	}

	fsm.recordMilestone("PROCESS_START", "gracegate initialized in RUNNING state")
	return fsm
}

// CurrentPhase returns the active lifecycle state.
func (f *FSM) CurrentPhase() Phase {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.current
}

// IsDraining returns true if the pod is in any drain phase.
func (f *FSM) IsDraining() bool {
	phase := f.CurrentPhase()
	return phase != PhaseRunning
}

// IsQuorumPhase returns true if gracegate is waiting for ingress propagation.
func (f *FSM) IsQuorumPhase() bool {
	return f.CurrentPhase() == PhaseDrainingQuorum
}

// IsCloseConnectionRequired returns true if responses must include Connection: close.
func (f *FSM) IsCloseConnectionRequired() bool {
	phase := f.CurrentPhase()
	return phase == PhaseDrainingInflight || phase == PhaseTerminated
}

// RecordIngressPacket marks the arrival of a packet during draining to compute propagation delay.
func (f *FSM) RecordIngressPacket() {
	now := time.Now().UTC()
	f.lastIngressPacket.Store(now.UnixNano())

	f.mu.Lock()
	f.timeline.LastPacketTime = now
	f.mu.Unlock()
}

// Subscribe registers a callback that triggers on phase transitions.
func (f *FSM) Subscribe(cb func(Phase)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listeners = append(f.listeners, cb)
}

// TriggerDrain initiates the graceful drain sequence asynchronously.
func (f *FSM) TriggerDrain(reason DrainReason) {
	if !f.drainInitiated.CompareAndSwap(false, true) {
		return
	}

	now := time.Now().UTC()
	f.mu.Lock()
	f.reason = reason
	f.triggeredAt = now
	f.current = PhaseDrainingQuorum
	f.timeline.DrainTriggerTime = now
	f.timeline.ReadinessFailTime = now
	f.recordMilestone("DRAIN_TRIGGERED", fmt.Sprintf("Triggered by %s; readiness set to 503", reason))
	f.mu.Unlock()

	f.broadcast(PhaseDrainingQuorum)

	go f.executeDrain(now)
}

// ExecuteDrainSync performs the drain synchronously with a timeout context.
func (f *FSM) ExecuteDrainSync(ctx context.Context, reason DrainReason) error {
	f.TriggerDrain(reason)

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if f.CurrentPhase() == PhaseTerminated {
				return nil
			}
		}
	}
}

func (f *FSM) executeDrain(startTime time.Time) {
	now := startTime

	// Phase 1: Wait for Ingress / Service Mesh propagation quorum
	quorumTimer := time.NewTimer(f.config.QuorumDuration)
	<-quorumTimer.C

	f.mu.Lock()
	quorumEnd := time.Now().UTC()
	f.timeline.QuorumDurationMs = quorumEnd.Sub(now).Milliseconds()

	lastPktNano := f.lastIngressPacket.Load()
	if lastPktNano > 0 {
		lastPktTime := time.Unix(0, lastPktNano).UTC()
		if lastPktTime.After(now) {
			f.timeline.MeasuredIngressDelay = lastPktTime.Sub(now).Milliseconds()
		}
	}

	f.current = PhaseDrainingInflight
	f.recordMilestone("QUORUM_COMPLETE", fmt.Sprintf("Quorum wait completed (%dms); initiating in-flight request flush", f.timeline.QuorumDurationMs))
	f.mu.Unlock()

	f.broadcast(PhaseDrainingInflight)

	// Phase 2: Wait for in-flight requests to flush cleanly
	inflightStart := time.Now().UTC()
	remainingBudget := f.config.DrainTimeout - f.config.QuorumDuration
	if remainingBudget < 1*time.Second {
		remainingBudget = 5 * time.Second
	}

	drained := f.tracker.WaitForZeroInFlight(remainingBudget)
	inflightEnd := time.Now().UTC()

	f.mu.Lock()
	f.timeline.InflightZeroTime = inflightEnd
	f.timeline.InflightDrainMs = inflightEnd.Sub(inflightStart).Milliseconds()
	f.timeline.TotalDrainMs = inflightEnd.Sub(now).Milliseconds()
	f.timeline.TerminationTime = inflightEnd
	f.current = PhaseTerminated

	var resultMsg string
	if drained {
		resultMsg = fmt.Sprintf("All in-flight requests flushed to 0 in %dms (Zero-Loss Verified)", f.timeline.InflightDrainMs)
	} else {
		resultMsg = fmt.Sprintf("Drain deadline reached with %d requests still active", f.tracker.CurrentInFlight())
	}
	f.recordMilestone("TERMINATED", resultMsg)
	f.mu.Unlock()

	f.broadcast(PhaseTerminated)
}

func (f *FSM) recordMilestone(name, description string) {
	now := time.Now().UTC()
	offset := now.Sub(f.timeline.StartTime).Milliseconds()
	f.timeline.Milestones = append(f.timeline.Milestones, TimelineMilestone{
		Name:        name,
		Timestamp:   now,
		OffsetMs:    offset,
		Description: description,
	})
}

func (f *FSM) broadcast(p Phase) {
	f.mu.RLock()
	callbacks := make([]func(Phase), len(f.listeners))
	copy(callbacks, f.listeners)
	f.mu.RUnlock()

	for _, cb := range callbacks {
		cb(p)
	}

	select {
	case f.notifyCh <- p:
	default:
	}
}

// Timeline returns an audit copy of the drain timeline.
func (f *FSM) Timeline() DrainTimeline {
	f.mu.RLock()
	defer f.mu.RUnlock()

	tl := f.timeline
	tl.Milestones = make([]TimelineMilestone, len(f.timeline.Milestones))
	copy(tl.Milestones, f.timeline.Milestones)
	return tl
}
