package protocol

import (
	"math"
	"sort"
	"sync"
	"time"
)

// EstimatorConfig configures the EWMA propagation latency estimator.
type EstimatorConfig struct {
	Alpha       float64       // Smoothing factor (0.0 to 1.0), default 0.2
	MinWait     time.Duration // Minimum quorum wait, default 1s
	MaxWait     time.Duration // Maximum quorum wait, default 15s
	SampleLimit int           // Maximum history samples, default 100
}

// EstimatorStats summarizes empirical network propagation latency across the cluster.
type EstimatorStats struct {
	Count               int           `json:"sample_count"`
	EWMAMs              float64       `json:"ewma_ms"`
	StdDevMs            float64       `json:"std_dev_ms"`
	MinMs               int64         `json:"min_ms"`
	MaxMs               int64         `json:"max_ms"`
	P50Ms               int64         `json:"p50_ms"`
	P95Ms               int64         `json:"p95_ms"`
	P99Ms               int64         `json:"p99_ms"`
	RecommendedQuorumMs int64         `json:"recommended_quorum_ms"`
	RecommendedQuorum   time.Duration `json:"recommended_quorum_duration"`
}

// EWMAEstimator computes cluster ingress/iptables propagation latency using EWMA.
type EWMAEstimator struct {
	cfg     EstimatorConfig
	mu      sync.RWMutex
	samples []int64 // Latency samples in milliseconds
	ewma    float64
	variance float64
}

// NewEWMAEstimator creates an estimator with sensible defaults.
func NewEWMAEstimator(cfg EstimatorConfig) *EWMAEstimator {
	if cfg.Alpha <= 0 || cfg.Alpha > 1.0 {
		cfg.Alpha = 0.2
	}
	if cfg.MinWait <= 0 {
		cfg.MinWait = 1 * time.Second
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 15 * time.Second
	}
	if cfg.SampleLimit <= 0 {
		cfg.SampleLimit = 100
	}

	return &EWMAEstimator{
		cfg:     cfg,
		samples: make([]int64, 0, cfg.SampleLimit),
	}
}

// RecordObservation records a measured propagation delay for a pod termination event.
func (e *EWMAEstimator) RecordObservation(d time.Duration) {
	ms := d.Milliseconds()
	if ms < 0 {
		ms = 0
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.samples = append(e.samples, ms)
	if len(e.samples) > e.cfg.SampleLimit {
		e.samples = e.samples[1:]
	}

	if len(e.samples) == 1 {
		e.ewma = float64(ms)
		e.variance = 0
		return
	}

	alpha := e.cfg.Alpha
	diff := float64(ms) - e.ewma
	e.ewma = e.ewma + alpha*diff
	e.variance = (1.0 - alpha) * (e.variance + alpha*diff*diff)
}

// Stats returns statistical metrics and recommended quorum wait duration.
func (e *EWMAEstimator) Stats() EstimatorStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	n := len(e.samples)
	if n == 0 {
		minWaitMs := e.cfg.MinWait.Milliseconds()
		return EstimatorStats{
			Count:               0,
			RecommendedQuorumMs: minWaitMs,
			RecommendedQuorum:   e.cfg.MinWait,
		}
	}

	sorted := make([]int64, n)
	copy(sorted, e.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	minVal := sorted[0]
	maxVal := sorted[n-1]
	p50 := sorted[n*50/100]
	p95 := sorted[n*95/100]
	p99 := sorted[int(math.Min(float64(n*99/100), float64(n-1)))]

	stdDev := math.Sqrt(e.variance)

	// Recommended quorum wait: EWMA + 3 * StdDev, bounded by MinWait and MaxWait
	recommendedMs := int64(e.ewma + 3.0*stdDev)
	minMs := e.cfg.MinWait.Milliseconds()
	maxMs := e.cfg.MaxWait.Milliseconds()

	if recommendedMs < minMs {
		recommendedMs = minMs
	}
	if recommendedMs > maxMs {
		recommendedMs = maxMs
	}

	return EstimatorStats{
		Count:               n,
		EWMAMs:              math.Round(e.ewma*100) / 100,
		StdDevMs:            math.Round(stdDev*100) / 100,
		MinMs:               minVal,
		MaxMs:               maxVal,
		P50Ms:               p50,
		P95Ms:               p95,
		P99Ms:               p99,
		RecommendedQuorumMs: recommendedMs,
		RecommendedQuorum:   time.Duration(recommendedMs) * time.Millisecond,
	}
}
