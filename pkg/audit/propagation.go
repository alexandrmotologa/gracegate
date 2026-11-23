package audit

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

// AuditConfig specifies probe parameters for ingress propagation latency profiling.
type AuditConfig struct {
	TargetURL    string        `json:"target_url"`
	ProbeCount   int           `json:"probe_count"`
	Interval     time.Duration `json:"interval"`
	Timeout      time.Duration `json:"timeout"`
	OutputFile   string        `json:"output_file"`
	DrainTrigger string        `json:"drain_trigger_url"`
}

// ProbeResult records the outcome of a single probe request.
type ProbeResult struct {
	Sequence     int           `json:"sequence"`
	Timestamp    time.Time     `json:"timestamp"`
	OffsetMs     int64         `json:"offset_ms"`
	StatusCode   int           `json:"status_code"`
	Latency      time.Duration `json:"latency"`
	LatencyMs    float64       `json:"latency_ms"`
	Error        string        `json:"error,omitempty"`
	PodHeader    string        `json:"pod_header,omitempty"`
	IsDuringDrain bool         `json:"is_during_drain"`
}

// AuditReport summarizes the empirical propagation results.
type AuditReport struct {
	TargetURL           string        `json:"target_url"`
	AuditStartTime      time.Time     `json:"audit_start_time"`
	DrainTriggeredTime  time.Time     `json:"drain_triggered_time"`
	TotalProbes         int           `json:"total_probes"`
	SuccessfulProbes    int           `json:"successful_probes"`
	FailedProbes        int           `json:"failed_probes"`
	DroppedRequests     int           `json:"dropped_requests"`
	ZeroLossMaintained  bool          `json:"zero_loss_maintained"`
	PropagationDelayMs  int64         `json:"propagation_delay_ms"`
	MinLatencyMs        float64       `json:"min_latency_ms"`
	MaxLatencyMs        float64       `json:"max_latency_ms"`
	AvgLatencyMs        float64       `json:"avg_latency_ms"`
	RecommendedWaitMs   int64         `json:"recommended_quorum_wait_ms"`
	Probes              []ProbeResult `json:"probes"`
}

// Auditor conducts propagation delay profiling.
type Auditor struct {
	config AuditConfig
	client *http.Client
}

// NewAuditor creates an ingress propagation auditor.
func NewAuditor(cfg AuditConfig) *Auditor {
	if cfg.ProbeCount <= 0 {
		cfg.ProbeCount = 50
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 50 * time.Millisecond
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   cfg.Timeout,
			KeepAlive: 0, // Disable keep-alive for auditor to test fresh DNS/iptables routing per probe
		}).DialContext,
		DisableKeepAlives: true,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	return &Auditor{
		config: cfg,
		client: &http.Client{
			Transport: transport,
			Timeout:   cfg.Timeout,
		},
	}
}

// Run executes the audit sequence.
func (a *Auditor) Run(ctx context.Context) (*AuditReport, error) {
	report := &AuditReport{
		TargetURL:      a.config.TargetURL,
		AuditStartTime: time.Now().UTC(),
		Probes:         make([]ProbeResult, 0, a.config.ProbeCount),
	}

	var mu sync.Mutex
	startTime := time.Now()
	var drainTriggeredTime time.Time
	drainTriggered := false

	ticker := time.NewTicker(a.config.Interval)
	defer ticker.Stop()

	// Trigger drain midway through probes if drain trigger URL is specified
	triggerAtProbe := a.config.ProbeCount / 3

	for seq := 1; seq <= a.config.ProbeCount; seq++ {
		select {
		case <-ctx.Done():
			return report, ctx.Err()
		case <-ticker.C:
		}

		if seq == triggerAtProbe && a.config.DrainTrigger != "" && !drainTriggered {
			drainTriggered = true
			drainTriggeredTime = time.Now().UTC()
			report.DrainTriggeredTime = drainTriggeredTime
			go func() {
				req, _ := http.NewRequestWithContext(context.Background(), "GET", a.config.DrainTrigger, nil)
				resp, err := a.client.Do(req)
				if err == nil {
					_ = resp.Body.Close()
				}
			}()
		}

		probeStart := time.Now()
		offsetMs := probeStart.Sub(startTime).Milliseconds()

		req, err := http.NewRequestWithContext(ctx, "GET", a.config.TargetURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create probe request: %w", err)
		}

		resp, err := a.client.Do(req)
		dur := time.Since(probeStart)
		durMs := float64(dur.Nanoseconds()) / 1e6

		res := ProbeResult{
			Sequence:      seq,
			Timestamp:     probeStart.UTC(),
			OffsetMs:      offsetMs,
			Latency:       dur,
			LatencyMs:     durMs,
			IsDuringDrain: drainTriggered,
		}

		if err != nil {
			res.Error = err.Error()
			res.StatusCode = 0
			mu.Lock()
			report.FailedProbes++
			report.DroppedRequests++
			mu.Unlock()
		} else {
			res.StatusCode = resp.StatusCode
			res.PodHeader = resp.Header.Get("X-Pod-Name")
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()

			mu.Lock()
			if resp.StatusCode >= 500 {
				report.FailedProbes++
				report.DroppedRequests++
			} else {
				report.SuccessfulProbes++
			}
			mu.Unlock()
		}

		mu.Lock()
		report.Probes = append(report.Probes, res)
		mu.Unlock()
	}

	report.TotalProbes = len(report.Probes)
	report.ZeroLossMaintained = (report.DroppedRequests == 0)

	// Calculate latency stats
	var sumLatency float64
	minLat := 1e9
	maxLat := 0.0

	for _, p := range report.Probes {
		if p.LatencyMs > 0 {
			sumLatency += p.LatencyMs
			if p.LatencyMs < minLat {
				minLat = p.LatencyMs
			}
			if p.LatencyMs > maxLat {
				maxLat = p.LatencyMs
			}
		}
	}

	if report.TotalProbes > 0 {
		report.AvgLatencyMs = sumLatency / float64(report.TotalProbes)
		report.MinLatencyMs = minLat
		report.MaxLatencyMs = maxLat
	}

	// Calculate observed propagation delay if drain was triggered
	if !drainTriggeredTime.IsZero() {
		var lastProbeTime time.Time
		for _, p := range report.Probes {
			if p.IsDuringDrain && p.StatusCode < 500 && p.StatusCode > 0 {
				lastProbeTime = p.Timestamp
			}
		}
		if !lastProbeTime.IsZero() && lastProbeTime.After(drainTriggeredTime) {
			report.PropagationDelayMs = lastProbeTime.Sub(drainTriggeredTime).Milliseconds()
		}
	}

	// Recommendation: observed delay + 3000ms safety buffer
	if report.PropagationDelayMs > 0 {
		report.RecommendedWaitMs = report.PropagationDelayMs + 3000
	} else {
		report.RecommendedWaitMs = 5000
	}

	if a.config.OutputFile != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			_ = os.WriteFile(a.config.OutputFile, data, 0644)
		}
	}

	return report, nil
}
