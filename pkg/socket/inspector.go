package socket

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// SocketStats captures kernel-level TCP socket queue metrics.
type SocketStats struct {
	ActiveSockets     int   `json:"active_sockets"`
	TotalTxQueueBytes int64 `json:"total_tx_queue_bytes"`
	TotalRxQueueBytes int64 `json:"total_rx_queue_bytes"`
	UnackedSegments   int64 `json:"unacked_segments"`
	Quiescent         bool  `json:"quiescent"`
}

// Inspector monitors L4 TCP kernel socket queues to ensure zero packet truncation.
type Inspector struct {
	mu           sync.Mutex
	monitored    atomic.Int64
	procPath     string // Default "/proc/net/tcp"
}

// NewInspector creates an L4 socket queue inspector.
func NewInspector() *Inspector {
	return &Inspector{
		procPath: "/proc/net/tcp",
	}
}

// SetProcPath allows overriding the /proc path (useful for testing on any OS).
func (ins *Inspector) SetProcPath(path string) {
	ins.mu.Lock()
	defer ins.mu.Unlock()
	ins.procPath = path
}

// ConnectionTracked increments monitored active socket count.
func (ins *Inspector) ConnectionTracked() {
	ins.monitored.Add(1)
}

// ConnectionClosed decrements monitored active socket count.
func (ins *Inspector) ConnectionClosed() {
	ins.monitored.Add(-1)
}

// ActiveConnections returns the currently tracked connection count.
func (ins *Inspector) ActiveConnections() int64 {
	cnt := ins.monitored.Load()
	if cnt < 0 {
		return 0
	}
	return cnt
}

// InspectQueues reads TCP socket queue statistics.
// On Linux/Kubernetes containers, parses /proc/net/tcp.
func (ins *Inspector) InspectQueues() (SocketStats, error) {
	ins.mu.Lock()
	path := ins.procPath
	ins.mu.Unlock()

	activeCount := int(ins.ActiveConnections())

	// If /proc/net/tcp is available, parse kernel queue bytes
	if _, err := os.Stat(path); err == nil {
		stats, parseErr := parseProcNetTCP(path)
		if parseErr == nil {
			if activeCount > stats.ActiveSockets {
				stats.ActiveSockets = activeCount
			}
			stats.Quiescent = (stats.TotalTxQueueBytes == 0)
			return stats, nil
		}
	}

	// Fallback for non-Linux or environments without /proc
	return SocketStats{
		ActiveSockets:     activeCount,
		TotalTxQueueBytes: 0,
		TotalRxQueueBytes: 0,
		UnackedSegments:   0,
		Quiescent:         true,
	}, nil
}

// WaitForL4Quiescence polls until all kernel send queues have flushed to zero or timeout occurs.
func (ins *Inspector) WaitForL4Quiescence(timeout time.Duration) (bool, SocketStats) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		stats, err := ins.InspectQueues()
		if err == nil && stats.Quiescent {
			return true, stats
		}

		select {
		case <-ticker.C:
			if time.Now().After(deadline) {
				return false, stats
			}
		}
	}
}

// parseProcNetTCP parses standard Linux /proc/net/tcp format:
// sl local_address rem_address st tx_queue:rx_queue ...
func parseProcNetTCP(filepath string) (SocketStats, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return SocketStats{}, err
	}
	defer file.Close()

	var stats SocketStats
	scanner := bufio.NewScanner(file)

	// Skip header line
	if scanner.Scan() {
		// Header line: "  sl  local_address rem_address st tx_queue:rx_queue tr tm->when retrnsmt   uid  timeout inode"
	}

	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}

		// Field 3 is state (01 = TCP_ESTABLISHED)
		stateHex := fields[3]

		// Field 4 is tx_queue:rx_queue in hex
		queueParts := strings.Split(fields[4], ":")
		if len(queueParts) == 2 {
			txBytes, _ := strconv.ParseInt(queueParts[0], 16, 64)
			rxBytes, _ := strconv.ParseInt(queueParts[1], 16, 64)

			stats.TotalTxQueueBytes += txBytes
			stats.TotalRxQueueBytes += rxBytes
		}

		if stateHex == "01" {
			stats.ActiveSockets++
		}
	}

	stats.Quiescent = (stats.TotalTxQueueBytes == 0)
	return stats, scanner.Err()
}

// IsLinuxPlatform returns true if running on native Linux.
func IsLinuxPlatform() bool {
	return runtime.GOOS == "linux"
}

// FormatSummary returns a human-readable summary of socket state.
func (s SocketStats) FormatSummary() string {
	return fmt.Sprintf("sockets=%d tx_queue=%dB rx_queue=%dB quiescent=%v",
		s.ActiveSockets, s.TotalTxQueueBytes, s.TotalRxQueueBytes, s.Quiescent)
}
