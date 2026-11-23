package socket

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseProcNetTCP(t *testing.T) {
	// Sample /proc/net/tcp fixture with 2 sockets:
	// Socket 1: Established (01), tx_queue=0, rx_queue=0
	// Socket 2: Established (01), tx_queue=1A (26 bytes), rx_queue=0
	sampleContent := `  sl  local_address rem_address   st tx_queue:rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:1F90 0100007F:C350 01 00000000:00000000 00:00000000 00000000  1000        0 12345 1 00000000 100 0 0 10 -1
   1: 0100007F:1F91 0100007F:C351 01 0000001A:00000000 00:00000000 00000000  1000        0 12346 1 00000000 100 0 0 10 -1
   2: 00000000:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 12340 1 00000000 100 0 0 10 -1
`

	tmpDir := t.TempDir()
	procFile := filepath.Join(tmpDir, "tcp")
	if err := os.WriteFile(procFile, []byte(sampleContent), 0644); err != nil {
		t.Fatalf("failed to write tmp proc file: %v", err)
	}

	stats, err := parseProcNetTCP(procFile)
	if err != nil {
		t.Fatalf("failed to parse proc file: %v", err)
	}

	if stats.ActiveSockets != 2 {
		t.Fatalf("expected 2 established sockets, got %d", stats.ActiveSockets)
	}
	if stats.TotalTxQueueBytes != 26 {
		t.Fatalf("expected 26 tx queue bytes, got %d", stats.TotalTxQueueBytes)
	}
	if stats.Quiescent {
		t.Fatalf("expected not quiescent when tx queue has bytes")
	}

	// Test Inspector with this path
	ins := NewInspector()
	ins.SetProcPath(procFile)

	qStats, err := ins.InspectQueues()
	if err != nil {
		t.Fatalf("inspect queues failed: %v", err)
	}
	if qStats.TotalTxQueueBytes != 26 {
		t.Fatalf("expected 26 tx queue bytes, got %d", qStats.TotalTxQueueBytes)
	}

	// Update to clean 0-byte queue
	cleanContent := `  sl  local_address rem_address   st tx_queue:rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:1F90 0100007F:C350 01 00000000:00000000 00:00000000 00000000  1000        0 12345 1 00000000 100 0 0 10 -1
`
	_ = os.WriteFile(procFile, []byte(cleanContent), 0644)

	quiescent, cleanStats := ins.WaitForL4Quiescence(100 * time.Millisecond)
	if !quiescent {
		t.Fatalf("expected quiescent to be true")
	}
	if cleanStats.TotalTxQueueBytes != 0 {
		t.Fatalf("expected 0 tx queue bytes, got %d", cleanStats.TotalTxQueueBytes)
	}
}

func TestInspectorFallback(t *testing.T) {
	ins := NewInspector()
	ins.SetProcPath("/non/existent/path/tcp")

	ins.ConnectionTracked()
	ins.ConnectionTracked()
	if ins.ActiveConnections() != 2 {
		t.Fatalf("expected 2 active connections, got %d", ins.ActiveConnections())
	}

	stats, err := ins.InspectQueues()
	if err != nil {
		t.Fatalf("expected no error in fallback mode, got %v", err)
	}
	if stats.ActiveSockets != 2 {
		t.Fatalf("expected 2 active sockets, got %d", stats.ActiveSockets)
	}
	if !stats.Quiescent {
		t.Fatalf("expected quiescent in fallback mode")
	}

	ins.ConnectionClosed()
	if ins.ActiveConnections() != 1 {
		t.Fatalf("expected 1 active connection after close, got %d", ins.ActiveConnections())
	}
}
