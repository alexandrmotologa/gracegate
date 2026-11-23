package test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alexandrmotologa/gracegate/pkg/lifecycle"
	"github.com/alexandrmotologa/gracegate/pkg/proxy"
)

func TestZeroLossUnderSimulatedRollingUpdate(t *testing.T) {
	// 1. Setup Mock Backend with simulated database query delay
	backend := NewMockBackend(MockBackendOptions{
		ProcessingDelay: 15 * time.Millisecond,
		ReturnHeaders: map[string]string{
			"X-Backend-Service": "order-processor",
		},
	})
	defer backend.Close()

	// 2. Setup Gracegate Proxy
	tracker := proxy.NewAtomicTracker()
	fsm := lifecycle.NewFSM(lifecycle.Config{
		QuorumDuration: 150 * time.Millisecond,
		DrainTimeout:   2 * time.Second,
	}, tracker)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind test listener: %v", err)
	}
	defer ln.Close()

	fwdCfg := proxy.Config{
		ListenAddr:    ln.Addr().String(),
		TargetAddr:    backend.URL(),
		DrainTimeout:  2 * time.Second,
		StateProvider: fsm,
	}

	fwd, err := proxy.NewForwarder(fwdCfg, tracker)
	if err != nil {
		t.Fatalf("failed to create forwarder: %v", err)
	}

	proxyServer := &http.Server{Handler: fwd}
	go func() {
		_ = proxyServer.Serve(ln)
	}()
	defer func() { _ = proxyServer.Close() }()

	proxyURL := "http://" + ln.Addr().String()

	// 3. Concurrently emit requests across 30 workers
	const workers = 30
	const requestsPerWorker = 40
	const totalExpected = workers * requestsPerWorker

	var successCount atomic.Int64
	var droppedCount atomic.Int64
	var errorCount atomic.Int64

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 50,
			IdleConnTimeout:     10 * time.Second,
		},
		Timeout: 3 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(workers)

	// Trigger SIGTERM at 200ms
	go func() {
		time.Sleep(120 * time.Millisecond)
		fsm.TriggerDrain(lifecycle.ReasonSIGTERM)
	}()

	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < requestsPerWorker; j++ {
				endpoint := fmt.Sprintf("%s/api/checkout?item=%d", proxyURL, j)
				req, _ := http.NewRequest("POST", endpoint, nil)
				if workerID%2 == 0 {
					req.Header.Set("Content-Type", "application/grpc")
				}

				resp, err := client.Do(req)
				if err != nil {
					droppedCount.Add(1)
					continue
				}

				if resp.StatusCode == http.StatusOK {
					successCount.Add(1)
				} else {
					errorCount.Add(1)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()

				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	// Wait for FSM to terminate cleanly
	done := tracker.WaitForZeroInFlight(2 * time.Second)
	if !done {
		t.Fatalf("failed to drain all in-flight requests; remaining: %d", tracker.CurrentInFlight())
	}

	totalSuccess := successCount.Load()
	totalDropped := droppedCount.Load()
	totalErrors := errorCount.Load()

	if totalDropped > 0 {
		t.Fatalf("zero-loss invariant violated: %d dropped requests / connection resets", totalDropped)
	}
	if totalErrors > 0 {
		t.Fatalf("zero-loss invariant violated: %d 5xx errors returned during drain", totalErrors)
	}
	if totalSuccess != int64(totalExpected) {
		t.Fatalf("expected %d successful requests, got %d", totalExpected, totalSuccess)
	}

	tl := fsm.Timeline()
	if tl.QuorumDurationMs <= 0 {
		t.Fatalf("expected recorded QuorumDurationMs, got %d", tl.QuorumDurationMs)
	}
	if fsm.CurrentPhase() != lifecycle.PhaseTerminated {
		t.Fatalf("expected phase TERMINATED, got %s", fsm.CurrentPhase())
	}
}

func TestMemoryFootprintUnderLoad(t *testing.T) {
	runtime.GC()
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	tracker := proxy.NewAtomicTracker()
	const iterations = 50000

	for i := 0; i < iterations; i++ {
		done := tracker.RequestStarted(proxy.ProtocolHTTP1)
		done(200, 128, 256, 100*time.Microsecond)
	}

	runtime.ReadMemStats(&m2)
	allocMB := float64(m2.Alloc) / (1024 * 1024)

	// Enforce < 10MB memory requirement
	if allocMB > 10.0 {
		t.Fatalf("memory footprint exceeded budget: %.2f MB allocated (target < 10MB)", allocMB)
	}
}
