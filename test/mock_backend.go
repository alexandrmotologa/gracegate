package test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"
)

// MockBackendOptions defines behavior of the simulated backend service.
type MockBackendOptions struct {
	ProcessingDelay time.Duration
	ReturnHeaders   map[string]string
}

// MockBackend simulates a real microservice handling transactions with latency.
type MockBackend struct {
	server       *httptest.Server
	requestCount atomic.Int64
	options      MockBackendOptions
}

// NewMockBackend starts an in-process HTTP backend server.
func NewMockBackend(opts MockBackendOptions) *MockBackend {
	mb := &MockBackend{options: opts}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mb.requestCount.Add(1)

		if opts.ProcessingDelay > 0 {
			time.Sleep(opts.ProcessingDelay)
		}

		for k, v := range opts.ReturnHeaders {
			w.Header().Set(k, v)
		}

		if r.Header.Get("Content-Type") == "application/grpc" {
			w.Header().Set("Content-Type", "application/grpc")
			w.Header().Set("Trailer", "grpc-status, grpc-message")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("\x00\x00\x00\x00\x07payload"))
			w.Header().Set("grpc-status", "0")
			w.Header().Set("grpc-message", "ok")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Pod-Host", "mock-order-backend-pod-a")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"status":"processed","timestamp":%d}`, time.Now().UnixNano())
	})

	mb.server = httptest.NewServer(handler)
	return mb
}

// URL returns the mock backend address.
func (mb *MockBackend) URL() string {
	return mb.server.URL
}

// TotalRequests returns the number of requests processed by the backend.
func (mb *MockBackend) TotalRequests() int64 {
	return mb.requestCount.Load()
}

// Close terminates the mock backend.
func (mb *MockBackend) Close() {
	mb.server.Close()
}
