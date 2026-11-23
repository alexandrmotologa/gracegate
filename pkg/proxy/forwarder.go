package proxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

// DrainStateProvider provides the current drain state.
type DrainStateProvider interface {
	IsDraining() bool
	IsQuorumPhase() bool
	IsCloseConnectionRequired() bool
	RecordIngressPacket()
}

// Config holds configuration parameters for the forwarder.
type Config struct {
	ListenAddr    string
	TargetAddr    string
	DrainTimeout  time.Duration
	EnableH2C     bool
	StateProvider DrainStateProvider
}

// Forwarder manages high-performance transparent proxying between ingress and the local app container.
type Forwarder struct {
	config     Config
	tracker    *AtomicTracker
	bufferPool *BufferPool
	server     *http.Server
	targetURL  *url.URL
	reverse    *httputil.ReverseProxy

	mu       sync.Mutex
	listener net.Listener
	stopChan chan struct{}
}

// NewForwarder creates a new high-throughput transparent forwarder.
func NewForwarder(cfg Config, tracker *AtomicTracker) (*Forwarder, error) {
	if tracker == nil {
		tracker = NewAtomicTracker()
	}

	target := cfg.TargetAddr
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		target = "http://" + target
	}
	parsedURL, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("invalid target address: %w", err)
	}

	pool := NewBufferPool(DefaultBufferSize)

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   500,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	// Upgrade transport to support h2c (cleartext HTTP/2) for gRPC and HTTP/2 local backends
	if err := http2.ConfigureTransport(transport); err != nil {
		// Log or retain fallback
	}

	fwd := &Forwarder{
		config:     cfg,
		tracker:    tracker,
		bufferPool: pool,
		targetURL:  parsedURL,
		stopChan:   make(chan struct{}),
	}

	revProxy := httputil.NewSingleHostReverseProxy(parsedURL)
	revProxy.Transport = transport
	revProxy.BufferPool = &bufferPoolAdapter{pool: pool}
	revProxy.ModifyResponse = fwd.modifyResponse
	revProxy.ErrorHandler = fwd.errorHandler

	fwd.reverse = revProxy

	return fwd, nil
}

// bufferPoolAdapter implements httputil.BufferPool using our BufferPool.
type bufferPoolAdapter struct {
	pool *BufferPool
}

func (b *bufferPoolAdapter) Get() []byte {
	bufPtr := b.pool.Get()
	return *bufPtr
}

func (b *bufferPoolAdapter) Put(buf []byte) {
	b.pool.Put(&buf)
}

// ServeHTTP handles inbound requests, protocol tagging, in-flight tracking, and header mutations.
func (f *Forwarder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	proto := classifyProtocol(r)

	if f.config.StateProvider != nil && f.config.StateProvider.IsDraining() {
		f.config.StateProvider.RecordIngressPacket()
	}

	if f.config.StateProvider != nil && f.config.StateProvider.IsCloseConnectionRequired() {
		w.Header().Set("Connection", "close")
	}

	done := f.tracker.RequestStarted(proto)
	trackingWriter := &responseRecorder{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
	}

	defer func() {
		dur := time.Since(startTime)
		done(trackingWriter.statusCode, r.ContentLength, trackingWriter.bytesWritten, dur)
	}()

	f.reverse.ServeHTTP(trackingWriter, r)
}

// modifyResponse handles protocol-specific drain headers.
func (f *Forwarder) modifyResponse(resp *http.Response) error {
	if f.config.StateProvider != nil && f.config.StateProvider.IsCloseConnectionRequired() {
		// Inject Connection: close to force client disconnect after active response completes
		resp.Header.Set("Connection", "close")
		resp.Close = true
	}
	return nil
}

// errorHandler handles backend errors during proxying.
func (f *Forwarder) errorHandler(w http.ResponseWriter, r *http.Request, err error) {
	statusCode := http.StatusBadGateway
	if strings.Contains(err.Error(), "context canceled") {
		statusCode = 499 // Client Closed Request
	}
	w.WriteHeader(statusCode)
	_, _ = fmt.Fprintf(w, "gracegate proxy error: %v\n", err)
}

// Start begins listening on the configured address.
func (f *Forwarder) Start() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	ln, err := net.Listen("tcp", f.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to bind forwarder on %s: %w", f.config.ListenAddr, err)
	}
	f.listener = ln

	h2Server := &http2.Server{}
	server := &http.Server{
		Addr: f.config.ListenAddr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f.ServeHTTP(w, r)
		}),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ConnState: func(conn net.Conn, state http.ConnState) {
			switch state {
			case http.StateNew:
				f.tracker.ClientConnected()
			case http.StateClosed, http.StateHijacked:
				f.tracker.ClientDisconnected()
			}
		},
	}

	if err := http2.ConfigureServer(server, h2Server); err != nil {
		// Keep server running with HTTP/1.1 fallback if h2 configuration fails
	}

	f.server = server

	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			// Proxy listener stopped
		}
	}()

	return nil
}

// Stop gracefully stops accepting new connections on the proxy listener.
func (f *Forwarder) Stop(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.server != nil {
		return f.server.Shutdown(ctx)
	}
	return nil
}

// Close immediately terminates the proxy listener.
func (f *Forwarder) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.server != nil {
		return f.server.Close()
	}
	return nil
}

// classifyProtocol inspects request headers and properties to classify the protocol.
func classifyProtocol(r *http.Request) Protocol {
	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "application/grpc") {
		return ProtocolGRPC
	}
	if r.ProtoMajor == 2 {
		return ProtocolHTTP2
	}
	return ProtocolHTTP1
}

// responseRecorder records written status codes and bytes.
type responseRecorder struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
	wroteHeader  bool
}

func (r *responseRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.statusCode = code
		r.wroteHeader = true
		r.ResponseWriter.WriteHeader(code)
	}
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytesWritten += int64(n)
	return n, err
}

// Flush supports streaming responses if the underlying writer is a Flusher.
func (r *responseRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
