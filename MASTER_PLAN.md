# gracegate: Architecture and Engineering Master Plan
Kubernetes Zero-Loss Pod Draining, In-Flight Request Flusher and Connection Handover Proxy
Target: Ultra-Lightweight Go Sidecar Proxy (< 10MB RAM, sub-microsecond forwarding overhead) with Embedded Cockpit

---

## 1. Problem Statement and Scope

### 1.1 Connection Drops During Kubernetes Rolling Updates
In Kubernetes clusters, applications undergo rolling updates, Horizontal Pod Autoscaling (HPA) scale-downs, spot instance evictions, and node maintenance operations (`kubectl drain`). When a pod is targeted for termination, Kubernetes initiates two parallel asynchronous processes:

1. **Endpoint Deregistration**: Kubelet marks the pod as `Terminating`, and the EndpointSlice controller updates endpoints. This change propagates through `kube-proxy` (iptables or IPVS) and external ingress controllers (Envoy, NGINX Ingress, AWS ALB, Traefik). This network propagation introduces a delay typically between 2 and 8 seconds.
2. **Pod Process Termination**: Kubelet sends a `SIGTERM` signal to the container process. If the application handles `SIGTERM` by stopping its HTTP listener immediately, it refuses new connections while ingress controllers still route traffic to it, resulting in **502 Bad Gateway** and **Connection Refused** errors.
3. **Abrupt In-Flight Stream Severing**: Active HTTP/1.1 keep-alive connections, multiplexed HTTP/2 streams, and streaming gRPC calls are terminated mid-flight, resulting in dropped transactions, truncated payloads, and client `ECONNRESET` errors.
4. **The Flaw of Static Sleep Workarounds**: Inserting `sleep 15` in container `preStop` lifecycle hooks slows down CI/CD rollouts, fails to emit RFC-compliant HTTP/2 `GOAWAY` frames, and still risks cutting off requests if traffic duration exceeds the hardcoded sleep value.

### 1.2 The gracegate Solution
`gracegate` operates as an autonomous zero-loss sidecar proxy and pod lifecycle sentinel written in Go:

- **Low-Overhead Transparent Sidecar**: Runs in the same pod network namespace as the target application container (< 10MB RSS memory footprint, sub-microsecond forwarding overhead).
- **Two-Phase Draining State Machine**:
  - **Phase 1 (Network Quorum Wait)**: Upon receiving `SIGTERM` (or a `preStop` HTTP trigger), `gracegate` switches its local readiness probe `/gracegate/healthz` to HTTP 503 to signal ingress controllers to remove the pod from routing tables. The traffic listener remains fully operational to accept any packets still in transit.
  - **Phase 2 (Protocol-Specific Drain)**: Sends standard HTTP/2 and gRPC `GOAWAY` frames, sets `Connection: close` headers on HTTP/1.1 keep-alive responses, and waits for all active in-flight requests to complete.
- **Adaptive In-Flight Request Flusher**: Continuously tracks atomic counters of active requests, concluding the drain phase when the counter reaches zero or when a safety deadline expires.
- **Propagation Latency Profiler**: Measures cluster ingress and iptables/IPVS propagation delays, allowing platform teams to configure precise termination budgets.
- **Embedded Web Cockpit**: Serves a real-time diagnostics dashboard on port 8282 showing in-flight request gauges, waterfall timelines, and zero-error verification proofs.

---

## 2. System Architecture

```
+-------------------------------------------------------------+
|               Ingress / Service Mesh / Kube-Proxy           |
|        (Envoy / NGINX / AWS ALB / Istio / iptables)         |
+-------------------------------------------------------------+
                               |
               Inbound HTTP/1.1, HTTP/2, gRPC Traffic
                               v
+-------------------------------------------------------------+
|                    gracegate Sidecar Proxy                  |
|                                                             |
|  +---------------------+        +------------------------+  |
|  | Traffic Forwarder   |------->| In-Flight Request      |  |
|  | (Sub-Microsecond)   |        | Atomic Counter (N)     |  |
|  +---------------------+        +------------------------+  |
|            |                                 |              |
|            v                                 v              |
|  +---------------------+        +------------------------+  |
|  | Kube Lifecycle Hook |        | Protocol Drain Engine  |  |
|  | (SIGTERM / preStop) |        | - HTTP/2 GOAWAY Frames |  |
|  +---------------------+        | - Connection: close    |  |
|            |                    +------------------------+  |
|            v                                 |              |
|  +---------------------+                     v              |
|  | Health Check Server |        +------------------------+  |
|  | /healthz (Readiness)|        | Application Container  |  |
|  +---------------------+        | (Localhost Backend)    |  |
+------------------------------|------------------------------+
                               |
                               v
+-------------------------------------------------------------+
|                      Drain Transition                       |
|  1. Ingress detects /healthz failure -> Stops new routes    |
|  2. In-flight requests drain to 0                           |
|  3. gracegate forwards clean SIGTERM to Application         |
|  4. Pod exits with zero dropped requests (100% 2xx/3xx/4xx) |
+-------------------------------------------------------------+
```

---

## 3. Mathematical and Algorithmic Formulation

### 3.1 Network Propagation and Drain Horizon
Let $T_{\text{sig}}$ denote the timestamp when Kubelet issues `SIGTERM`. The network propagation delay across cluster ingress endpoints follows a distribution $T_{\text{prop}} \sim \mathcal{D}(\mu_{\text{prop}}, \sigma_{\text{prop}})$.

The total active request lifetime of in-flight request $i$ is denoted by $\tau_i$. To guarantee zero dropped connections, the minimum drain wait duration $T_{\text{drain}}$ must satisfy:

$$T_{\text{drain}} \ge \max\left(T_{\text{prop}}\right) + \max_{i \in \text{In-Flight}}(\tau_i)$$

`gracegate` dynamically measures $\max(T_{\text{prop}})$ by tracking the timestamp of the last newly routed request received after `/healthz` transitions to failing.

### 3.2 Two-Step HTTP/2 and gRPC GOAWAY Protocol
For HTTP/2 and gRPC multiplexed streams, abruptly closing the TCP connection causes client-side stream truncation. `gracegate` enforces RFC 7540 graceful shutdown:

1. **Step 1 (Advisory Frame)**: Transmits a `GOAWAY` frame with stream identifier $2^{31} - 1$ (`0x7FFFFFFF`). This advises the client to cease creating new streams on this connection while continuing current work.
2. **Step 2 (Final Frame)**: After a propagation delay $\delta = \hat{\mu}_{\text{prop}}$, transmits a second `GOAWAY` frame containing the exact identifier $S_{\text{last}}$ of the highest stream processed by the application.
3. TCP socket is closed only when all active streams $\le S_{\text{last}}$ receive complete response frames.

### 3.3 Zero-Loss Invariant
For a pod undergoing termination with $M$ total requests processed during the termination window $[T_{\text{sig}}, T_{\text{exit}}]$:

$$\sum_{j=1}^M \mathbf{1}\left(\text{ExitCode}(j) \in \{502, 503, 504, \text{ECONNRESET}\}\right) = 0$$

$$\text{Dropped Request Rate} = 0.00\%$$

---

## 4. Enhanced Subsystems and Extensions

### 4.1 Transparent Traffic Proxy (`pkg/proxy`)
- Low memory forwarder written in Go using zero-allocation buffer pools (`sync.Pool`).
- Supports transparent HTTP/1.1, HTTP/2, and gRPC connection proxying.
- Atomic counters track active in-flight requests and total connections per protocol.
- Latency tracking calculates exact forwarding overhead (p50, p95, p99).

### 4.2 Lifecycle and Signal Interceptor (`pkg/lifecycle`)
- Traps OS `SIGTERM` and `SIGINT` signals.
- Manages `/gracegate/healthz` (Readiness) and `/gracegate/livez` (Liveness) HTTP endpoints.
- Two-Phase Drain State Machine:
  - `StateRunning`: normal operation, health probes return 200 OK.
  - `StateDrainingQuorum`: readiness probe returns 503 Service Unavailable, proxy continues forwarding in-flight and incoming traffic while ingress deregisters pod.
  - `StateDrainingInflight`: protocol-specific teardown active (`Connection: close`, `GOAWAY`), waiting for active in-flight count to hit zero.
  - `StateTerminated`: sends graceful shutdown signal to backend application and exits.

### 4.3 Protocol Drain Engine (`pkg/protocol`)
- **HTTP/1.1 Engine**: Parses response headers, appends `Connection: close` during drain states, and gracefully flushes the socket.
- **HTTP/2 & gRPC Engine**: Constructs raw HTTP/2 frames (`GOAWAY`, `PING`, `WINDOW_UPDATE`). Emits RFC 7540 two-step `GOAWAY` sequences and drains gRPC channels with status codes and trailers preserved.
- **EWMA Propagation Estimator**: Exponentially weighted moving average of ingress propagation latency to adaptively optimize drain timers.

### 4.4 Kubernetes Manifest Generator and Injector (`pkg/inject`)
- CLI command `gracegate inject` parses existing Kubernetes Deployment or Pod YAML files.
- Injects `gracegate` sidecar container with:
  - Loopback port routing configuration.
  - Container `preStop` HTTP hook pointing to `http://localhost:9090/gracegate/drain`.
  - Tuned `terminationGracePeriodSeconds`.
  - Zero-drift resource requests (10m CPU, 16Mi RAM).

### 4.5 Propagation Latency Auditor (`pkg/audit`)
- CLI command `gracegate audit` measures cluster ingress propagation delay.
- Emits test probes against endpoints during rolling updates to detect the exact moment routing tables converge.
- Exports structured JSON audit reports and summary charts.

### 4.6 Embedded Diagnostics Cockpit (`pkg/cockpit`)
- Runs a web server on port 8282 with Server-Sent Events (SSE) telemetry.
- Real-time in-flight request gauge with instant status updates.
- Visual waterfall timeline documenting each milestone in milliseconds:
  - `SIGTERM Captured`
  - `Readiness Fails (HTTP 503)`
  - `Last Ingress Request Received`
  - `In-Flight Requests Flushed`
  - `Pod Exit Clean`
- Zero-loss verification badge validating 100% successful response codes.

### 4.7 Prometheus Metrics Exporter (`pkg/metrics`)
- Exposes standard Prometheus `/metrics` endpoint:
  - `gracegate_inflight_requests`: current gauge of active requests.
  - `gracegate_requests_total`: counter labeled by protocol and status code.
  - `gracegate_drain_duration_seconds`: histogram of pod drain duration.
  - `gracegate_drain_phase`: current state machine phase (0=running, 1=quorum, 2=inflight, 3=done).

---

## 5. Command-Line Interface (CLI)

```bash
# Run gracegate as a sidecar proxy wrapping an application container
gracegate proxy \
  --listen ":8080" \
  --target "127.0.0.1:8081" \
  --health-port 9090 \
  --drain-timeout 30s \
  --cockpit-port 8282

# Audit cluster ingress propagation delay during a deployment rollout
gracegate audit \
  --target "http://order-service.default.svc.cluster.local:8080/health" \
  --probes 100 \
  --interval 50ms \
  --output ./propagation_audit.json

# Test graceful shutdown locally against a simulated HTTP/1.1 and HTTP/2 backend
gracegate test \
  --concurrency 100 \
  --duration 5s \
  --simulate-sigterm-at 2s

# Inject gracegate sidecar into an existing Kubernetes deployment manifest
gracegate inject \
  --file deployment.yaml \
  --app-port 8080 \
  --output deployment.patched.yaml

# Launch standalone Cockpit UI
gracegate cockpit --port 8282
```

---

## 6. Directory Structure

```
B:\workgit\gracegate\
├── cmd/
│   └── gracegate/
│       └── main.go                  # Main entry point and CLI commands
├── pkg/
│   ├── proxy/
│   │   ├── forwarder.go             # High-throughput TCP/HTTP stream forwarder
│   │   ├── counter.go               # Atomic in-flight request tracker
│   │   └── buffer_pool.go           # Zero-allocation memory pool
│   ├── lifecycle/
│   │   ├── signals.go               # SIGTERM and OS signal listener
│   │   ├── health.go                # K8s readiness probe controller
│   │   └── fsm.go                   # Two-phase drain finite state machine
│   ├── protocol/
│   │   ├── http1.go                 # HTTP/1.1 Connection: close injector
│   │   ├── http2.go                 # HTTP/2 binary GOAWAY frame builder
│   │   ├── grpc.go                  # gRPC status and channel drainer
│   │   └── estimator.go             # EWMA propagation delay estimator
│   ├── inject/
│   │   └── injector.go              # Kubernetes YAML manifest sidecar injector
│   ├── audit/
│   │   └── propagation.go           # Ingress propagation latency profiler
│   ├── metrics/
│   │   └── metrics.go               # Prometheus metrics exporter
│   └── cockpit/
│       ├── server.go                # Embedded HTTP server with SSE
│       └── web/
│           ├── index.html           # Draining timeline and live gauge UI
│           ├── app.js               # Visual waterfall and in-flight meter
│           └── style.css            # Dark tech clean UI styling
├── test/
│   ├── mock_backend.go              # Slow-responding HTTP/1.1, HTTP/2, and gRPC mock
│   └── drain_test.go                # Verification of zero dropped requests
├── go.mod
├── go.sum
├── README.md
├── LICENSE
└── MASTER_PLAN.md
```

---

## 7. Implementation Roadmap

### Phase 1: Core Scaffolding and High-Speed Forwarder
- Initialize Go module `github.com/alexandrmotologa/gracegate`.
- Implement zero-allocation buffer pool and atomic in-flight request counter.
- Build high-throughput stream forwarder for HTTP/1.1, HTTP/2, and gRPC.
- Implement Prometheus metrics instrumentation.

### Phase 2: Lifecycle FSM and Signal Trapping
- Build OS signal trap (`SIGTERM`, `SIGINT`) and HTTP `preStop` drain trigger.
- Implement readiness/liveness HTTP controller (`/gracegate/healthz`, `/gracegate/livez`).
- Implement two-phase drain finite state machine with adaptive timeout.

### Phase 3: Protocol Drain Engines and EWMA Profiler
- Build HTTP/1.1 `Connection: close` header injector.
- Build HTTP/2 raw frame parser and two-step `GOAWAY` frame emitter (RFC 7540).
- Build gRPC stream awareness and trailer preservation.
- Implement EWMA propagation latency estimator.

### Phase 4: Kubernetes Injector and Ingress Propagation Auditor
- Implement `gracegate inject` command for zero-friction YAML patching.
- Implement `gracegate audit` engine to probe real-time ingress convergence times.

### Phase 5: Embedded Diagnostics Cockpit (Port 8282)
- Build HTTP server with Server-Sent Events (SSE) telemetry.
- Create dark tech UI dashboard with live in-flight gauges, timeline waterfall, and zero-loss certificate proof.
- Verify responsiveness and layout integrity.

### Phase 6: Automated Verification, Benchmarks and Documentation
- Build test harness (`test/mock_backend.go` and `test/drain_test.go`).
- Verify zero dropped requests during heavy simulated rolling update with SIGTERM.
- Run memory and CPU benchmarks confirming < 10MB RSS and sub-microsecond overhead.
- Compile factual documentation and README adhering to humanizer guidelines.
