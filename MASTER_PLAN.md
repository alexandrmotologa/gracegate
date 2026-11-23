# gracegate: Architecture and Engineering Master Plan
Kubernetes Zero-Loss Pod Draining, In-Flight Request Flusher & Connection Handover Proxy
Target: Ultra-Lightweight Go Sidecar Proxy (< 10MB RAM, sub-microsecond forwarding) with Embedded Cockpit

---

## 1. Problem Statement and Scope

### 1.1 Connection Drops During Kubernetes Rolling Updates
In modern Kubernetes clusters, applications undergo frequent rolling updates, Horizontal Pod Autoscaling (HPA) scale-downs, spot instance evictions, and node maintenance (`kubectl drain`). When a pod is targeted for termination, Kubernetes initiates two parallel asynchronous processes:
1. **Endpoint Deregistration**: Kubelet marks the pod as `Terminating`, and the EndpointSlice controller updates endpoints. This change propagates through `kube-proxy` (iptables/IPVS) and external ingress controllers (Envoy, NGINX Ingress, AWS ALB, Traefik). This network propagation introduces an unavoidable **2 to 8 second delay**.
2. **Pod Process Termination**: Kubelet sends a `SIGTERM` signal to the container process. If the application handles `SIGTERM` by stopping its HTTP listener immediately, it refuses new connections while ingress controllers are still actively routing traffic to it, resulting in a flurry of **502 Bad Gateway** and **Connection Refused** errors.
3. **Abrupt In-Flight Stream Severing**: Long-running HTTP/1.1 keep-alive connections, multiplexed HTTP/2 streams, and streaming gRPC calls are abruptly killed, dropping financial transactions, file uploads, and active user sessions with `ECONNRESET`.
4. **The Flaw of Naive "Sleep Hacks"**: Standard practices attempt to mitigate this by inserting `sleep 15` in container `preStop` lifecycle hooks. This blindly slows down CI/CD deployments, fails to signal HTTP/2 `GOAWAY` frames, and still risks cutting off requests if traffic exceeds the hardcoded sleep duration.

### 1.2 The gracegate Solution
`gracegate` is an autonomous zero-loss sidecar proxy and pod lifecycle sentinel written in pure Go:
- **Zero-Overhead Transparent Sidecar**: Deployed as an ultra-lightweight (< 10MB RAM) sidecar proxy sitting between the ingress/mesh and the application container.
- **Two-Phase Draining State Machine**:
  - **Phase 1 (Network Quorum Wait)**: Upon receiving `SIGTERM`, `gracegate` fails its local readiness probe to signal ingress controllers to remove the pod from routing tables, while keeping its traffic listener fully open and serving in-flight and newly arriving packets.
  - **Phase 2 (Protocol-Specific Drain)**: Sends standard HTTP/2 and gRPC `GOAWAY` frames, sets `Connection: close` headers on HTTP/1.1 keep-alive responses, and waits for all active in-flight requests to complete cleanly.
- **Adaptive In-Flight Request Flusher**: Actively monitors the exact in-flight request counter, terminating the process only when the counter reaches zero or an adaptive safety deadline is reached.
- **Propagation Latency Profiler**: Measures exact cluster iptables/IPVS propagation delays, allowing platform teams to configure precise, minimal termination budgets.
- **Embedded Web Cockpit**: A visual cockpit on port 8282 showing real-time in-flight request gauges, pod draining timelines, and zero-error verification proofs.

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
|  4. Pod exits with ZERO dropped requests (100% 2xx/3xx/4xx) |
+-------------------------------------------------------------+
```

---

## 3. Mathematical & Algorithmic Formulation

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

## 4. Component Design and Subsystems

### 4.1 Transparent Traffic Proxy (`pkg/proxy`)
- Ultra-low memory proxy written in Go using `net.TCPListener` and zero-copy `io.CopyBuffer`.
- Intercepts inbound ports (e.g. `8080`) and forwards directly to application container on loopback (e.g. `127.0.0.1:8081`).
- Atomic In-Flight Counter: Increments on new request headers; decrements when the response stream finishes writing.

### 4.2 Lifecycle & Signal Interceptor (`pkg/lifecycle`)
- Traps OS `SIGTERM` and `SIGINT` signals.
- Manages the `/gracegate/healthz` endpoint: returns HTTP 200 during normal operation; instantly switches to HTTP 503 upon `SIGTERM` to fail K8s readiness checks.
- Enforces configurable maximum graceful termination timeout (`--drain-timeout 30s`).

### 4.3 Protocol Drainer (`pkg/protocol`)
- **HTTP/1.1 Drainer**: Injects `Connection: close` header into response headers during drain phase, preventing keep-alive connection reuse.
- **HTTP/2 & gRPC Drainer**: Low-level frame parser that intercepts HPACK headers and injects binary `GOAWAY` frames directly onto client sockets.

### 4.4 Embedded Diagnostics Cockpit (`pkg/cockpit`)
- Serves interactive dashboard on port 8282.
- Features:
  - **Live In-Flight Meter**: Real-time gauge showing currently active requests and drain progression.
  - **Termination Sequence Waterfall**: Visual timeline mapping exact milliseconds: `SIGTERM Received` -> `Readiness Failed` -> `Ingress Traffic Cessation` -> `In-Flight Emptied` -> `Clean Exit`.
  - **Cluster Propagation Latency Stats**: Calculates true ingress propagation time to optimize cluster-wide termination grace periods.

---

## 5. Command-Line Interface (CLI)

```bash
# Run gracegate as a sidecar proxy wrapping an application container
gracegate proxy \
  --listen ":8080" \
  --target "127.0.0.1:8081" \
  --health-port 9090 \
  --drain-timeout 30s \
  --cockpit

# Audit cluster ingress propagation delay during a deployment rollout
gracegate audit \
  --namespace production \
  --deployment order-service \
  --probes 50 \
  --output ./reports/propagation_latency.json

# Test graceful shutdown locally against a simulated HTTP/2 backend
gracegate test \
  --target "http://localhost:8081" \
  --concurrency 100 \
  --simulate-sigterm-at 2s

# Launch Cockpit UI directly
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
│   │   └── grpc.go                  # gRPC status and channel drainer
│   ├── audit/
│   │   └── propagation.go           # Ingress propagation latency profiler
│   └── cockpit/
│       ├── server.go                # Embedded HTTP server
│       └── web/
│           ├── index.html           # Draining timeline and live gauge UI
│           ├── app.js               # Visual waterfall and in-flight meter
│           └── style.css            # Dark tech clean UI styling
├── test/
│   ├── mock_backend.go              # Slow-responding HTTP/1.1 & HTTP/2 mock
│   └── drain_test.go                # Verification of zero dropped requests
├── go.mod
├── go.sum
├── README.md
└── MASTER_PLAN.md
```

---

## 7. Implementation Roadmap

### Phase 1: High-Speed Stream Forwarder & Atomic Counter
- Initialize Go module `github.com/mtlglabs/gracegate`.
- Implement zero-overhead TCP forwarder with atomic in-flight counter.
- Verify forwarding latency: strictly $< 0.1\text{ms}$ overhead.

### Phase 2: Kube Lifecycle FSM & Signal Interceptor
- Implement OS signal trapping and `/healthz` readiness controller.
- Implement Two-Phase Drain Finite State Machine.
- Build adaptive timeout termination coordinator.

### Phase 3: HTTP/2 & gRPC GOAWAY Protocol Engine
- Implement binary HTTP/2 `GOAWAY` frame generator.
- Implement HTTP/1.1 `Connection: close` header mutator.
- Test with simulated multiplexed client streams.

### Phase 4: Embedded Cockpit Dashboard
- Implement HTTP server on port 8282.
- Render interactive in-flight request gauge and termination timeline waterfall.
- Build live cluster ingress propagation delay calculator.

### Phase 5: Verification and Benchmarks
- Benchmark memory footprint: strictly $< 10\text{MB}$ RSS.
- Verify zero dropped connections across 1,000 parallel requests during live `SIGTERM`.
