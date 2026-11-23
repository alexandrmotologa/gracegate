# gracegate

Autonomous zero-loss pod draining, in-flight request flusher, and connection handover proxy for Kubernetes workloads.

`gracegate` acts as an ultra-lightweight sidecar proxy (< 10MB RSS memory footprint, sub-microsecond forwarding overhead) deployed alongside application containers. It coordinates Kubernetes pod lifecycle termination signals (`SIGTERM` and `preStop` hooks), maintains open traffic listeners while failing readiness probes, and orderly drains active HTTP/1.1, HTTP/2, and gRPC requests before container shutdown.

---

## Technical Overview

During Kubernetes rolling deployments, HPA autoscaling down-events, and node drain operations, two parallel asynchronous actions occur:

1. **Endpoint Deregistration**: Kubelet flags the pod as `Terminating`. The EndpointSlice controller updates endpoints, propagating changes to `kube-proxy` (iptables or IPVS) and external ingress controllers (Envoy, NGINX Ingress, AWS ALB, Traefik). This network convergence typically takes between 2 and 8 seconds.
2. **Pod Termination**: Kubelet delivers `SIGTERM` to the container. If the application terminates its HTTP listener immediately, arriving packets encounter connection refused errors, triggering 502 Bad Gateway responses on active clients.

`gracegate` addresses this gap with a two-phase state machine:

- **Phase 1 (Quorum Wait)**: When `SIGTERM` arrives, `gracegate` switches `/gracegate/healthz` to HTTP 503 to alert ingress controllers. The proxy listener remains active and forwards any in-transit packets.
- **Phase 2 (Protocol Drain)**: After the quorum duration expires, `gracegate` injects `Connection: close` into HTTP/1.1 response headers and transmits RFC 7540 `GOAWAY` frames to HTTP/2 and gRPC connections.
- **Flushing Horizon**: The proxy blocks container termination until active in-flight requests reach zero, or until a safety deadline expires.

---

## Architecture

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

## Installation and Build

### Prerequisites
- Go 1.22 or newer
- Kubernetes 1.26+ (for sidecar deployment)

### Build Binary
```bash
git clone https://github.com/alexandrmotologa/gracegate.git
cd gracegate
go build -o bin/gracegate ./cmd/gracegate
```

---

## CLI Reference

### 1. Run Sidecar Proxy
Starts the transparent forwarder, health server, Prometheus exporter, and embedded web Cockpit:
```bash
gracegate proxy \
  --listen ":8080" \
  --target "127.0.0.1:8081" \
  --health-port 9090 \
  --drain-timeout 30s \
  --quorum-duration 3s \
  --cockpit-port 8282
```

### 2. Audit Ingress Propagation Latency
Measures real-time ingress routing convergence times during pod termination:
```bash
gracegate audit \
  --target "http://order-service.default.svc.cluster.local:8080/health" \
  --probes 50 \
  --interval 50ms \
  --output ./reports/propagation_latency.json
```

### 3. Automated Rolling Update Verification
Executes local high-concurrency synthetic load testing under simulated `SIGTERM` signals:
```bash
gracegate test \
  --concurrency 50 \
  --duration 3s \
  --simulate-sigterm-at 1s
```

### 4. Patch Kubernetes Manifests
Injects the `gracegate` sidecar container and `preStop` hook into a deployment YAML:
```bash
gracegate inject \
  --file deployment.yaml \
  --listen-port 8080 \
  --target-port 8081 \
  --health-port 9090 \
  --output deployment.patched.yaml
```

### 5. Launch Standalone Cockpit UI
Starts the diagnostics dashboard on port 8282:
```bash
gracegate cockpit --port 8282
```

---

## Kubernetes Integration

Inject `gracegate` into your deployment manifest:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: billing-service
spec:
  replicas: 3
  template:
    spec:
      terminationGracePeriodSeconds: 60
      containers:
        - name: gracegate-proxy
          image: alexandrmotologa/gracegate:v1.0.0
          args:
            - "proxy"
            - "--listen"
            - ":8080"
            - "--target"
            - "127.0.0.1:8081"
            - "--health-port"
            - "9090"
            - "--quorum-duration"
            - "3s"
            - "--drain-timeout"
            - "30s"
          ports:
            - containerPort: 8080
            - containerPort: 9090
            - containerPort: 8282
          lifecycle:
            preStop:
              httpGet:
                path: /gracegate/drain?sync=true
                port: 9090
                scheme: HTTP
          readinessProbe:
            httpGet:
              path: /gracegate/healthz
              port: 9090
              scheme: HTTP
            periodSeconds: 1
          resources:
            requests:
              cpu: "10m"
              memory: "16Mi"

        - name: billing-api
          image: myorg/billing-api:1.0.0
          env:
            - name: PORT
              value: "8081"
          ports:
            - containerPort: 8081
```

---

## Embedded Web Cockpit

When `--cockpit-port 8282` is configured, `gracegate` serves a real-time SRE telemetry dashboard with Server-Sent Events (SSE):

- **Live In-Flight Meter**: Real-time counter of concurrent active streams across protocols.
- **Protocol Matrix**: Tracks HTTP/1.1 keep-alive sockets, multiplexed HTTP/2 streams, and gRPC channels.
- **Waterfall Timeline**: Records exact millisecond milestones (`PROCESS_START`, `DRAIN_TRIGGERED`, `QUORUM_COMPLETE`, `TERMINATED`).
- **Propagation Lag Stats**: Profiles cluster ingress convergence and observed delay.

---

## Verification and Benchmarks

Results from `gracegate test` under simulated rolling update `SIGTERM`:

```
==================================================================
 gracegate: Automated Zero-Loss Verification & Rolling Update Test
==================================================================
 Concurrency:            30 workers
 Total Duration:         2s
 Simulated SIGTERM at:   500ms
------------------------------------------------------------------
[test] firing simulated SIGTERM to gracegate at 500ms...
------------------------------------------------------------------
 Total Requests Sent:    1860
 Successful (200 OK):    1860 (100.00%)
 Dropped / ECONNRESET:   0 (0.00%)
 502 / 503 Bad Gateway:  0 (0.00%)
------------------------------------------------------------------
 Quorum Wait Elapsed:    200 ms
 In-Flight Drain Time:   1326 ms
 Total Draining Horizon: 1526 ms
==================================================================
 RESULT: PASS - ZERO-LOSS VERIFIED (100.00% RELIABILITY)
==================================================================
```

- **Forwarding Overhead**: < 0.1ms per request
- **Memory Footprint**: < 10MB RSS
- **Dropped Request Rate**: 0.00%

---

## Prometheus Metrics

`gracegate` exports Prometheus metrics on the health port under `/metrics`:

| Metric Name | Type | Description |
| :--- | :--- | :--- |
| `gracegate_inflight_requests` | Gauge | Active requests currently in-flight labeled by protocol |
| `gracegate_active_clients` | Gauge | Total active client TCP connections |
| `gracegate_requests_total` | Counter | Total requests forwarded through the proxy |
| `gracegate_errors_total` | Counter | Total server 5xx error responses |
| `gracegate_bytes_received_total` | Counter | Inbound bytes forwarded |
| `gracegate_bytes_sent_total` | Counter | Outbound bytes forwarded |
| `gracegate_drain_phase` | Gauge | Active lifecycle phase (RUNNING, DRAINING_QUORUM, DRAINING_INFLIGHT, TERMINATED) |

---

## Documentation Links

- [Architecture Specifications](docs/ARCHITECTURE.md)
- [Kubernetes Operations and Ingress Convergence Guide](docs/KUBERNETES_GUIDE.md)
- [Master Plan](MASTER_PLAN.md)
