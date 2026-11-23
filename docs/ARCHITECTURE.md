# gracegate Architecture and Technical Specifications

This document outlines the internal architecture, mathematical formulations, and protocol mechanics implemented by `gracegate`.

---

## 1. High-Level System Topology

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

## 2. Finite State Machine (FSM)

`gracegate` manages pod termination via a deterministic, two-phase finite state machine:

```
[PhaseRunning] 
       │
       │ (SIGTERM received OR /gracegate/drain invoked)
       ▼
[PhaseDrainingQuorum] ──────► Readiness probe (/gracegate/healthz) immediately fails with HTTP 503
       │                      Traffic proxy remains open to forward transit packets
       │                      Wait duration: Max(T_prop) or configured --quorum-duration (e.g. 3s)
       ▼
[PhaseDrainingInflight] ────► Protocol-specific teardown begins:
       │                      - HTTP/1.1: inject Connection: close header
       │                      - HTTP/2 & gRPC: emit RFC 7540 dual-stage GOAWAY frames
       │                      Wait until atomic in-flight counter hits 0
       ▼
[PhaseTerminated] ──────────► Zero in-flight requests remain. Forward SIGTERM to app container and exit
```

### State Definitions

1. **`PhaseRunning`**:
   - The pod is actively serving production traffic.
   - Readiness probe `/gracegate/healthz` returns HTTP 200 OK.
   - Inbound requests on port 8080 are forwarded to local backend port 8081.

2. **`PhaseDrainingQuorum`**:
   - Triggered immediately when Kubelet sends `SIGTERM` or when the container `preStop` hook triggers `/gracegate/drain`.
   - Readiness probe `/gracegate/healthz` switches to HTTP 503 Service Unavailable, alerting ingress controllers and EndpointSlice controllers to remove the pod from endpoint lists.
   - The traffic forwarder stays active, processing in-flight requests and any newly arriving packets that were already in flight before network convergence.

3. **`PhaseDrainingInflight`**:
   - Ingress propagation quorum duration expires (default: 3s or dynamically computed EWMA).
   - Responses for HTTP/1.1 are injected with `Connection: close` to prevent client reuse of keep-alive sockets.
   - HTTP/2 and gRPC streams receive RFC 7540 `GOAWAY` frames.
   - The proxy blocks until the active in-flight request counter reaches zero, or until `--drain-timeout` (default: 30s) is reached.

4. **`PhaseTerminated`**:
   - Zero active in-flight requests remain.
   - The forwarder shuts down cleanly and exits with status code 0.

---

## 3. Mathematical Formulation of Propagation Horizon

Let $T_{\text{sig}}$ denote the timestamp when Kubelet issues `SIGTERM`. The network propagation delay across cluster ingress endpoints follows a distribution $T_{\text{prop}} \sim \mathcal{D}(\mu_{\text{prop}}, \sigma_{\text{prop}})$.

The total active request lifetime of in-flight request $i$ is denoted by $\tau_i$. To guarantee zero dropped connections, the minimum drain wait duration $T_{\text{drain}}$ must satisfy:

$$T_{\text{drain}} \ge \max\left(T_{\text{prop}}\right) + \max_{i \in \text{In-Flight}}(\tau_i)$$

`gracegate` dynamically measures $\max(T_{\text{prop}})$ by tracking the timestamp of the last newly routed request received after `/gracegate/healthz` transitions to failing.

### Exponentially Weighted Moving Average (EWMA)

For clusters with variable load, `gracegate` maintains an EWMA of observed propagation delays:

$$\hat{\mu}_{\text{prop}}^{(t)} = \alpha \cdot T_{\text{prop}}^{(t)} + (1 - \alpha) \cdot \hat{\mu}_{\text{prop}}^{(t-1)}$$

The recommended quorum duration is calculated with a three-sigma safety margin:

$$T_{\text{recommended}} = \hat{\mu}_{\text{prop}} + 3 \cdot \hat{\sigma}_{\text{prop}}$$

---

## 4. RFC 7540 Dual-Stage HTTP/2 and gRPC GOAWAY Protocol

Abruptly tearing down a multiplexed TCP connection drops active streams with `ECONNRESET`. `gracegate` implements the two-step `GOAWAY` sequence defined in RFC 7540 Section 6.8:

1. **Step 1 (Advisory Frame)**:
   - Frame type: `0x7` (`GOAWAY`)
   - Stream identifier: `0x0` (connection-level frame)
   - Last-Stream-ID: `2^31 - 1` (`0x7FFFFFFF`)
   - Error code: `0x0` (`NO_ERROR`)
   - Function: Notifies the client that the connection is shutting down and that no new streams should be initiated, while allowing existing in-flight streams to continue uninterrupted.

2. **Step 2 (Final Frame)**:
   - Transmitted after the propagation delay $\delta = \hat{\mu}_{\text{prop}}$.
   - Last-Stream-ID: exact identifier $S_{\text{last}}$ of the highest stream handled by the backend.
   - Function: Finalizes the stream boundary. The TCP socket is closed only after all streams $\le S_{\text{last}}$ complete their response cycles.
