# Kubernetes Deployment and Operations Guide

This guide details how to integrate `gracegate` into Kubernetes deployments to achieve zero-loss pod lifecycle management during rolling updates, HPA scale-downs, and node evacuations.

---

## 1. Why Connection Drops Occur in Kubernetes

When a pod is deleted (e.g. during a deployment rollout or node drain), two independent asynchronous events occur in parallel:

1. **Endpoint Deregistration**:
   - The Kubelet marks the pod as `Terminating`.
   - The EndpointSlice controller detects the termination and removes the pod IP from EndpointSlices.
   - `kube-proxy` (iptables or IPVS) removes routing rules on every cluster worker node.
   - Ingress controllers (AWS Load Balancer Controller, NGINX Ingress, Envoy, Traefik) receive endpoint updates and remove the pod from their upstream pools.
   - **Observed Propagation Lag**: In practice, this convergence takes between 2 and 8 seconds.

2. **Pod Process Termination**:
   - Kubelet simultaneously sends `SIGTERM` to the container process.
   - If the container terminates its listener immediately, ingress controllers continue routing traffic to the dead socket for several seconds.
   - Clients experience `502 Bad Gateway`, `Connection Refused`, and `ECONNRESET` errors.

---

## 2. Why "sleep" in preStop Hooks Fails

A common workaround is adding a sleep command in the container `lifecycle.preStop` hook:

```yaml
# Flawed workaround
lifecycle:
  preStop:
    exec:
      command: ["/bin/sh", "-c", "sleep 15"]
```

This approach has four major weaknesses:
- **Blind Latency**: Slows down CI/CD rollouts by 15 seconds per pod even when zero requests are active.
- **No HTTP/2 Signal**: Fails to transmit HTTP/2 `GOAWAY` frames, causing multiplexed clients to see abrupt stream truncation.
- **Inadequate for Long Requests**: If a database query or transaction takes 16 seconds, the sleep expires and Kubelet terminates the process anyway.
- **Resource Waste**: Keeps compute resources tied up needlessly during rapid scale-down operations.

---

## 3. Automated Injection with gracegate

You can inject `gracegate` into existing deployment manifests automatically using the CLI:

```bash
gracegate inject \
  --file deployment.yaml \
  --listen-port 8080 \
  --target-port 8081 \
  --health-port 9090 \
  --output deployment.patched.yaml
```

---

## 4. Complete Production Deployment Example

Below is a complete Kubernetes Deployment manifest configured with `gracegate`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: payment-service
  namespace: production
  labels:
    app: payment-service
spec:
  replicas: 4
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 25%
      maxUnavailable: 0
  selector:
    matchLabels:
      app: payment-service
  template:
    metadata:
      labels:
        app: payment-service
    spec:
      terminationGracePeriodSeconds: 60
      containers:
        # gracegate Sidecar Sentinel (< 10MB RAM footprint)
        - name: gracegate-proxy
          image: alexandrmotologa/gracegate:v1.0.0
          imagePullPolicy: IfNotPresent
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
            - "--cockpit-port"
            - "8282"
          ports:
            - name: http-traffic
              containerPort: 8080
              protocol: TCP
            - name: health-probes
              containerPort: 9090
              protocol: TCP
            - name: cockpit-ui
              containerPort: 8282
              protocol: TCP
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
            initialDelaySeconds: 2
            periodSeconds: 1
            timeoutSeconds: 1
            successThreshold: 1
            failureThreshold: 1
          livenessProbe:
            httpGet:
              path: /gracegate/livez
              port: 9090
              scheme: HTTP
            initialDelaySeconds: 5
            periodSeconds: 5
          resources:
            limits:
              cpu: "100m"
              memory: "32Mi"
            requests:
              cpu: "10m"
              memory: "16Mi"

        # Application Container (Listens on loopback port 8081)
        - name: payment-api
          image: myorg/payment-service:v2.4.0
          env:
            - name: PORT
              value: "8081"
          ports:
            - name: local-backend
              containerPort: 8081
              protocol: TCP
          resources:
            limits:
              cpu: "1000m"
              memory: "512Mi"
            requests:
              cpu: "250m"
              memory: "256Mi"
```

---

## 5. Ingress Controller Convergence Reference

Empirical convergence benchmarks across common ingress solutions:

| Ingress / Mesh Provider | Typical Propagation Delay | Recommended Quorum Duration |
| :--- | :--- | :--- |
| **Envoy Gateway / Istio** | 1.2s to 2.4s | 3.0s |
| **NGINX Ingress Controller** | 1.8s to 3.5s | 4.0s |
| **AWS ALB (Target Group deregistration)** | 3.5s to 7.0s | 8.0s |
| **Cilium eBPF / kube-proxy IPVS** | 0.8s to 1.6s | 2.5s |
| **Traefik Ingress** | 1.5s to 3.0s | 3.5s |

Use `gracegate audit` to profile the exact propagation latency for your specific cluster infrastructure:

```bash
gracegate audit \
  --target "http://payment-service.production.svc.cluster.local:8080/health" \
  --probes 100 \
  --interval 50ms \
  --output ./audit_report.json
```
