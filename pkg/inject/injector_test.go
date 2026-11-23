package inject

import (
	"strings"
	"testing"
)

func TestInjectManifest(t *testing.T) {
	sampleManifest := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
spec:
  replicas: 3
  template:
    spec:
      containers:
        - name: app
          image: myorg/order-service:1.0
          ports:
            - containerPort: 8081`

	opts := DefaultOptions()
	opts.ListenPort = 8080
	opts.TargetPort = 8081
	opts.HealthPort = 9090

	patched, err := InjectManifest(sampleManifest, opts)
	if err != nil {
		t.Fatalf("failed to inject manifest: %v", err)
	}

	if !strings.Contains(patched, "name: gracegate-proxy") {
		t.Fatalf("patched manifest missing gracegate container")
	}
	if !strings.Contains(patched, "/gracegate/drain?sync=true") {
		t.Fatalf("patched manifest missing preStop hook")
	}
	if !strings.Contains(patched, "/gracegate/healthz") {
		t.Fatalf("patched manifest missing readinessProbe")
	}
	if !strings.Contains(patched, "containerPort: 8080") {
		t.Fatalf("patched manifest missing traffic port")
	}
}
