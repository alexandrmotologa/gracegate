package webhook

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWebhookMutate(t *testing.T) {
	server := NewServer("alexandrmotologa/gracegate:v1.0.0", 8080, 8081, 9090, 8282)

	// 1. Pod without injection annotation: should be allowed with NO patch
	rawPodNoInject := `{
		"metadata": {"name": "plain-pod", "namespace": "default"},
		"spec": {"containers": [{"name": "app", "image": "nginx"}]}
	}`

	req1 := AdmissionReview{
		APIVersion: "admission.k8s.io/v1",
		Kind:       "AdmissionReview",
		Request: &AdmissionRequest{
			UID:    "uid-1",
			Object: json.RawMessage(rawPodNoInject),
		},
	}

	body1, _ := json.Marshal(req1)
	httpReq1 := httptest.NewRequest("POST", "/mutate", bytes.NewReader(body1))
	w1 := httptest.NewRecorder()
	server.ServeHTTP(w1, httpReq1)

	if w1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w1.Code)
	}

	var resp1 AdmissionReview
	_ = json.Unmarshal(w1.Body.Bytes(), &resp1)
	if !resp1.Response.Allowed {
		t.Fatalf("expected allowed true")
	}
	if resp1.Response.Patch != "" {
		t.Fatalf("expected no patch when annotation is missing")
	}

	// 2. Pod WITH injection annotation: gracegate.io/inject: "true"
	rawPodInject := `{
		"metadata": {
			"name": "payment-api",
			"namespace": "production",
			"annotations": {"gracegate.io/inject": "true"}
		},
		"spec": {
			"containers": [{"name": "payment-app", "image": "myorg/payment:1.0"}],
			"terminationGracePeriodSeconds": 30
		}
	}`

	req2 := AdmissionReview{
		APIVersion: "admission.k8s.io/v1",
		Kind:       "AdmissionReview",
		Request: &AdmissionRequest{
			UID:    "uid-2",
			Object: json.RawMessage(rawPodInject),
		},
	}

	body2, _ := json.Marshal(req2)
	httpReq2 := httptest.NewRequest("POST", "/mutate", bytes.NewReader(body2))
	w2 := httptest.NewRecorder()
	server.ServeHTTP(w2, httpReq2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w2.Code)
	}

	var resp2 AdmissionReview
	_ = json.Unmarshal(w2.Body.Bytes(), &resp2)
	if !resp2.Response.Allowed {
		t.Fatalf("expected allowed true")
	}
	if resp2.Response.PatchType != "JSONPatch" {
		t.Fatalf("expected JSONPatch patchType")
	}

	patchBytes, err := base64.StdEncoding.DecodeString(resp2.Response.Patch)
	if err != nil {
		t.Fatalf("failed to decode base64 patch: %v", err)
	}

	patchStr := string(patchBytes)
	if !strings.Contains(patchStr, "gracegate-proxy") {
		t.Fatalf("patch missing gracegate-proxy container: %s", patchStr)
	}
	if !strings.Contains(patchStr, "/gracegate/drain?sync=true") {
		t.Fatalf("patch missing preStop hook: %s", patchStr)
	}
	if !strings.Contains(patchStr, "/spec/terminationGracePeriodSeconds") {
		t.Fatalf("patch missing terminationGracePeriodSeconds adjustment")
	}
}

func TestGenerateWebhookManifest(t *testing.T) {
	manifest := GenerateWebhookManifest("my-webhook-svc", "kube-system", "LS0tLS1CRUdJTi...")
	if !strings.Contains(manifest, "name: gracegate-mutating-webhook") {
		t.Fatalf("missing webhook name")
	}
	if !strings.Contains(manifest, "name: my-webhook-svc") {
		t.Fatalf("missing service name")
	}
}
