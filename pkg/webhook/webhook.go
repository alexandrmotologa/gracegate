package webhook

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// AdmissionReview mirrors Kubernetes admission.k8s.io/v1 AdmissionReview.
type AdmissionReview struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Request    *AdmissionRequest  `json:"request,omitempty"`
	Response   *AdmissionResponse `json:"response,omitempty"`
}

// AdmissionRequest contains the request payload sent by the Kube API server.
type AdmissionRequest struct {
	UID       string          `json:"uid"`
	Kind      map[string]string `json:"kind"`
	Resource  map[string]string `json:"resource"`
	Namespace string          `json:"namespace"`
	Operation string          `json:"operation"`
	Object    json.RawMessage `json:"object"`
}

// AdmissionResponse contains the mutation outcome.
type AdmissionResponse struct {
	UID       string `json:"uid"`
	Allowed   bool   `json:"allowed"`
	Patch     string `json:"patch,omitempty"`
	PatchType string `json:"patchType,omitempty"`
	Status    *Status `json:"status,omitempty"`
}

// Status contains message and error code if admission is rejected.
type Status struct {
	Message string `json:"message,omitempty"`
	Code    int32  `json:"code,omitempty"`
}

// JSONPatchOperation represents an RFC 6902 JSON patch.
type JSONPatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

// PodSpecMetadata mirrors pod metadata for inspection.
type PodSpecMetadata struct {
	Metadata struct {
		Name        string            `json:"name"`
		Namespace   string            `json:"namespace"`
		Annotations map[string]string `json:"annotations"`
		Labels      map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		Containers []struct {
			Name string `json:"name"`
		} `json:"containers"`
		TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
	} `json:"spec"`
}

// Server handles Kubernetes mutating admission webhook calls.
type Server struct {
	image       string
	listenPort  int
	targetPort  int
	healthPort  int
	cockpitPort int
}

// NewServer creates a webhook admission server.
func NewServer(image string, listenPort, targetPort, healthPort, cockpitPort int) *Server {
	if image == "" {
		image = "alexandrmotologa/gracegate:v1.0.0"
	}
	if listenPort <= 0 {
		listenPort = 8080
	}
	if targetPort <= 0 {
		targetPort = 8081
	}
	if healthPort <= 0 {
		healthPort = 9090
	}
	if cockpitPort <= 0 {
		cockpitPort = 8282
	}

	return &Server{
		image:       image,
		listenPort:  listenPort,
		targetPort:  targetPort,
		healthPort:  healthPort,
		cockpitPort: cockpitPort,
	}
}

// ServeHTTP handles admission mutation requests.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var review AdmissionReview
	if err := json.Unmarshal(body, &review); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	if review.Request == nil {
		http.Error(w, "Missing admission request", http.StatusBadRequest)
		return
	}

	response := s.mutate(review.Request)
	review.Response = response

	respBytes, err := json.Marshal(review)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(respBytes)
}

func (s *Server) mutate(req *AdmissionRequest) *AdmissionResponse {
	resp := &AdmissionResponse{
		UID:     req.UID,
		Allowed: true,
	}

	var pod PodSpecMetadata
	if err := json.Unmarshal(req.Object, &pod); err != nil {
		resp.Allowed = false
		resp.Status = &Status{Message: fmt.Sprintf("Failed to unmarshal pod: %v", err)}
		return resp
	}

	// Check if injection is explicitly requested via annotation: gracegate.io/inject: "true"
	injectVal := pod.Metadata.Annotations["gracegate.io/inject"]
	if !strings.EqualFold(injectVal, "true") && !strings.EqualFold(injectVal, "yes") {
		// No injection requested, allow unmodified
		return resp
	}

	// Check if already injected
	for _, c := range pod.Spec.Containers {
		if c.Name == "gracegate-proxy" {
			return resp // already present
		}
	}

	patches := s.buildPatches(&pod)
	if len(patches) > 0 {
		patchData, err := json.Marshal(patches)
		if err == nil {
			resp.Patch = base64.StdEncoding.EncodeToString(patchData)
			resp.PatchType = "JSONPatch"
		}
	}

	return resp
}

func (s *Server) buildPatches(pod *PodSpecMetadata) []JSONPatchOperation {
	sidecarContainer := map[string]any{
		"name":            "gracegate-proxy",
		"image":           s.image,
		"imagePullPolicy": "IfNotPresent",
		"args": []string{
			"proxy",
			"--listen", fmt.Sprintf(":%d", s.listenPort),
			"--target", fmt.Sprintf("127.0.0.1:%d", s.targetPort),
			"--health-port", fmt.Sprintf("%d", s.healthPort),
			"--cockpit-port", fmt.Sprintf("%d", s.cockpitPort),
		},
		"ports": []any{
			map[string]any{
				"name":          "http-traffic",
				"containerPort": s.listenPort,
				"protocol":      "TCP",
			},
			map[string]any{
				"name":          "health-probes",
				"containerPort": s.healthPort,
				"protocol":      "TCP",
			},
			map[string]any{
				"name":          "cockpit-ui",
				"containerPort": s.cockpitPort,
				"protocol":      "TCP",
			},
		},
		"lifecycle": map[string]any{
			"preStop": map[string]any{
				"httpGet": map[string]any{
					"path":   "/gracegate/drain?sync=true",
					"port":   s.healthPort,
					"scheme": "HTTP",
				},
			},
		},
		"readinessProbe": map[string]any{
			"httpGet": map[string]any{
				"path":   "/gracegate/healthz",
				"port":   s.healthPort,
				"scheme": "HTTP",
			},
			"initialDelaySeconds": 1,
			"periodSeconds":        1,
			"timeoutSeconds":       1,
			"successThreshold":     1,
			"failureThreshold":     1,
		},
		"livenessProbe": map[string]any{
			"httpGet": map[string]any{
				"path":   "/gracegate/livez",
				"port":   s.healthPort,
				"scheme": "HTTP",
			},
			"initialDelaySeconds": 2,
			"periodSeconds":        5,
		},
		"resources": map[string]any{
			"limits": map[string]any{
				"cpu":    "100m",
				"memory": "32Mi",
			},
			"requests": map[string]any{
				"cpu":    "10m",
				"memory": "16Mi",
			},
		},
	}

	patches := []JSONPatchOperation{
		{
			Op:    "add",
			Path:  "/spec/containers/0",
			Value: sidecarContainer,
		},
	}

	// Ensure terminationGracePeriodSeconds is at least 60s
	if pod.Spec.TerminationGracePeriodSeconds == nil || *pod.Spec.TerminationGracePeriodSeconds < 60 {
		patches = append(patches, JSONPatchOperation{
			Op:    "add",
			Path:  "/spec/terminationGracePeriodSeconds",
			Value: 60,
		})
	}

	return patches
}

// GenerateWebhookManifest creates the Kubernetes MutatingWebhookConfiguration YAML.
func GenerateWebhookManifest(serviceName, namespace string, caBundle string) string {
	if serviceName == "" {
		serviceName = "gracegate-webhook"
	}
	if namespace == "" {
		namespace = "kube-system"
	}

	return fmt.Sprintf(`apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: gracegate-mutating-webhook
  labels:
    app: gracegate-webhook
webhooks:
  - name: inject.gracegate.io
    rules:
      - apiGroups: [""]
        apiVersions: ["v1"]
        operations: ["CREATE"]
        resources: ["pods"]
        scope: "Namespaced"
    clientConfig:
      service:
        name: %s
        namespace: %s
        path: "/mutate"
        port: 8443
      caBundle: "%s"
    admissionReviewVersions: ["v1"]
    sideEffects: None
    timeoutSeconds: 5
    failurePolicy: Ignore
`, serviceName, namespace, caBundle)
}
