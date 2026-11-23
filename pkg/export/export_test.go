package export

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestGenerateGrafanaDashboard(t *testing.T) {
	dash, err := GenerateGrafanaDashboard("Test Dashboard")
	if err != nil {
		t.Fatalf("failed to generate dashboard: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(dash), &parsed); err != nil {
		t.Fatalf("generated invalid JSON: %v", err)
	}

	if parsed["title"] != "Test Dashboard" {
		t.Fatalf("unexpected dashboard title: %v", parsed["title"])
	}

	panels, ok := parsed["panels"].([]any)
	if !ok || len(panels) < 4 {
		t.Fatalf("expected at least 4 dashboard panels, got %d", len(panels))
	}
}

func TestGeneratePrometheusRules(t *testing.T) {
	rules := GeneratePrometheusRules("production")

	if !strings.Contains(rules, "GracegateDroppedRequestsDetected") {
		t.Fatalf("missing GracegateDroppedRequestsDetected alert")
	}
	if !strings.Contains(rules, "GracegateExcessiveDrainLatency") {
		t.Fatalf("missing GracegateExcessiveDrainLatency alert")
	}
	if !strings.Contains(rules, "namespace: production") {
		t.Fatalf("expected namespace: production in PrometheusRule")
	}
}
