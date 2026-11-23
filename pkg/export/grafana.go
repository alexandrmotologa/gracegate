package export

import (
	"encoding/json"
)

// GenerateGrafanaDashboard produces a production-grade Grafana dashboard JSON.
func GenerateGrafanaDashboard(title string) (string, error) {
	if title == "" {
		title = "gracegate - Kubernetes Zero-Loss Pod Draining Overview"
	}

	dashboard := map[string]any{
		"annotations": map[string]any{
			"list": []any{},
		},
		"editable":     true,
		"fiscalYearStartMonth": 0,
		"graphTooltip": 1,
		"id":           nil,
		"links":        []any{},
		"liveNow":      true,
		"panels": []any{
			// Row 1: Key Metrics
			map[string]any{
				"id":    1,
				"title": "Active In-Flight Requests",
				"type":  "timeseries",
				"gridPos": map[string]any{
					"h": 8,
					"w": 12,
					"x": 0,
					"y": 0,
				},
				"targets": []any{
					map[string]any{
						"expr":         "gracegate_inflight_requests{protocol=\"total\"}",
						"legendFormat": "Total In-Flight",
						"refId":        "A",
					},
					map[string]any{
						"expr":         "gracegate_inflight_requests{protocol=\"HTTP/1.1\"}",
						"legendFormat": "HTTP/1.1",
						"refId":        "B",
					},
					map[string]any{
						"expr":         "gracegate_inflight_requests{protocol=\"HTTP/2\"}",
						"legendFormat": "HTTP/2",
						"refId":        "C",
					},
					map[string]any{
						"expr":         "gracegate_inflight_requests{protocol=\"gRPC\"}",
						"legendFormat": "gRPC",
						"refId":        "D",
					},
				},
			},
			map[string]any{
				"id":    2,
				"title": "Zero-Loss Reliability Rate",
				"type":  "stat",
				"gridPos": map[string]any{
					"h": 8,
					"w": 6,
					"x": 12,
					"y": 0,
				},
				"targets": []any{
					map[string]any{
						"expr":         "100 - (sum(rate(gracegate_errors_total[1m])) / sum(rate(gracegate_requests_total[1m]) > 0) * 100)",
						"legendFormat": "Reliability %",
						"refId":        "A",
					},
				},
				"fieldConfig": map[string]any{
					"defaults": map[string]any{
						"unit": "percent",
						"color": map[string]any{
							"mode": "thresholds",
						},
						"thresholds": map[string]any{
							"mode": "absolute",
							"steps": []any{
								map[string]any{"color": "red", "value": nil},
								map[string]any{"color": "yellow", "value": 99.0},
								map[string]any{"color": "green", "value": 99.99},
							},
						},
					},
				},
			},
			map[string]any{
				"id":    3,
				"title": "Active Client TCP Connections",
				"type":  "stat",
				"gridPos": map[string]any{
					"h": 8,
					"w": 6,
					"x": 18,
					"y": 0,
				},
				"targets": []any{
					map[string]any{
						"expr":         "sum(gracegate_active_clients)",
						"legendFormat": "TCP Clients",
						"refId":        "A",
					},
				},
			},
			// Row 2: Throughput and Latency
			map[string]any{
				"id":    4,
				"title": "Proxy Request Throughput (req/s)",
				"type":  "timeseries",
				"gridPos": map[string]any{
					"h": 8,
					"w": 12,
					"x": 0,
					"y": 8,
				},
				"targets": []any{
					map[string]any{
						"expr":         "sum(rate(gracegate_requests_total[1m]))",
						"legendFormat": "Requests / sec",
						"refId":        "A",
					},
				},
			},
			map[string]any{
				"id":    5,
				"title": "Lifecycle Draining Phase Status",
				"type":  "state-timeline",
				"gridPos": map[string]any{
					"h": 8,
					"w": 12,
					"x": 12,
					"y": 8,
				},
				"targets": []any{
					map[string]any{
						"expr":         "gracegate_drain_phase",
						"legendFormat": "{{phase}}",
						"refId":        "A",
					},
				},
			},
		},
		"refresh":       "5s",
		"schemaVersion": 38,
		"tags":          []any{"kubernetes", "sre", "zero-loss", "gracegate", "fintech"},
		"time": map[string]any{
			"from": "now-15m",
			"to":   "now",
		},
		"title": title,
		"uid":   "gracegate-zero-loss",
	}

	bytes, err := json.MarshalIndent(dashboard, "", "  ")
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
