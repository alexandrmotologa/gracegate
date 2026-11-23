package export

import (
	"fmt"
)

// GeneratePrometheusRules produces a Kubernetes PrometheusRule YAML manifest.
func GeneratePrometheusRules(namespace string) string {
	if namespace == "" {
		namespace = "monitoring"
	}

	return fmt.Sprintf(`apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: gracegate-alerts
  namespace: %s
  labels:
    role: alert-rules
    app: gracegate
spec:
  groups:
    - name: gracegate.zero-loss.rules
      rules:
        # Alert 1: Dropped requests or connection cuts detected during drain
        - alert: GracegateDroppedRequestsDetected
          expr: sum(rate(gracegate_errors_total[1m])) > 0
          for: 10s
          labels:
            severity: critical
            team: sre
          annotations:
            summary: "gracegate detected dropped requests during pod lifecycle event"
            description: "Pod {{ $labels.pod }} in namespace {{ $labels.namespace }} encountered 5xx errors while draining traffic."

        # Alert 2: In-flight draining took unusually long (> 25s)
        - alert: GracegateExcessiveDrainLatency
          expr: gracegate_drain_phase{phase="DRAINING_INFLIGHT"} == 1
          for: 25s
          labels:
            severity: warning
            team: sre
          annotations:
            summary: "Pod in-flight request draining exceeded 25 seconds"
            description: "Pod {{ $labels.pod }} has been flushing in-flight requests for over 25 seconds, approaching the terminationGracePeriod boundary."

        # Alert 3: Proactive circuit breaker tripped
        - alert: GracegateCircuitBreakerTripped
          expr: gracegate_drain_phase{phase="RUNNING"} == 0 and gracegate_inflight_requests{protocol="total"} > 0
          for: 15s
          labels:
            severity: warning
            team: sre
          annotations:
            summary: "gracegate proactive circuit breaker isolated application"
            description: "Backend application behind gracegate sidecar on {{ $labels.pod }} failed consecutive health checks and has been temporarily isolated from ingress."
`, namespace)
}
