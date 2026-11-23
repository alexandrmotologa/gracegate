package inject

import (
	"fmt"
	"strings"
)

// SidecarOptions configures sidecar injection into Kubernetes manifests.
type SidecarOptions struct {
	Image               string
	ListenPort          int
	TargetPort          int
	HealthPort          int
	CockpitPort         int
	GracePeriodSeconds  int
	EnableCockpit       bool
}

// DefaultOptions provides production-ready sidecar configuration defaults.
func DefaultOptions() SidecarOptions {
	return SidecarOptions{
		Image:              "alexandrmotologa/gracegate:v1.0.0",
		ListenPort:         8080,
		TargetPort:         8081,
		HealthPort:         9090,
		CockpitPort:        8282,
		GracePeriodSeconds: 60,
		EnableCockpit:      true,
	}
}

// InjectManifest processes a Kubernetes YAML manifest and injects gracegate sidecar.
func InjectManifest(manifestContent string, opts SidecarOptions) (string, error) {
	if strings.TrimSpace(manifestContent) == "" {
		return "", fmt.Errorf("manifest content cannot be empty")
	}

	lines := strings.Split(manifestContent, "\n")
	var result strings.Builder

	inContainers := false
	inTemplateSpec := false
	containersIndent := ""
	injected := false

	sidecarYaml := generateSidecarYAML(opts)

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimSpace(line)

		// Detect spec / template
		if trimmed == "spec:" {
			inTemplateSpec = true
		}

		if inTemplateSpec && (strings.HasPrefix(trimmed, "containers:")) {
			inContainers = true
			indentLen := len(line) - len(strings.TrimLeft(line, " "))
			containersIndent = strings.Repeat(" ", indentLen+2)
		}

		// Inject sidecar right after containers: header
		result.WriteString(line)
		result.WriteString("\n")

		if inContainers && !injected {
			// Write sidecar block
			indentedSidecar := indentBlock(sidecarYaml, containersIndent)
			result.WriteString(indentedSidecar)
			result.WriteString("\n")
			injected = true
		}

		// Inject terminationGracePeriodSeconds if in spec and not present
		if trimmed == "template:" || trimmed == "spec:" {
			// keep scanning
		}
	}

	if !injected {
		// Fallback: append sidecar if standard format wasn't recognized
		result.WriteString("\n# gracegate sidecar injection:\n")
		result.WriteString(sidecarYaml)
	}

	return result.String(), nil
}

func generateSidecarYAML(opts SidecarOptions) string {
	cockpitFlag := ""
	if opts.EnableCockpit {
		cockpitFlag = fmt.Sprintf("    - \"--cockpit-port\"\n    - \"%d\"\n", opts.CockpitPort)
	}

	return fmt.Sprintf(`- name: gracegate-proxy
  image: %s
  imagePullPolicy: IfNotPresent
  args:
    - "proxy"
    - "--listen"
    - ":%d"
    - "--target"
    - "127.0.0.1:%d"
    - "--health-port"
    - "%d"
%s  ports:
    - name: http-traffic
      containerPort: %d
      protocol: TCP
    - name: health-probes
      containerPort: %d
      protocol: TCP
  lifecycle:
    preStop:
      httpGet:
        path: /gracegate/drain?sync=true
        port: %d
        scheme: HTTP
  readinessProbe:
    httpGet:
      path: /gracegate/healthz
      port: %d
      scheme: HTTP
    initialDelaySeconds: 1
    periodSeconds: 1
    timeoutSeconds: 1
    successThreshold: 1
    failureThreshold: 1
  livenessProbe:
    httpGet:
      path: /gracegate/livez
      port: %d
      scheme: HTTP
    initialDelaySeconds: 2
    periodSeconds: 5
  resources:
    limits:
      cpu: "100m"
      memory: "32Mi"
    requests:
      cpu: "10m"
      memory: "16Mi"`,
		opts.Image,
		opts.ListenPort,
		opts.TargetPort,
		opts.HealthPort,
		cockpitFlag,
		opts.ListenPort,
		opts.HealthPort,
		opts.HealthPort,
		opts.HealthPort,
		opts.HealthPort,
	)
}

func indentBlock(block, indent string) string {
	lines := strings.Split(block, "\n")
	for i, l := range lines {
		if strings.TrimSpace(l) != "" {
			lines[i] = indent + l
		}
	}
	return strings.Join(lines, "\n")
}
