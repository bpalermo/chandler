package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Every component pushes its metrics over OTLP to the collector e2e/run.sh
// deploys, which prints what it receives.
func TestMetricsReachTheCollector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)
	key := ns + ".app.yaml"

	// Something to measure: a pod with a volume, and an update to it.
	publish(ctx, t, key, "v1")
	mustApply(ctx, t, ns, consumerPod("consumer", true, fmt.Sprintf("- uri: file:///%s\n  path: app.yaml\n", key)))
	if _, err := waitForPodPhase(ctx, ns, "consumer", "Running"); err != nil {
		t.Fatal(err)
	}
	publish(ctx, t, key, "v2")
	expectFile(ctx, t, ns, "consumer", "app.yaml", "v2")

	want := []string{
		// Who sent them.
		"service.name: Str(chandler-server)",
		"service.name: Str(chandler-csi)",
		"service.name: Str(chandler-provider-file)",
		"service.name: Str(chandler-provider-s3)",
		"k8s.cluster.name: Str(chandler-e2e)",
		// The server: what it serves, holds and hears back.
		"chandler.server.subscriptions",
		"chandler.server.provider.connected",
		"chandler.server.cache.keys",
		"chandler.server.acks",
		// The driver: mounts and writes.
		"chandler.csi.volumes",
		"chandler.csi.publish.duration",
		"chandler.csi.apply.duration",
		// The providers: watches and what they emit.
		"chandler.provider.watches",
		"chandler.provider.updates",
	}
	err := eventually(ctx, func() error {
		logs, err := kubectl(ctx, "logs", "-n", driverNamespace, "deployment/otel-collector", "--tail=-1")
		if err != nil {
			return err
		}
		var missing []string
		for _, w := range want {
			if !strings.Contains(logs, w) {
				missing = append(missing, w)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("the collector has not received: %s", strings.Join(missing, ", "))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
