package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
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

	// Something to measure, from both providers so that both have something
	// to report: a pod with a volume, and an update to it.
	publish(ctx, t, key, "v1")
	endpoint := s3Endpoint(ctx, t)
	s3Do(ctx, t, http.MethodPut, endpoint+"/"+bucket, "")
	s3Do(ctx, t, http.MethodPut, fmt.Sprintf("%s/%s/%s/app.yaml", endpoint, bucket, ns), "from s3")
	mustApply(ctx, t, ns, consumerPod("consumer", true, fmt.Sprintf(
		"- uri: file:///%s\n- uri: s3://%s/%s/app.yaml\n", key, bucket, ns)))
	if _, err := waitForPodPhase(ctx, ns, "consumer", "Running"); err != nil {
		t.Fatal(err)
	}
	publish(ctx, t, key, "v2")
	expectFile(ctx, t, ns, "consumer", "file/"+key, "v2")

	want := []string{
		// Who sent them.
		"service.name: Str(stevedore-server)",
		"service.name: Str(stevedore-csi)",
		"service.name: Str(stevedore-provider-file)",
		"service.name: Str(stevedore-provider-s3)",
		"k8s.cluster.name: Str(stevedore-e2e)",
		// The server: what it serves, holds and hears back.
		"stevedore.server.subscriptions",
		"stevedore.server.provider.connected",
		"stevedore.server.cache.keys",
		"stevedore.server.acks",
		"stevedore.server.cache.keys.limit",
		"stevedore.server.cache.uri_variants",
		"stevedore.server.namespace_uris",
		"stevedore.server.peer.sync_age",
		// The driver: mounts and writes.
		"stevedore.csi.volumes",
		"stevedore.csi.publish.duration",
		"stevedore.csi.apply.duration",
		"stevedore.csi.cache.bytes",
		// The providers: watches and what they emit.
		"stevedore.provider.watches",
		"stevedore.provider.updates",
	}
	// Every component reports the version it was built from, the one
	// e2e/run.sh stamped the images with.
	version := os.Getenv("E2E_VERSION")
	if version == "" || version == "dev" {
		t.Fatalf("E2E_VERSION is %q: run the suite through e2e/run.sh", version)
	}
	services := []string{"stevedore-server", "stevedore-csi", "stevedore-provider-file", "stevedore-provider-s3"}
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
		// The debug exporter prints each resource in its own block.
		stamped := make(map[string]bool)
		for _, block := range strings.Split(logs, "ResourceMetrics #") {
			for _, service := range services {
				if strings.Contains(block, "service.name: Str("+service+")") &&
					strings.Contains(block, "service.version: Str("+version+")") {
					stamped[service] = true
				}
			}
		}
		for _, service := range services {
			if !stamped[service] {
				missing = append(missing, service+" at version "+version)
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
