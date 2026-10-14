package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The server's cache is bounded per provider, and what pods use is what
// counts: with a provider's two cache keys both in use a third configuration
// is refused, and it is served as soon as one of the two is no longer used,
// in place of the entry kept for it.
//
// The provider is "small", a file provider e2e/run.sh deploys beside the
// chart's and e2e/values.yaml allows two keys.
func TestUnusedCacheEntriesMakeRoom(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)

	pods := []string{"one", "two", "three"}
	key := func(pod string) string { return ns + "." + pod + ".yaml" }
	for _, pod := range pods {
		publish(ctx, t, key(pod), "for "+pod)
	}
	start := func(pod string) {
		mustApply(ctx, t, ns, consumerPod(pod, true, fmt.Sprintf("- uri: small:///%s\n  path: app.yaml\n", key(pod))))
	}

	for _, pod := range pods[:2] {
		start(pod)
		if _, err := waitForPodPhase(ctx, ns, pod, "Running"); err != nil {
			t.Fatal(err)
		}
		expectFile(ctx, t, ns, pod, "app.yaml", "for "+pod)
	}

	// Both keys are in use, so there is nothing to evict.
	start("three")
	expectEvent(ctx, t, ns, "three", "FailedMount", "ResourceExhausted")
	if phase := mustKubectl(ctx, t, "get", "pod", "three", "-n", ns, "-o", "jsonpath={.status.phase}"); phase != "Pending" {
		t.Errorf("pod phase = %q, want Pending", phase)
	}

	// Nobody uses the first key any more. The server still holds it, for a
	// day by default, but not at the price of refusing a pod.
	mustKubectl(ctx, t, "delete", "pod", "one", "-n", ns, "--wait=true")
	if _, err := waitForPodPhase(ctx, ns, "three", "Running"); err != nil {
		t.Fatal(err)
	}
	expectFile(ctx, t, ns, "three", "app.yaml", "for three")

	// The pod whose configuration stayed in use noticed nothing, and still
	// gets updates.
	publish(ctx, t, key("two"), "for two, again")
	expectFile(ctx, t, ns, "two", "app.yaml", "for two, again")

	err := eventually(ctx, func() error {
		logs, err := kubectl(ctx, "logs", "-n", driverNamespace, "deployment/otel-collector", "--tail=-1")
		if err != nil {
			return err
		}
		if !strings.Contains(logs, "stevedore.server.cache.evictions") || !strings.Contains(logs, "reason: Str(pressure)") {
			return fmt.Errorf("the collector has not received stevedore.server.cache.evictions with reason pressure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
