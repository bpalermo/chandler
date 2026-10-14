package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// The suite runs against whatever cluster the ambient kubeconfig points at. It
// is tagged e2e and excluded from `bazel test //...`; e2e/run.sh creates the
// kind cluster, loads the images built from this checkout and runs it.

const (
	// imageTag is the tag the Bazel image_load targets give locally built
	// images; e2e/run.sh loads them into the kind node under the same names.
	imageTag = "e2e"

	pollInterval = time.Second
)

// image returns the image of a component the suite runs: the one built from
// this checkout and loaded into the node, or, when e2e/run.sh tests a
// published chart, the one the chart pins (E2E_IMAGES, "name=ref" pairs).
func image(name string) string {
	for _, pair := range strings.Fields(os.Getenv("E2E_IMAGES")) {
		if n, ref, ok := strings.Cut(pair, "="); ok && n == name {
			return ref
		}
	}
	return fmt.Sprintf("localhost/%s:%s", name, imageTag)
}

// imagePullPolicy is what to run image() with: never pull an image loaded
// into the node, which is in no registry, and pull a published one.
func imagePullPolicy() string {
	if os.Getenv("E2E_IMAGES") != "" {
		return "IfNotPresent"
	}
	return "Never"
}

// kubectl runs one kubectl command and returns its standard output.
func kubectl(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// mustApply applies a manifest in a namespace.
func mustApply(ctx context.Context, t *testing.T, ns, manifest string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", ns, "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("kubectl apply: %v: %s", err, out)
	}
}

// eventually retries check until it passes or the context ends, and then
// returns the last failure.
func eventually(ctx context.Context, check func() error) error {
	for {
		err := check()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (gave up: %v)", err, ctx.Err())
		case <-time.After(pollInterval):
		}
	}
}

func mustKubectl(ctx context.Context, t *testing.T, args ...string) string {
	t.Helper()
	out, err := kubectl(ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// newNamespace creates a namespace for one test and removes it afterwards. On
// failure it first dumps what is in it, since the cluster is gone by the time
// anyone reads the CI log.
func newNamespace(ctx context.Context, t *testing.T) string {
	t.Helper()
	ns := fmt.Sprintf("e2e-%s-%d", strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-")), time.Now().UnixNano()%1e6)
	if len(ns) > 63 {
		ns = ns[:63]
	}
	mustKubectl(ctx, t, "create", "namespace", ns)
	t.Cleanup(func() {
		// The test's context may already be done.
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if t.Failed() {
			for _, args := range [][]string{
				{"get", "pods", "-n", ns, "-o", "wide"},
				{"get", "events", "-n", ns, "--sort-by=.lastTimestamp"},
				{"logs", "-n", "stevedore-system", "-l", "app.kubernetes.io/component=csi", "--all-containers", "--tail=100"},
				{"logs", "-n", "stevedore-system", "-l", "app.kubernetes.io/component=server", "--tail=100"},
				{"logs", "-n", "stevedore-system", "-l", "app.kubernetes.io/component=provider-file", "--tail=100"},
				{"logs", "-n", "stevedore-system", "-l", "app.kubernetes.io/component=provider-s3", "--all-containers", "--tail=100"},
				{"get", "pods", "-n", "stevedore-system", "-o", "wide"},
			} {
				out, err := kubectl(ctx, args...)
				t.Logf("kubectl %s:\n%s%v", strings.Join(args, " "), out, errString(err))
			}
		}
		if _, err := kubectl(ctx, "delete", "namespace", ns, "--wait=false"); err != nil {
			t.Logf("cleanup: %v", err)
		}
	})
	return ns
}

// expectEvent waits for a warning event on a pod with the given reason whose
// message mentions text.
func expectEvent(ctx context.Context, t *testing.T, ns, pod, reason, text string) {
	t.Helper()
	err := eventually(ctx, func() error {
		messages, err := kubectl(ctx, "get", "events", "-n", ns,
			"--field-selector", fmt.Sprintf("involvedObject.name=%s,reason=%s,type=Warning", pod, reason),
			"-o", "jsonpath={.items[*].message}")
		if err != nil {
			return err
		}
		if !strings.Contains(messages, text) {
			return fmt.Errorf("no %s event on pod %s mentioning %q yet; found: %q", reason, pod, text, messages)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// waitForPodPhase polls until the pod reaches one of the wanted phases and
// returns the phase it reached.
func waitForPodPhase(ctx context.Context, ns, pod string, want ...string) (string, error) {
	var last string
	for {
		phase, err := kubectl(ctx, "get", "pod", pod, "-n", ns, "-o", "jsonpath={.status.phase}")
		if err == nil {
			last = phase
			for _, w := range want {
				if phase == w {
					return phase, nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return last, fmt.Errorf("pod %s/%s: waiting for phase %v, last seen %q: %w", ns, pod, want, last, ctx.Err())
		case <-time.After(pollInterval):
		}
	}
}

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if _, err := kubectl(ctx, "version", "--request-timeout=20s"); err != nil {
		cancel()
		fmt.Fprintf(os.Stderr, "e2e: no reachable cluster: %v\nrun e2e/run.sh, which creates one\n", err)
		os.Exit(1)
	}
	cancel()
	os.Exit(m.Run())
}
