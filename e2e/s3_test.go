package e2e

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// The bucket the chart's S3 provider is allowed to serve (e2e/values.yaml).
const bucket = "chandler-e2e"

// s3Endpoint forwards a local port to the LocalStack that runs beside the S3
// provider and returns its URL. The forward ends with the test.
func s3Endpoint(ctx context.Context, t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(ctx, "kubectl", "port-forward", "-n", driverNamespace,
		"deployment/chandler-provider-s3", ":4566")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		cmd.Wait()
	})

	// kubectl prints "Forwarding from 127.0.0.1:<port> -> 4566" once ready.
	found := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if rest, ok := strings.CutPrefix(scanner.Text(), "Forwarding from 127.0.0.1:"); ok {
				port, _, _ := strings.Cut(rest, " ")
				found <- port
				break
			}
		}
		io.Copy(io.Discard, stdout)
	}()
	select {
	case port := <-found:
		return "http://127.0.0.1:" + port
	case <-time.After(30 * time.Second):
		t.Fatal("kubectl port-forward to LocalStack did not start")
		return ""
	}
}

// s3Do sends one request to LocalStack, which takes them unsigned.
func s3Do(ctx context.Context, t *testing.T, method, url, body string, header ...string) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		detail, _ := io.ReadAll(resp.Body)
		t.Fatalf("%s %s: %s: %s", method, url, resp.Status, detail)
	}
}

// The S3 provider against an S3 API: a single object, a prefix as a
// directory, changes to both, and a bucket it is not allowed to serve.
func TestS3Provider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)
	endpoint := s3Endpoint(ctx, t)
	put := func(key, content string) {
		t.Helper()
		s3Do(ctx, t, http.MethodPut, fmt.Sprintf("%s/%s/%s/%s", endpoint, bucket, ns, key), content)
	}

	// Creating a bucket that exists succeeds in us-east-1, so reruns are fine.
	s3Do(ctx, t, http.MethodPut, endpoint+"/"+bucket, "")
	put("app.yaml", "replicas: 1\n")
	put("flags/a.json", `{"a": 1}`)
	put("flags/nested/b.json", `{"b": 1}`)

	mustApply(ctx, t, ns, consumerPod("consumer", true, fmt.Sprintf(`
- uri: s3://%[1]s/%[2]s/app.yaml
  path: app.yaml
- uri: s3://%[1]s/%[2]s/flags/
  path: flags
`, bucket, ns)))
	if _, err := waitForPodPhase(ctx, ns, "consumer", "Running"); err != nil {
		t.Fatal(err)
	}
	for file, want := range map[string]string{
		"app.yaml":            "replicas: 1\n",
		"flags/a.json":        `{"a": 1}`,
		"flags/nested/b.json": `{"b": 1}`,
	} {
		if got := mustKubectl(ctx, t, "exec", "consumer", "-n", ns, "--", "cat", mountPath+"/"+file); got != want {
			t.Errorf("%s = %q on start, want %q", file, got, want)
		}
	}

	// An object is replaced.
	put("app.yaml", "replicas: 5\n")
	expectFile(ctx, t, ns, "consumer", "app.yaml", "replicas: 5\n")

	// The directory follows the prefix: a change, an addition and a removal.
	put("flags/a.json", `{"a": 2}`)
	put("flags/c.json", `{"c": 1}`)
	s3Do(ctx, t, http.MethodDelete, fmt.Sprintf("%s/%s/%s/flags/nested/b.json", endpoint, bucket, ns), "")
	expectFile(ctx, t, ns, "consumer", "flags/a.json", `{"a": 2}`)
	expectFile(ctx, t, ns, "consumer", "flags/c.json", `{"c": 1}`)
	err := eventually(ctx, func() error {
		if out, err := kubectl(ctx, "exec", "consumer", "-n", ns, "--", "ls", mountPath+"/flags/nested/b.json"); err == nil {
			return fmt.Errorf("the removed object is still in the volume: %s", out)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// A bucket outside the provider's allow-list is refused, and the pod
	// that asked for it is told why.
	mustApply(ctx, t, ns, consumerPod("refused", true, "- uri: s3://someone-elses-bucket/app.yaml\n  path: app.yaml\n"))
	err = eventually(ctx, func() error {
		events, err := kubectl(ctx, "get", "events", "-n", ns,
			"--field-selector", "involvedObject.name=refused,reason=FailedMount",
			"-o", "jsonpath={.items[*].message}")
		if err != nil {
			return err
		}
		if !strings.Contains(events, "does not serve bucket") {
			return fmt.Errorf("no FailedMount event naming the bucket yet; events: %q", events)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Content the provider marks no_store, here by the object's Cache-Control,
// never reaches the node's disk: a pod gets it only in a volume kept in
// memory, and is told so otherwise.
func TestNoStoreStaysInMemory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)
	endpoint := s3Endpoint(ctx, t)
	const secret = "token: 6f1c0e2d9a7b4c58"
	sources := fmt.Sprintf("- uri: s3://%s/%s/secret.yaml\n  path: secret.yaml\n", bucket, ns)

	s3Do(ctx, t, http.MethodPut, endpoint+"/"+bucket, "")
	s3Do(ctx, t, http.MethodPut, fmt.Sprintf("%s/%s/%s/secret.yaml", endpoint, bucket, ns), secret, "Cache-Control", "no-store")

	// A volume on the node's disk is refused the content, with the way out.
	mustApply(ctx, t, ns, consumerPod("on-disk", true, sources))
	err := eventually(ctx, func() error {
		events, err := kubectl(ctx, "get", "events", "-n", ns,
			"--field-selector", "involvedObject.name=on-disk,reason=FailedMount",
			"-o", "jsonpath={.items[*].message}")
		if err != nil {
			return err
		}
		if !strings.Contains(events, "medium: Memory") {
			return fmt.Errorf("no FailedMount event naming the attribute yet; events: %q", events)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	mustApply(ctx, t, ns, consumerPodWithAttributes("in-memory", true, sources, "medium: Memory"))
	if _, err := waitForPodPhase(ctx, ns, "in-memory", "Running"); err != nil {
		t.Fatal(err)
	}
	// The inspector starts after the volume exists: it sees the mounts that
	// are on the node when it starts, not the ones made later.
	mustApply(ctx, t, ns, inspectorPod)
	if _, err := waitForPodPhase(ctx, ns, "inspector", "Running"); err != nil {
		t.Fatal(err)
	}
	if got := mustKubectl(ctx, t, "exec", "in-memory", "-n", ns, "--", "cat", mountPath+"/secret.yaml"); got != secret {
		t.Errorf("secret.yaml = %q", got)
	}
	// The pod sees a filesystem in memory...
	if fs := strings.TrimSpace(mustKubectl(ctx, t, "exec", "in-memory", "-n", ns, "--", "stat", "-f", "-c", "%T", mountPath)); fs != "tmpfs" {
		t.Errorf("the volume is on %q, want tmpfs", fs)
	}
	// ...and on the node the bytes are in no file outside it: not in the
	// node cache, and not under the volume except through that mount.
	script := fmt.Sprintf(`grep -rlsF %q /state/cache; for m in /state/volumes/*/meta.json; do d=$(dirname "$m"); `+
		`if grep -qs '"namespace":"%s"' "$m"; then echo "fs=$(stat -f -c %%T "$d/data")"; fi; done`, secret, ns)
	out := mustKubectl(ctx, t, "exec", "inspector", "-n", ns, "--", "sh", "-c", script)
	if strings.Contains(out, "/state/cache") {
		t.Errorf("the node cache holds the no_store content:\n%s", out)
	}
	if !strings.Contains(out, "fs=tmpfs") {
		t.Errorf("the volume's data directory on the node is not a tmpfs:\n%s", out)
	}

	// An update is delivered like any other.
	s3Do(ctx, t, http.MethodPut, fmt.Sprintf("%s/%s/%s/secret.yaml", endpoint, bucket, ns), secret+"-rotated", "Cache-Control", "no-store")
	expectFile(ctx, t, ns, "in-memory", "secret.yaml", secret+"-rotated")

	// The memory outlives the driver: after a restart the pod still reads
	// its file, and the new driver finds the volume and keeps updating it.
	mustKubectl(ctx, t, "rollout", "restart", "daemonset/chandler-csi", "-n", driverNamespace)
	mustKubectl(ctx, t, "rollout", "status", "daemonset/chandler-csi", "-n", driverNamespace, "--timeout=180s")
	if got := mustKubectl(ctx, t, "exec", "in-memory", "-n", ns, "--", "cat", mountPath+"/secret.yaml"); got != secret+"-rotated" {
		t.Errorf("after a driver restart secret.yaml = %q", got)
	}
	s3Do(ctx, t, http.MethodPut, fmt.Sprintf("%s/%s/%s/secret.yaml", endpoint, bucket, ns), secret+"-again", "Cache-Control", "no-store")
	expectFile(ctx, t, ns, "in-memory", "secret.yaml", secret+"-again")

	// And when the pod goes, so does its memory.
	mustKubectl(ctx, t, "delete", "pod", "in-memory", "-n", ns, "--wait=true")
	err = eventually(ctx, func() error {
		left, err := kubectl(ctx, "exec", "inspector", "-n", ns, "--", "sh", "-c",
			fmt.Sprintf(`grep -ls '"namespace":"%s"' /state/volumes/*/meta.json || true`, ns))
		if err != nil {
			return err
		}
		if strings.TrimSpace(left) != "" {
			return fmt.Errorf("the volume is still on the node: %q", strings.TrimSpace(left))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
