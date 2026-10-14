package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	stevedorev1 "github.com/bpalermo/stevedore/api/stevedore/v1"
	"github.com/bpalermo/stevedore/common/bundle"
	"github.com/bpalermo/stevedore/common/rollout"
)

// curl 8.16.0, pinned: the application side of the acknowledgement API is any
// HTTP client that can use a Unix socket. It also speaks HTTP/2 without TLS,
// which is all gRPC needs of it here.
const curlImage = "docker.io/curlimages/curl@sha256:463eaf6072688fe96ac64fa623fe73e1dbe25d8ad6c34404a669ad3ce1f104b6"

// ackPod mounts a volume with application acknowledgement, in a container
// that runs as an ordinary user and has curl.
func ackPod(name, sources string) string {
	return ackPodWithTimeout(name, sources, "")
}

// ackPodWithTimeout is ackPod with an appAckTimeout, when timeout is set.
func ackPodWithTimeout(name, sources, timeout string) string {
	extra := ""
	if timeout != "" {
		extra = fmt.Sprintf("\n          appAckTimeout: %q", timeout)
	}
	quoted, err := json.Marshal(sources)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 0
  containers:
    - name: main
      image: %s
      command: ["sleep", "3600"]
      volumeMounts:
        - name: config
          mountPath: %s
  volumes:
    - name: config
      csi:
        driver: %s
        readOnly: true
        volumeAttributes:
          sources: %s
          appAck: "true"%s
`, name, curlImage, mountPath, driverName, quoted, extra)
}

type ackConfig struct {
	Generation string `json:"generation"`
	Sources    []struct {
		Name    string `json:"name"`
		Version string `json:"version"`
		State   string `json:"state"`
	} `json:"sources"`
}

// ackGet asks the driver, from inside the pod, what the volume holds.
func ackGet(ctx context.Context, t *testing.T, ns, pod string) ackConfig {
	t.Helper()
	out := mustKubectl(ctx, t, "exec", pod, "-n", ns, "--",
		"curl", "-sS", "--unix-socket", mountPath+"/..stevedore.sock", "http://stevedore/v1/config")
	var config ackConfig
	if err := json.Unmarshal([]byte(out), &config); err != nil {
		t.Fatalf("GET /v1/config returned %q: %v", out, err)
	}
	return config
}

// ackReport reports from inside the pod and returns the HTTP status.
func ackReport(ctx context.Context, t *testing.T, ns, pod, body string) string {
	t.Helper()
	return mustKubectl(ctx, t, "exec", pod, "-n", ns, "--",
		"curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
		"--unix-socket", mountPath+"/..stevedore.sock", "-d", body, "http://stevedore/v1/reports")
}

// Application acknowledgement, from the application's side: it finds the
// manifest and the socket in its volume, reports what it loaded, and when it
// rejects a version the files go back to the last one it accepted.
func TestApplicationAcknowledgement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)
	key := ns + ".app.yaml"

	publish(ctx, t, key, "v1")
	mustApply(ctx, t, ns, ackPod("app", fmt.Sprintf("- name: app\n  uri: file:///%s\n  path: app.yaml\n", key)))
	if _, err := waitForPodPhase(ctx, ns, "app", "Running"); err != nil {
		t.Fatal(err)
	}

	// The manifest file is in the volume and agrees with the API.
	var manifest ackConfig
	raw := mustKubectl(ctx, t, "exec", "app", "-n", ns, "--", "cat", mountPath+"/..stevedore.json")
	if err := json.Unmarshal([]byte(raw), &manifest); err != nil {
		t.Fatalf("..stevedore.json = %q: %v", raw, err)
	}
	first := ackGet(ctx, t, ns, "app")
	if first.Generation == "" || first.Generation != manifest.Generation || len(first.Sources) != 1 ||
		first.Sources[0].Name != "app" || first.Sources[0].State != "STATE_PENDING" {
		t.Fatalf("config = %+v, manifest = %+v; want one pending source and the same generation", first, manifest)
	}
	if code := ackReport(ctx, t, ns, "app", fmt.Sprintf(`{"generation":%q,"state":"STATE_LOADED"}`, first.Generation)); code != "204" {
		t.Fatalf("reporting loaded returned %s, want 204", code)
	}
	if got := ackGet(ctx, t, ns, "app"); got.Sources[0].State != "STATE_LOADED" {
		t.Errorf("after reporting loaded the state is %q", got.Sources[0].State)
	}

	// The same socket answers gRPC, told apart by the Content-Type: an empty
	// GetConfigRequest, framed by hand, comes back with the generation and a
	// status of OK in the trailers.
	out := mustKubectl(ctx, t, "exec", "app", "-n", ns, "--", "sh", "-c", fmt.Sprintf(
		`printf '\000\000\000\000\000' | curl -sS --http2-prior-knowledge --unix-socket %s/..stevedore.sock `+
			`-H 'Content-Type: application/grpc' -H 'TE: trailers' --data-binary @- -D /tmp/headers -o /tmp/body `+
			`http://stevedore/stevedore.v1.ApplicationService/GetConfig && cat /tmp/headers && grep -c %s /tmp/body`,
		mountPath, first.Generation))
	if !strings.Contains(out, "application/grpc") || !strings.Contains(out, "grpc-status: 0") || !strings.HasSuffix(strings.TrimSpace(out), "1") {
		t.Errorf("GetConfig over gRPC returned %q, want a gRPC response with status 0 and the generation", out)
	}

	// A new version arrives; the application cannot use it.
	publish(ctx, t, key, "v2: broken")
	expectFile(ctx, t, ns, "app", "app.yaml", "v2: broken")
	var second ackConfig
	err := eventually(ctx, func() error {
		second = ackGet(ctx, t, ns, "app")
		if second.Generation == first.Generation {
			return fmt.Errorf("the generation is still %s", second.Generation)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`{"generation":%q,"sources":[{"name":"app","state":"STATE_REJECTED","message":"cannot parse"}]}`, second.Generation)
	if code := ackReport(ctx, t, ns, "app", body); code != "204" {
		t.Fatalf("reporting rejected returned %s, want 204", code)
	}
	// At once, and locally: the driver does not wait for the server.
	if got := mustKubectl(ctx, t, "exec", "app", "-n", ns, "--", "cat", mountPath+"/app.yaml"); got != "v1" {
		t.Errorf("after the rejection the file reads %q, want v1", got)
	}
	// And it is on the pod, for whoever looks at it with kubectl.
	expectEvent(ctx, t, ns, "app", "ConfigRejected", "cannot parse")
	// The server was told, and does not send the version again.
	err = eventually(ctx, func() error {
		logs, err := kubectl(ctx, "logs", "-n", driverNamespace, "-l", "app.kubernetes.io/component=server", "--tail=-1")
		if err != nil {
			return err
		}
		if !strings.Contains(logs, "an application rejected a version") || !strings.Contains(logs, "cannot parse") {
			return fmt.Errorf("the config server has not logged the rejection yet")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second) // several poll and retry intervals
	if got := mustKubectl(ctx, t, "exec", "app", "-n", ns, "--", "cat", mountPath+"/app.yaml"); got != "v1" {
		t.Errorf("the rejected version came back: the file reads %q", got)
	}

	// A report about a generation that is gone is refused, with what to
	// reload from.
	if code := ackReport(ctx, t, ns, "app", `{"generation":"g-0000000000000000","state":"STATE_LOADED"}`); code != "409" {
		t.Errorf("a stale generation returned %s, want 409", code)
	}

	// The next version is delivered as usual.
	publish(ctx, t, key, "v3")
	expectFile(ctx, t, ns, "app", "app.yaml", "v3")
}

// A rollout stops when the applications that got the new version do not say
// they loaded it, and goes on once they do. Nothing is reverted for silence.
func TestRolloutHoldsWhileApplicationsStaySilent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)
	// e2e/values.yaml: keys of this form go to half the pods for 45 seconds,
	// and one overdue application holds the rollout.
	key := ns + ".staged.yaml"
	sources := fmt.Sprintf("- name: app\n  uri: file:///%s\n  path: app.yaml\n", key)

	next := stevedorev1.Bundle_builder{Files: []*stevedorev1.File{
		stevedorev1.File_builder{Path: proto.String(key), Mode: proto.Uint32(0o644), Content: []byte("v2")}.Build(),
	}}.Build()
	version := bundle.Version(next)

	publish(ctx, t, key, "v1")
	var in, out []string
	for i := 0; i < 12 && (len(in) == 0 || len(out) == 0 || i < 3); i++ {
		name := fmt.Sprintf("app-%d", i)
		mustApply(ctx, t, ns, ackPodWithTimeout(name, sources, "5s"))
		if _, err := waitForPodPhase(ctx, ns, name, "Running"); err != nil {
			t.Fatal(err)
		}
		uid := mustKubectl(ctx, t, "get", "pod", name, "-n", ns, "-o", "jsonpath={.metadata.uid}")
		if rollout.Placed(version, uid, 5000) {
			in = append(in, name)
		} else {
			out = append(out, name)
		}
	}
	if len(in) == 0 || len(out) == 0 {
		t.Fatalf("12 pods all fell on one side of a 50%% rollout (%d in, %d out)", len(in), len(out))
	}

	publish(ctx, t, key, "v2")
	for _, pod := range in {
		expectFile(ctx, t, ns, pod, "app.yaml", "v2")
	}
	// The applications say nothing. Well past the 45 seconds after which the
	// version would have gone to everyone, the rest must still be on v1, and
	// the silent ones still on v2.
	time.Sleep(75 * time.Second)
	for _, pod := range out {
		if got := mustKubectl(ctx, t, "exec", pod, "-n", ns, "--", "cat", mountPath+"/app.yaml"); got != "v1" {
			t.Errorf("%s reads %q: the rollout went on although the applications never reported", pod, got)
		}
	}
	for _, pod := range in {
		if got := mustKubectl(ctx, t, "exec", pod, "-n", ns, "--", "cat", mountPath+"/app.yaml"); got != "v2" {
			t.Errorf("%s reads %q: a pod must not be taken back for being slow to report", pod, got)
		}
	}

	// They report at last: the hold lifts and the version reaches everyone.
	for _, pod := range in {
		config := ackGet(ctx, t, ns, pod)
		if code := ackReport(ctx, t, ns, pod, fmt.Sprintf(`{"generation":%q,"state":"STATE_LOADED"}`, config.Generation)); code != "204" {
			t.Fatalf("%s: reporting loaded returned %s", pod, code)
		}
	}
	for _, pod := range out {
		expectFile(ctx, t, ns, pod, "app.yaml", "v2")
	}
}
