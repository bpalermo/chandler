package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// curl 8.16.0, pinned: the application side of the acknowledgement API is any
// HTTP client that can use a Unix socket.
const curlImage = "docker.io/curlimages/curl@sha256:463eaf6072688fe96ac64fa623fe73e1dbe25d8ad6c34404a669ad3ce1f104b6"

// ackPod mounts a volume with application acknowledgement, in a container
// that runs as an ordinary user and has curl.
func ackPod(name, sources string) string {
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
          appAck: "true"
`, name, curlImage, mountPath, driverName, quoted)
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
		"curl", "-sS", "--unix-socket", mountPath+"/..chandler.sock", "http://chandler/v1/config")
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
		"--unix-socket", mountPath+"/..chandler.sock", "-d", body, "http://chandler/v1/reports")
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
	raw := mustKubectl(ctx, t, "exec", "app", "-n", ns, "--", "cat", mountPath+"/..chandler.json")
	if err := json.Unmarshal([]byte(raw), &manifest); err != nil {
		t.Fatalf("..chandler.json = %q: %v", raw, err)
	}
	first := ackGet(ctx, t, ns, "app")
	if first.Generation == "" || first.Generation != manifest.Generation || len(first.Sources) != 1 ||
		first.Sources[0].Name != "app" || first.Sources[0].State != "pending" {
		t.Fatalf("config = %+v, manifest = %+v; want one pending source and the same generation", first, manifest)
	}
	if code := ackReport(ctx, t, ns, "app", fmt.Sprintf(`{"generation":%q,"state":"loaded"}`, first.Generation)); code != "204" {
		t.Fatalf("reporting loaded returned %s, want 204", code)
	}
	if got := ackGet(ctx, t, ns, "app"); got.Sources[0].State != "loaded" {
		t.Errorf("after reporting loaded the state is %q", got.Sources[0].State)
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
	body := fmt.Sprintf(`{"generation":%q,"sources":[{"name":"app","state":"rejected","message":"cannot parse"}]}`, second.Generation)
	if code := ackReport(ctx, t, ns, "app", body); code != "204" {
		t.Fatalf("reporting rejected returned %s, want 204", code)
	}
	// At once, and locally: the driver does not wait for the server.
	if got := mustKubectl(ctx, t, "exec", "app", "-n", ns, "--", "cat", mountPath+"/app.yaml"); got != "v1" {
		t.Errorf("after the rejection the file reads %q, want v1", got)
	}
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
	if code := ackReport(ctx, t, ns, "app", `{"generation":"g-0000000000000000","state":"loaded"}`); code != "409" {
		t.Errorf("a stale generation returned %s, want 409", code)
	}

	// The next version is delivered as usual.
	publish(ctx, t, key, "v3")
	expectFile(ctx, t, ns, "app", "app.yaml", "v3")
}
