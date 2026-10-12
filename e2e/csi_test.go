package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

const (
	driverName      = "csi.chandler.dev"
	driverNamespace = "chandler-system"

	// The ConfigMap the chart's file provider serves; e2e/run.sh creates it
	// and points the provider at it.
	configMap = "chandler-e2e-config"

	// busybox 1.37, pinned so the suite does not depend on a moving tag.
	busybox = "docker.io/library/busybox@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e"

	mountPath = "/etc/chandler"
)

// consumerPod is a pod that mounts a Chandler inline volume with the given
// sources, the YAML list a pod author writes.
func consumerPod(name string, readOnly bool, sources string) string {
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
        readOnly: %t
        volumeAttributes:
          sources: %s
`, name, busybox, mountPath, driverName, readOnly, quoted)
}

// inspectorPod looks at the driver's state directory on the node.
const inspectorPod = `
apiVersion: v1
kind: Pod
metadata:
  name: inspector
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 0
  containers:
    - name: main
      image: ` + busybox + `
      command: ["sleep", "3600"]
      volumeMounts:
        - name: state
          mountPath: /state
          readOnly: true
  volumes:
    - name: state
      hostPath:
        path: /var/lib/chandler
`

// publish puts a file into the origin the file provider serves: a key of the
// ConfigMap mounted into the provider's pod.
//
// kubelet refreshes a mounted ConfigMap about once a minute. Touching the pod
// makes it sync at once, which keeps the suite fast; without it the tests
// would still pass, only later.
func publish(ctx context.Context, t *testing.T, key, content string) {
	t.Helper()
	patch, err := json.Marshal(map[string]any{"data": map[string]string{key: content}})
	if err != nil {
		t.Fatal(err)
	}
	mustKubectl(ctx, t, "patch", "configmap", configMap, "-n", driverNamespace, "--type=merge", "-p", string(patch))
	mustKubectl(ctx, t, "annotate", "pods", "-n", driverNamespace, "-l", "app.kubernetes.io/component=provider-file",
		"--overwrite", fmt.Sprintf("e2e.chandler.dev/published=%d", time.Now().UnixNano()))
}

// expectFile waits until a file in the consumer's volume has the content.
func expectFile(ctx context.Context, t *testing.T, ns, pod, file, want string) {
	t.Helper()
	err := eventually(ctx, func() error {
		got, err := kubectl(ctx, "exec", pod, "-n", ns, "--", "cat", mountPath+"/"+file)
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("%s = %q, want %q", file, got, want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDriverRegistersWithKubelet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	mustKubectl(ctx, t, "rollout", "status", "daemonset/chandler-csi", "-n", driverNamespace, "--timeout=150s")

	// The registrar sidecar hands the driver's socket to kubelet, which then
	// lists the driver on the node's CSINode object.
	err := eventually(ctx, func() error {
		drivers, err := kubectl(ctx, "get", "csinode", "-o", "jsonpath={.items[*].spec.drivers[*].name}")
		if err != nil {
			return err
		}
		if !strings.Contains(drivers, driverName) {
			return fmt.Errorf("CSINode drivers = %q, want %s among them", drivers, driverName)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// The whole path: a file at the origin reaches a pod's volume through the
// provider, the server and the driver, and a change to it follows without the
// pod restarting.
func TestConfigIsDeliveredAndUpdated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)
	app, flags := ns+".app.yaml", ns+".flags.json"

	publish(ctx, t, app, "replicas: 1\n")
	publish(ctx, t, flags, `{"beta": false}`)

	// Two sources in one volume: a single file under a name of the pod's
	// choosing, and the origin's whole directory.
	mustApply(ctx, t, ns, consumerPod("consumer", true, fmt.Sprintf(`
- uri: file:///%s
  path: app.yaml
- uri: file:///
  path: all
`, app)))
	mustApply(ctx, t, ns, inspectorPod)
	for _, pod := range []string{"consumer", "inspector"} {
		if _, err := waitForPodPhase(ctx, ns, pod, "Running"); err != nil {
			t.Fatal(err)
		}
	}

	// Mounting waited for the content, so it is there when the pod starts.
	if got := mustKubectl(ctx, t, "exec", "consumer", "-n", ns, "--", "cat", mountPath+"/app.yaml"); got != "replicas: 1\n" {
		t.Errorf("app.yaml = %q on start", got)
	}
	expectFile(ctx, t, ns, "consumer", "all/"+flags, `{"beta": false}`)

	// It is mounted read-only, whatever the container runs as.
	if out, err := kubectl(ctx, "exec", "consumer", "-n", ns, "--", "touch", mountPath+"/probe"); err == nil {
		t.Errorf("writing into the volume succeeded: %s", out)
	} else if !strings.Contains(err.Error(), "Read-only file system") {
		t.Errorf("writing into the volume failed for the wrong reason: %v", err)
	}

	// A change at the origin replaces the files in place, by a new generation.
	generation := mustKubectl(ctx, t, "exec", "consumer", "-n", ns, "--", "readlink", mountPath+"/..data")
	if !strings.HasPrefix(generation, "..") {
		t.Errorf("..data -> %q, want a generation directory", generation)
	}
	publish(ctx, t, app, "replicas: 3\n")
	expectFile(ctx, t, ns, "consumer", "app.yaml", "replicas: 3\n")
	expectFile(ctx, t, ns, "consumer", "all/"+app, "replicas: 3\n")
	if after := mustKubectl(ctx, t, "exec", "consumer", "-n", ns, "--", "readlink", mountPath+"/..data"); after == generation {
		t.Errorf("the content changed but ..data still points at %q", after)
	}
	// The other file of the directory survived the update of its neighbour.
	expectFile(ctx, t, ns, "consumer", "all/"+flags, `{"beta": false}`)
	if restarts := mustKubectl(ctx, t, "get", "pod", "consumer", "-n", ns,
		"-o", "jsonpath={.status.containerStatuses[0].restartCount}"); restarts != "0" {
		t.Errorf("the container restarted %s times; updates must not need a restart", restarts)
	}

	// The driver keeps this pod's volume on the node...
	volumesOf := func() (string, error) {
		// grep exits 1 when nothing matches, which is an answer here.
		out, err := kubectl(ctx, "exec", "inspector", "-n", ns, "--",
			"sh", "-c", fmt.Sprintf("grep -ls '\"namespace\":\"%s\"' /state/volumes/*/meta.json || true", ns))
		return strings.TrimSpace(out), err
	}
	if volumes, err := volumesOf(); err != nil || len(strings.Fields(volumes)) != 1 {
		t.Fatalf("volumes of this pod on the node = %q (%v), want exactly one", volumes, err)
	}

	// ...and removes it when the pod goes away.
	mustKubectl(ctx, t, "delete", "pod", "consumer", "-n", ns, "--wait=true")
	err := eventually(ctx, func() error {
		left, err := volumesOf()
		if err != nil {
			return err
		}
		if left != "" {
			return fmt.Errorf("volumes still on the node: %q", left)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Updates must keep arriving after the driver or the server is replaced: the
// pods they serve are not restarted with them.
func TestUpdatesSurviveRestarts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)
	key := ns + ".app.yaml"

	publish(ctx, t, key, "v1")
	mustApply(ctx, t, ns, consumerPod("consumer", true, fmt.Sprintf("- uri: file:///%s\n  path: app.yaml\n", key)))
	if _, err := waitForPodPhase(ctx, ns, "consumer", "Running"); err != nil {
		t.Fatal(err)
	}
	expectFile(ctx, t, ns, "consumer", "app.yaml", "v1")

	for i, workload := range []string{"daemonset/chandler-csi", "deployment/chandler-server", "deployment/chandler-provider-file"} {
		mustKubectl(ctx, t, "rollout", "restart", workload, "-n", driverNamespace)
		mustKubectl(ctx, t, "rollout", "status", workload, "-n", driverNamespace, "--timeout=180s")

		// The files stayed where they were while it was away.
		want := fmt.Sprintf("v%d", i+1)
		if got := mustKubectl(ctx, t, "exec", "consumer", "-n", ns, "--", "cat", mountPath+"/app.yaml"); got != want {
			t.Fatalf("after restarting %s the file reads %q, want %q", workload, got, want)
		}
		next := fmt.Sprintf("v%d", i+2)
		publish(ctx, t, key, next)
		expectFile(ctx, t, ns, "consumer", "app.yaml", next)
	}
}

// A pod must not start with part of its configuration: it waits, kubelet says
// why, and it starts once the missing source exists.
func TestPodWaitsForAMissingSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)
	key := ns + ".late.yaml"

	mustApply(ctx, t, ns, consumerPod("waiting", true, fmt.Sprintf("- uri: file:///%s\n  path: late.yaml\n", key)))
	err := eventually(ctx, func() error {
		events, err := kubectl(ctx, "get", "events", "-n", ns,
			"--field-selector", "involvedObject.name=waiting,reason=FailedMount",
			"-o", "jsonpath={.items[*].message}")
		if err != nil {
			return err
		}
		if !strings.Contains(events, "NotFound") || !strings.Contains(events, key) {
			return fmt.Errorf("no FailedMount event saying %s was not found yet; events: %q", key, events)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if phase := mustKubectl(ctx, t, "get", "pod", "waiting", "-n", ns, "-o", "jsonpath={.status.phase}"); phase != "Pending" {
		t.Errorf("pod phase = %q, want Pending", phase)
	}

	publish(ctx, t, key, "here now")
	if _, err := waitForPodPhase(ctx, ns, "waiting", "Running"); err != nil {
		t.Fatal(err)
	}
	expectFile(ctx, t, ns, "waiting", "late.yaml", "here now")
}

func TestWritableVolumeIsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)

	mustApply(ctx, t, ns, consumerPod("writable", false, "- uri: file:///app.yaml\n  path: app.yaml\n"))

	// The mount fails, so the pod never starts and kubelet says why.
	err := eventually(ctx, func() error {
		events, err := kubectl(ctx, "get", "events", "-n", ns,
			"--field-selector", "involvedObject.name=writable,reason=FailedMount",
			"-o", "jsonpath={.items[*].message}")
		if err != nil {
			return err
		}
		if !strings.Contains(events, "readOnly: true") {
			return fmt.Errorf("no FailedMount event naming readOnly yet; events: %q", events)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if phase := mustKubectl(ctx, t, "get", "pod", "writable", "-n", ns, "-o", "jsonpath={.status.phase}"); phase != "Pending" {
		t.Errorf("pod phase = %q, want Pending", phase)
	}
}
