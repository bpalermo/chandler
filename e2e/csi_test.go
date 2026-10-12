package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

const (
	driverName      = "csi.chandler.dev"
	driverNamespace = "chandler-system"

	// busybox 1.37, pinned so the suite does not depend on a moving tag.
	busybox = "docker.io/library/busybox@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e"

	mountPath = "/etc/chandler"
)

// consumerPod is a pod that mounts a Chandler inline volume.
func consumerPod(name string, readOnly bool) string {
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
`, name, busybox, mountPath, driverName, readOnly)
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

func TestInlineVolumeLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)

	mustApply(ctx, t, ns, consumerPod("consumer", true))
	mustApply(ctx, t, ns, inspectorPod)
	for _, pod := range []string{"consumer", "inspector"} {
		if _, err := waitForPodPhase(ctx, ns, pod, "Running"); err != nil {
			t.Fatal(err)
		}
	}

	// The volume was written by the atomic writer before it was mounted.
	generation := mustKubectl(ctx, t, "exec", "consumer", "-n", ns, "--", "readlink", mountPath+"/..data")
	if !strings.HasPrefix(generation, "..") {
		t.Errorf("..data -> %q, want a generation directory", generation)
	}

	// It is mounted read-only, whatever the container runs as.
	if out, err := kubectl(ctx, "exec", "consumer", "-n", ns, "--", "touch", mountPath+"/probe"); err == nil {
		t.Errorf("writing into the volume succeeded: %s", out)
	} else if !strings.Contains(err.Error(), "Read-only file system") {
		t.Errorf("writing into the volume failed for the wrong reason: %v", err)
	}

	// The driver keeps exactly this pod's volume on the node...
	volumes := strings.Fields(mustKubectl(ctx, t, "exec", "inspector", "-n", ns, "--", "ls", "/state/volumes"))
	if len(volumes) != 1 {
		t.Fatalf("volumes on the node = %v, want exactly one", volumes)
	}

	// ...and removes it when the pod goes away.
	mustKubectl(ctx, t, "delete", "pod", "consumer", "-n", ns, "--wait=true")
	err := eventually(ctx, func() error {
		left, err := kubectl(ctx, "exec", "inspector", "-n", ns, "--", "ls", "/state/volumes")
		if err != nil {
			return err
		}
		if strings.TrimSpace(left) != "" {
			return fmt.Errorf("volumes still on the node: %q", strings.TrimSpace(left))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestWritableVolumeIsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)

	mustApply(ctx, t, ns, consumerPod("writable", false))

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
