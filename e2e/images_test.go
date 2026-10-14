package e2e

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestImagesRunInCluster checks the delivery pipeline itself: each image built
// from this checkout is present on the node, starts as a pod under the
// restricted settings it will be deployed with, and runs the binary it is
// named after.
func TestImagesRunInCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)

	for _, name := range []string{"stevedore-server", "stevedore-csi", "stevedore-provider-s3", "stevedore-provider-file"} {
		t.Run(name, func(t *testing.T) {
			mustKubectl(ctx, t, "run", name, "-n", ns,
				"--image="+image(name),
				// Never pull: the image must be the one loaded from this build.
				"--image-pull-policy=Never",
				"--restart=Never",
				"--", "--version")

			phase, err := waitForPodPhase(ctx, ns, name, "Succeeded", "Failed")
			if err != nil {
				t.Fatal(err)
			}
			logs := mustKubectl(ctx, t, "logs", name, "-n", ns)
			if phase != "Succeeded" {
				t.Fatalf("pod ended in phase %s; logs:\n%s", phase, logs)
			}
			if !strings.HasPrefix(logs, name+" ") {
				t.Errorf("logs = %q, want them to start with %q", logs, name+" ")
			}
		})
	}
}
