package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Every other test in the suite already proves that the components reach each
// other over mutual TLS, since the chart turns it on by default. This one
// proves the other half: someone without a certificate gets nothing.
func TestServerRefusesClientsWithoutACertificate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ns := newNamespace(ctx, t)

	mustApply(ctx, t, ns, fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: stranger
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 0
  containers:
    - name: main
      image: %s
      command: ["sleep", "3600"]
`, curlImage))
	if _, err := waitForPodPhase(ctx, ns, "stranger", "Running"); err != nil {
		t.Fatal(err)
	}

	for name, port := range map[string]int{
		"stevedore-server":        9000,
		"stevedore-provider-file": 9100,
		"stevedore-provider-s3":   9100,
	} {
		host := fmt.Sprintf("%s.%s.svc:%d", name, driverNamespace, port)
		method := "stevedore.v1.ConfigProvider/StreamConfigs"
		if name == "stevedore-server" {
			method = "stevedore.v1.ConfigDiscoveryService/StreamConfigs"
		}

		// In the clear: the server only speaks TLS.
		out, err := kubectl(ctx, "exec", "stranger", "-n", ns, "--",
			"curl", "-sS", "--max-time", "10", "--http2-prior-knowledge", "-X", "POST",
			"-H", "content-type: application/grpc", "http://"+host+"/"+method)
		if err == nil {
			t.Errorf("%s answered a plaintext request: %q", name, out)
		}

		// Over TLS, trusting anything, but with no certificate of its own.
		out, err = kubectl(ctx, "exec", "stranger", "-n", ns, "--",
			"curl", "-sS", "-k", "--max-time", "10", "--http2", "-X", "POST",
			"-H", "content-type: application/grpc", "-H", "te: trailers", "https://"+host+"/"+method)
		switch {
		case err == nil:
			t.Errorf("%s answered a client with no certificate: %q", name, out)
		case !strings.Contains(strings.ToLower(err.Error()), "certificate") && !strings.Contains(strings.ToLower(err.Error()), "alert"):
			t.Errorf("%s refused a client with no certificate, but not for that reason: %v", name, err)
		}
	}
}
