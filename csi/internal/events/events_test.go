package events

import (
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type received struct {
	path, authorization string
	body                map[string]any
}

// apiServer is a stand-in for the Kubernetes API server.
func apiServer(t *testing.T, status int) (*API, <-chan received) {
	t.Helper()
	got := make(chan received, 16)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		got <- received{path: r.Method + " " + r.URL.Path, authorization: r.Header.Get("Authorization"), body: body}
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)

	dir := t.TempDir()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), ca, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "token"), []byte("driver-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &API{
		URL: server.URL, Node: "node-1",
		TokenFile: filepath.Join(dir, "token"), CAFile: filepath.Join(dir, "ca.crt"),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, got
}

func next(t *testing.T, got <-chan received) received {
	t.Helper()
	select {
	case r := <-got:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("no event reached the API server")
		return received{}
	}
}

func TestEventIsRecordedOnThePod(t *testing.T) {
	api, got := apiServer(t, http.StatusCreated)
	pod := Pod{Namespace: "shop", Name: "web-0", UID: "uid-1"}
	api.Warning(pod, ReasonRejected, "source \"app\": cannot parse")

	r := next(t, got)
	if r.path != "POST /api/v1/namespaces/shop/events" || r.authorization != "Bearer driver-token" {
		t.Errorf("request = %s with %q", r.path, r.authorization)
	}
	object := r.body["involvedObject"].(map[string]any)
	if object["kind"] != "Pod" || object["name"] != "web-0" || object["uid"] != "uid-1" || object["namespace"] != "shop" {
		t.Errorf("involvedObject = %v, want the pod", object)
	}
	if r.body["reason"] != ReasonRejected || r.body["type"] != "Warning" || !strings.Contains(r.body["message"].(string), "cannot parse") {
		t.Errorf("event = %v", r.body)
	}
	if source := r.body["source"].(map[string]any); source["component"] != Component || source["host"] != "node-1" {
		t.Errorf("source = %v", source)
	}

	// The same thing happening again is not told again at once...
	api.Warning(pod, ReasonRejected, "again")
	// ...but another reason, or another pod, is.
	api.Warning(pod, ReasonOverdue, "late")
	api.Warning(Pod{Namespace: "shop", Name: "web-1", UID: "uid-2"}, ReasonRejected, "other pod")
	for _, want := range []string{"late", "other pod"} {
		if r := next(t, got); r.body["message"] != want {
			t.Errorf("got the event %q, want %q", r.body["message"], want)
		}
	}
	select {
	case r := <-got:
		t.Errorf("a repeated event was recorded: %v", r.body["message"])
	case <-time.After(200 * time.Millisecond):
	}

	// A pod the driver cannot name gets no event, and nothing breaks.
	api.Warning(Pod{}, ReasonRefused, "nobody")
	Discard{}.Warning(pod, ReasonRefused, "nowhere")
}

// The API server refusing or being away must cost the driver nothing.
func TestEventFailuresAreSwallowed(t *testing.T) {
	api, got := apiServer(t, http.StatusForbidden)
	api.Warning(Pod{Namespace: "shop", Name: "web-0", UID: "uid-1"}, ReasonRefused, "x")
	next(t, got)

	gone := &API{URL: "https://127.0.0.1:1", Node: "n", TokenFile: api.TokenFile, CAFile: api.CAFile, Log: api.Log}
	start := time.Now()
	for i := range 1000 {
		gone.Warning(Pod{Namespace: "shop", Name: "web", UID: string(rune('a' + i%26))}, ReasonRefused, "x")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("recording 1000 events with no API server took %s; it must not block", elapsed)
	}
}
