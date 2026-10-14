// Package events records Kubernetes events on the pods the driver serves, so
// that what happened to a pod's configuration shows where people look first:
// `kubectl describe pod`.
//
// It talks to the API server directly, with the driver's service account and
// one permission, creating events. Recording is best effort and never blocks
// the driver: events are queued, sent in the background, and dropped when the
// API server is away or the queue is full.
package events

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	// Component is the name events are reported under.
	Component = "stevedore-csi"

	tokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	caFile    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

	// An update that keeps failing is one fact, not one event per retry.
	repeatAfter = time.Minute
	queueSize   = 256
	maxMessage  = 1024
)

// The reasons events are recorded with.
const (
	// ReasonRefused: an update can never be written to this volume.
	ReasonRefused = "ConfigRefused"
	// ReasonWriteFailed: an update could not be written for a reason on the
	// node; it is retried.
	ReasonWriteFailed = "ConfigWriteFailed"
	// ReasonFromCache: the volume was mounted from the node cache because
	// the server could not supply a source.
	ReasonFromCache = "ConfigFromCache"
	// ReasonStale: the server delivered a version it could not confirm with
	// the provider.
	ReasonStale = "ConfigStale"
	// ReasonRejected: the application rejected a version.
	ReasonRejected = "ConfigRejected"
	// ReasonOverdue: the application has not reported on a version in time.
	ReasonOverdue = "ConfigOverdue"
)

// Pod is the pod an event is about.
type Pod struct {
	Namespace, Name, UID string
}

// Recorder records events on pods.
type Recorder interface {
	// Warning records that something went wrong for a pod's configuration.
	Warning(pod Pod, reason, message string)
}

// Discard is a Recorder that records nothing.
type Discard struct{}

// Warning implements Recorder.
func (Discard) Warning(Pod, string, string) {}

// API records events through the Kubernetes API.
type API struct {
	// URL is the API server, e.g. https://10.96.0.1:443.
	URL string
	// Node is reported as the host the events come from.
	Node string
	// TokenFile and CAFile are the service account's; empty uses the
	// in-cluster paths.
	TokenFile, CAFile string
	Log               *slog.Logger

	once   sync.Once
	queue  chan event
	client *http.Client

	mu   sync.Mutex
	last map[string]time.Time
}

type event struct {
	pod             Pod
	reason, message string
	at              time.Time
}

// InCluster returns a Recorder for the cluster the driver runs in.
func InCluster(node string, log *slog.Logger) (*API, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("not running in a cluster: KUBERNETES_SERVICE_HOST is not set")
	}
	a := &API{URL: "https://" + net.JoinHostPort(host, port), Node: node, Log: log}
	if _, err := os.Stat(tokenFile); err != nil {
		return nil, fmt.Errorf("no service account token: %w", err)
	}
	return a, nil
}

// Warning implements Recorder. It never blocks.
func (a *API) Warning(pod Pod, reason, message string) {
	if pod.Namespace == "" || pod.Name == "" {
		return
	}
	a.once.Do(a.start)

	now := time.Now()
	key := pod.UID + "/" + reason
	a.mu.Lock()
	if last, ok := a.last[key]; ok && now.Sub(last) < repeatAfter {
		a.mu.Unlock()
		return
	}
	a.last[key] = now
	// Forget pods that are long gone.
	if len(a.last) > 4*queueSize {
		for k, t := range a.last {
			if now.Sub(t) >= repeatAfter {
				delete(a.last, k)
			}
		}
	}
	a.mu.Unlock()

	if len(message) > maxMessage {
		message = message[:maxMessage]
	}
	select {
	case a.queue <- event{pod: pod, reason: reason, message: message, at: now}:
	default:
		// Dropped: the driver's work comes before telling about it.
	}
}

func (a *API) start() {
	a.queue = make(chan event, queueSize)
	a.last = make(map[string]time.Time)
	go func() {
		for e := range a.queue {
			if err := a.post(e); err != nil {
				a.Log.Warn("could not record an event on a pod", "pod", e.pod.Namespace+"/"+e.pod.Name,
					"reason", e.reason, "error", err)
			}
		}
	}()
}

func (a *API) post(e event) error {
	tokenPath, caPath := a.TokenFile, a.CAFile
	if tokenPath == "" {
		tokenPath = tokenFile
	}
	if caPath == "" {
		caPath = caFile
	}
	// Read every time: the token is rotated under the driver.
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		return err
	}
	if a.client == nil {
		ca, err := os.ReadFile(caPath)
		if err != nil {
			return err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return fmt.Errorf("%s holds no certificate", caPath)
		}
		a.client = &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
		}
	}

	stamp := e.at.UTC().Format(time.RFC3339)
	body, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "Event",
		"metadata":   map[string]any{"generateName": e.pod.Name + ".", "namespace": e.pod.Namespace},
		"involvedObject": map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"namespace": e.pod.Namespace, "name": e.pod.Name, "uid": e.pod.UID,
		},
		"type":               "Warning",
		"reason":             e.reason,
		"message":            e.message,
		"firstTimestamp":     stamp,
		"lastTimestamp":      stamp,
		"count":              1,
		"source":             map[string]any{"component": Component, "host": a.Node},
		"reportingComponent": Component,
		"reportingInstance":  a.Node,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := fmt.Sprintf("%s/api/v1/namespaces/%s/events", strings.TrimSuffix(a.URL, "/"), e.pod.Namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("the API server answered %s: %s", resp.Status, strings.TrimSpace(string(detail)))
	}
	return nil
}
