package identity

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	// InClusterJWKS is where a cluster publishes its token signing keys.
	InClusterJWKS = "https://kubernetes.default.svc/openid/v1/jwks"
	// The server's own service account, which is what lets it read them.
	inClusterToken = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	inClusterCA    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

	// refreshInterval is how often the keys are fetched again, so that a
	// rotated-out key stops being trusted.
	refreshInterval = 10 * time.Minute
	// refreshFloor is the shortest time between fetches caused by tokens
	// naming unknown keys, so that bad tokens cannot make the server
	// hammer the API server.
	refreshFloor = 10 * time.Second

	maxJWKSBytes = 1 << 20
)

// JWKS is the cluster's signing keys, fetched from a URL or read from a file,
// and cached.
type JWKS struct {
	// URL is where the key set is fetched from; File is read instead when
	// set.
	URL, File string
	// TokenFile and CAFile authenticate to and verify the URL's server;
	// empty uses the in-cluster service account.
	TokenFile, CAFile string
	Log               *slog.Logger

	mu      sync.Mutex
	keys    map[string]crypto.PublicKey
	fetched time.Time
}

type jwk struct {
	KeyType string `json:"kty"`
	KeyID   string `json:"kid"`
	Curve   string `json:"crv"`
	N, E    string
	X, Y    string
}

// Key implements Keys. An unknown key ID triggers a fetch, at most once per
// refreshFloor: the cluster may have rotated its keys.
func (j *JWKS) Key(keyID string) crypto.PublicKey {
	j.mu.Lock()
	defer j.mu.Unlock()
	now := time.Now()
	key, known := j.keys[keyID]
	stale := now.Sub(j.fetched) > refreshInterval
	if (!known && now.Sub(j.fetched) > refreshFloor) || stale {
		if err := j.fetchLocked(); err != nil {
			j.Log.Warn("could not fetch the cluster's token signing keys", "error", err)
			// Keep what is cached: an API server outage must not stop
			// tokens from being verified.
		}
		j.fetched = now
		key = j.keys[keyID]
	}
	return key
}

// Load fetches the keys once, so that a misconfiguration shows at start.
func (j *JWKS) Load() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.fetched = time.Now()
	return j.fetchLocked()
}

func (j *JWKS) fetchLocked() error {
	raw, err := j.read()
	if err != nil {
		return err
	}
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.Unmarshal(raw, &set); err != nil {
		return fmt.Errorf("parse the key set: %w", err)
	}
	keys := make(map[string]crypto.PublicKey)
	for _, k := range set.Keys {
		key, err := k.public()
		if err != nil {
			continue // a key type this server does not verify with
		}
		keys[k.KeyID] = key
	}
	if len(keys) == 0 {
		return errors.New("the key set holds no usable key")
	}
	j.keys = keys
	return nil
}

func (j *JWKS) read() ([]byte, error) {
	if j.File != "" {
		return os.ReadFile(j.File)
	}
	tokenFile, caFile := j.TokenFile, j.CAFile
	if tokenFile == "" {
		tokenFile = inClusterToken
	}
	if caFile == "" {
		caFile = inClusterCA
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read the API server's CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("%s holds no certificate", caFile)
	}
	token, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read this server's service account token: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s answered %s", j.URL, resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxJWKSBytes))
}

func (k jwk) public() (crypto.PublicKey, error) {
	number := func(s string) (*big.Int, error) {
		raw, err := base64.RawURLEncoding.DecodeString(s)
		if err != nil || len(raw) == 0 {
			return nil, errors.New("bad key parameter")
		}
		return new(big.Int).SetBytes(raw), nil
	}
	switch k.KeyType {
	case "RSA":
		n, err := number(k.N)
		if err != nil {
			return nil, err
		}
		e, err := number(k.E)
		if err != nil || !e.IsInt64() || e.Int64() > 1<<31-1 {
			return nil, errors.New("bad exponent")
		}
		return &rsa.PublicKey{N: n, E: int(e.Int64())}, nil
	case "EC":
		if k.Curve != "P-256" {
			return nil, errors.New("unsupported curve")
		}
		x, err := number(k.X)
		if err != nil {
			return nil, err
		}
		y, err := number(k.Y)
		if err != nil {
			return nil, err
		}
		return &ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}, nil
	}
	return nil, errors.New("unsupported key type")
}
