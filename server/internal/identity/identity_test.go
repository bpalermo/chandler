package identity

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var now = time.Unix(1_700_000_000, 0)

type staticKeys map[string]crypto.PublicKey

func (s staticKeys) Key(id string) crypto.PublicKey { return s[id] }

func b64(v any) string {
	raw, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// podClaims is what kubelet's token for a CSI driver carries.
func podClaims() map[string]any {
	return map[string]any{
		"aud": []string{"chandler"},
		"exp": now.Add(time.Hour).Unix(),
		"nbf": now.Add(-time.Minute).Unix(),
		"iss": "https://kubernetes.default.svc.cluster.local",
		"sub": "system:serviceaccount:shop:web",
		"kubernetes.io": map[string]any{
			"namespace":      "shop",
			"pod":            map[string]any{"name": "web-0", "uid": "uid-1"},
			"serviceaccount": map[string]any{"name": "web", "uid": "sa-uid"},
		},
	}
}

func signRSA(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	signing := b64(map[string]string{"alg": "RS256", "kid": kid}) + "." + b64(claims)
	digest := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func signEC(t *testing.T, key *ecdsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	signing := b64(map[string]string{"alg": "ES256", "kid": kid}) + "." + b64(claims)
	digest := sha256.Sum256([]byte(signing))
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func TestVerify(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ecKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	otherKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	v := Verifier{
		Audience: "chandler",
		Keys:     staticKeys{"rsa": &rsaKey.PublicKey, "ec": &ecKey.PublicKey},
		Now:      func() time.Time { return now },
	}

	want := Identity{Namespace: "shop", ServiceAccount: "web", PodName: "web-0", PodUID: "uid-1"}
	for name, token := range map[string]string{
		"RS256": signRSA(t, rsaKey, "rsa", podClaims()),
		"ES256": signEC(t, ecKey, "ec", podClaims()),
	} {
		if got, err := v.Verify(token); err != nil || got != want {
			t.Errorf("%s: Verify = %+v, %v; want %+v", name, got, err, want)
		}
	}
	// "aud" may be a single string.
	single := podClaims()
	single["aud"] = "chandler"
	if _, err := v.Verify(signRSA(t, rsaKey, "rsa", single)); err != nil {
		t.Errorf("a single-string audience: %v", err)
	}

	with := func(mutate func(map[string]any)) string {
		c := podClaims()
		mutate(c)
		return signRSA(t, rsaKey, "rsa", c)
	}
	good := signRSA(t, rsaKey, "rsa", podClaims())
	parts := strings.Split(good, ".")
	forged := podClaims()
	forged["kubernetes.io"].(map[string]any)["namespace"] = "kube-system"

	for name, token := range map[string]string{
		"no token":      "",
		"not a JWT":     "hello",
		"expired":       with(func(c map[string]any) { c["exp"] = now.Add(-time.Second).Unix() }),
		"no expiry":     with(func(c map[string]any) { delete(c, "exp") }),
		"not valid yet": with(func(c map[string]any) { c["nbf"] = now.Add(time.Hour).Unix() }),
		// The token every pod can read from its own filesystem.
		"the API server's audience": with(func(c map[string]any) { c["aud"] = []string{"https://kubernetes.default.svc"} }),
		"not bound to a pod":        with(func(c map[string]any) { delete(c["kubernetes.io"].(map[string]any), "pod") }),
		"no namespace":              with(func(c map[string]any) { delete(c["kubernetes.io"].(map[string]any), "namespace") }),
		"signed by someone else":    signRSA(t, otherKey, "rsa", podClaims()),
		"an unknown key":            signRSA(t, rsaKey, "rotated-out", podClaims()),
		// Claims swapped under a signature made for other claims.
		"claims changed after signing": parts[0] + "." + b64(forged) + "." + parts[2],
		"no signature":                 parts[0] + "." + parts[1] + ".",
		// An attacker choosing the algorithm must not choose the outcome.
		"alg none": b64(map[string]string{"alg": "none", "kid": "rsa"}) + "." + parts[1] + ".",
		"EC key claimed as RS256": func() string {
			signing := b64(map[string]string{"alg": "RS256", "kid": "ec"}) + "." + b64(podClaims())
			return signing + "." + parts[2]
		}(),
	} {
		if got, err := v.Verify(token); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: Verify = %+v, %v; want ErrInvalid", name, got, err)
		}
	}
}

func TestJWKSFromFile(t *testing.T) {
	rsaKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	ecKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	enc := func(n *big.Int) string { return base64.RawURLEncoding.EncodeToString(n.Bytes()) }
	set := map[string]any{"keys": []map[string]string{
		{"kty": "RSA", "kid": "rsa", "alg": "RS256", "use": "sig", "n": enc(rsaKey.N), "e": enc(big.NewInt(int64(rsaKey.E)))},
		{"kty": "EC", "kid": "ec", "crv": "P-256", "x": enc(ecKey.X), "y": enc(ecKey.Y)},
		{"kty": "OKP", "kid": "ed", "crv": "Ed25519", "x": "AAAA"}, // not something we verify with
	}}
	raw, _ := json.Marshal(set)
	file := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(file, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	keys := &JWKS{File: file, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := keys.Load(); err != nil {
		t.Fatal(err)
	}
	v := Verifier{Audience: "chandler", Keys: keys, Now: func() time.Time { return now }}
	if _, err := v.Verify(signRSA(t, rsaKey, "rsa", podClaims())); err != nil {
		t.Errorf("a token signed with the published RSA key: %v", err)
	}
	if _, err := v.Verify(signEC(t, ecKey, "ec", podClaims())); err != nil {
		t.Errorf("a token signed with the published EC key: %v", err)
	}
	if keys.Key("ed") != nil || keys.Key("nope") != nil {
		t.Error("a key that cannot be used, or does not exist, was returned")
	}

	empty := &JWKS{File: filepath.Join(t.TempDir(), "missing"), Log: keys.Log}
	if err := empty.Load(); err == nil {
		t.Error("a missing key set loaded")
	}
}
