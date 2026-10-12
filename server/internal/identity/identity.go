// Package identity verifies the service account token a pod's volume was
// mounted with, so that the server takes the pod's namespace, service account
// and UID from Kubernetes rather than from what a driver says.
//
// The token is a JWT signed by the cluster. It is verified offline, against
// the cluster's public signing keys, which are fetched and cached: mounting a
// volume does not depend on the API server being reachable at that moment,
// and the server needs no permission beyond reading those public keys.
//
// What offline verification cannot see is that a pod was deleted: its token
// stays valid until it expires.
package identity

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"time"
)

// ErrInvalid reports a token that does not prove a pod's identity.
var ErrInvalid = errors.New("invalid service account token")

// Identity is who a verified token says the pod is.
type Identity struct {
	Namespace      string
	ServiceAccount string
	PodName        string
	PodUID         string
}

// Keys supplies the cluster's token signing keys.
type Keys interface {
	// Key returns the public key with the given ID, or nil when there is
	// none. It may refresh from the cluster when the ID is unknown.
	Key(keyID string) crypto.PublicKey
}

// Verifier verifies tokens issued for one audience.
type Verifier struct {
	// Audience is the audience the token must have been requested for. A
	// token for another audience, such as the API server's, is refused, so a
	// token a pod can read from its own filesystem is of no use here.
	Audience string
	Keys     Keys
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

type header struct {
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
}

type claims struct {
	Audience   audience `json:"aud"`
	Expiry     float64  `json:"exp"`
	NotBefore  float64  `json:"nbf"`
	Kubernetes struct {
		Namespace string `json:"namespace"`
		Pod       *struct {
			Name string `json:"name"`
			UID  string `json:"uid"`
		} `json:"pod"`
		ServiceAccount struct {
			Name string `json:"name"`
		} `json:"serviceaccount"`
	} `json:"kubernetes.io"`
}

// audience is the JWT "aud" claim: one string or a list.
type audience []string

func (a *audience) UnmarshalJSON(raw []byte) error {
	var one string
	if json.Unmarshal(raw, &one) == nil {
		*a = []string{one}
		return nil
	}
	return json.Unmarshal(raw, (*[]string)(a))
}

// Verify checks a token and returns the pod it was issued to.
func (v Verifier) Verify(token string) (Identity, error) {
	if token == "" {
		return Identity{}, fmt.Errorf("%w: the subscription carries none", ErrInvalid)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Identity{}, fmt.Errorf("%w: not a JWT", ErrInvalid)
	}
	var h header
	var c claims
	if err := decode(parts[0], &h); err != nil {
		return Identity{}, fmt.Errorf("%w: header: %v", ErrInvalid, err)
	}
	if err := decode(parts[1], &c); err != nil {
		return Identity{}, fmt.Errorf("%w: claims: %v", ErrInvalid, err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Identity{}, fmt.Errorf("%w: signature: %v", ErrInvalid, err)
	}

	// The signature first: nothing in an unsigned token means anything.
	key := v.Keys.Key(h.KeyID)
	if key == nil {
		return Identity{}, fmt.Errorf("%w: signed with a key the cluster does not publish (%q)", ErrInvalid, h.KeyID)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	switch pub := key.(type) {
	case *rsa.PublicKey:
		if h.Algorithm != "RS256" || rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], signature) != nil {
			return Identity{}, fmt.Errorf("%w: bad signature", ErrInvalid)
		}
	case *ecdsa.PublicKey:
		// A JWS ECDSA signature is the two integers side by side.
		if h.Algorithm != "ES256" || len(signature) != 64 ||
			!ecdsa.Verify(pub, digest[:], new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])) {
			return Identity{}, fmt.Errorf("%w: bad signature", ErrInvalid)
		}
	default:
		return Identity{}, fmt.Errorf("%w: unsupported key type", ErrInvalid)
	}

	now := time.Now()
	if v.Now != nil {
		now = v.Now()
	}
	seconds := float64(now.Unix())
	switch {
	case c.Expiry == 0 || seconds >= c.Expiry:
		return Identity{}, fmt.Errorf("%w: expired", ErrInvalid)
	case c.NotBefore != 0 && seconds < c.NotBefore-60:
		return Identity{}, fmt.Errorf("%w: not valid yet", ErrInvalid)
	case !slices.Contains(c.Audience, v.Audience):
		return Identity{}, fmt.Errorf("%w: issued for audience %q, not %q", ErrInvalid, []string(c.Audience), v.Audience)
	case c.Kubernetes.Pod == nil || c.Kubernetes.Pod.UID == "":
		// A token that is not bound to a pod could be any workload of the
		// service account; the volume belongs to one pod.
		return Identity{}, fmt.Errorf("%w: not bound to a pod", ErrInvalid)
	case c.Kubernetes.Namespace == "" || c.Kubernetes.ServiceAccount.Name == "":
		return Identity{}, fmt.Errorf("%w: names no namespace or service account", ErrInvalid)
	}
	return Identity{
		Namespace:      c.Kubernetes.Namespace,
		ServiceAccount: c.Kubernetes.ServiceAccount.Name,
		PodName:        c.Kubernetes.Pod.Name,
		PodUID:         c.Kubernetes.Pod.UID,
	}, nil
}

func decode(segment string, into any) error {
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, into)
}
