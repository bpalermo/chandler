// Package transport secures the gRPC connections between Chandler's
// components with mutual TLS.
//
// Both ends present a certificate. Each verifies that the other's chains to a
// trusted root, and then that it carries an identity it expects: a DNS name or
// a URI (a SPIFFE ID) from the certificate's subject alternative names. Who
// may call what is decided by identity, never by network position.
//
// Where the certificate and the roots come from is a Source, so that files
// written by anything (a Secret, cert-manager) and a SPIFFE Workload API are
// interchangeable. A Source is asked on every handshake, which is what makes
// rotation work without a restart.
package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Source supplies this workload's certificate and the roots it trusts. Both
// are read at every handshake and may change over time.
type Source interface {
	// Certificate returns the certificate to present, with its private key.
	Certificate() (*tls.Certificate, error)
	// Roots returns the certificate authorities peers must chain to.
	Roots() (*x509.CertPool, error)
}

// Files is a Source that reads PEM files, and reads them again when they
// change: a rotated Secret is picked up without a restart.
type Files struct {
	// Cert and Key are this workload's certificate chain and private key.
	Cert, Key string
	// CA is the bundle of roots peers must chain to.
	CA string

	mu       sync.Mutex
	cert     *tls.Certificate
	certTime [2]time.Time
	roots    *x509.CertPool
	caTime   time.Time
}

// Certificate implements Source.
func (f *Files) Certificate() (*tls.Certificate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	certInfo, err := os.Stat(f.Cert)
	if err != nil {
		return nil, fmt.Errorf("TLS certificate: %w", err)
	}
	keyInfo, err := os.Stat(f.Key)
	if err != nil {
		return nil, fmt.Errorf("TLS key: %w", err)
	}
	times := [2]time.Time{certInfo.ModTime(), keyInfo.ModTime()}
	if f.cert != nil && times == f.certTime {
		return f.cert, nil
	}
	cert, err := tls.LoadX509KeyPair(f.Cert, f.Key)
	if err != nil {
		// Mid-rotation the two files may not match yet: keep what works.
		if f.cert != nil {
			return f.cert, nil
		}
		return nil, fmt.Errorf("TLS certificate: %w", err)
	}
	f.cert, f.certTime = &cert, times
	return f.cert, nil
}

// Roots implements Source.
func (f *Files) Roots() (*x509.CertPool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	info, err := os.Stat(f.CA)
	if err != nil {
		return nil, fmt.Errorf("TLS roots: %w", err)
	}
	if f.roots != nil && info.ModTime().Equal(f.caTime) {
		return f.roots, nil
	}
	pem, err := os.ReadFile(f.CA)
	if err != nil {
		return nil, fmt.Errorf("TLS roots: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		if f.roots != nil {
			return f.roots, nil
		}
		return nil, fmt.Errorf("TLS roots: %s holds no certificate", f.CA)
	}
	f.roots, f.caTime = pool, info.ModTime()
	return f.roots, nil
}

// Identities returns the names a certificate gives its holder: the URIs and
// DNS names among its subject alternative names.
func Identities(cert *x509.Certificate) []string {
	var ids []string
	for _, u := range cert.URIs {
		ids = append(ids, u.String())
	}
	return append(ids, cert.DNSNames...)
}

// verify checks that a peer's certificate chains to the source's roots for
// the given use and returns the leaf.
func verify(src Source, raw [][]byte, usage x509.ExtKeyUsage) (*x509.Certificate, error) {
	if len(raw) == 0 {
		return nil, errors.New("the peer presented no certificate")
	}
	roots, err := src.Roots()
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(raw[0])
	if err != nil {
		return nil, fmt.Errorf("the peer's certificate: %w", err)
	}
	intermediates := x509.NewCertPool()
	for _, der := range raw[1:] {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, fmt.Errorf("the peer's certificate chain: %w", err)
		}
		intermediates.AddCert(cert)
	}
	_, err = leaf.Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{usage},
	})
	if err != nil {
		return nil, fmt.Errorf("the peer's certificate: %w", err)
	}
	return leaf, nil
}

// ServerCredentials returns credentials for a gRPC server that presents the
// source's certificate and requires every client to present one that chains
// to the source's roots. Which clients may call which method is then decided
// with Authorize.
func ServerCredentials(src Source) credentials.TransportCredentials {
	return credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return src.Certificate()
		},
		// The chain is verified below, against roots that may have been
		// rotated since the listener started.
		ClientAuth: tls.RequireAnyClientCert,
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			_, err := verify(src, raw, x509.ExtKeyUsageClientAuth)
			return err
		},
	})
}

// ClientCredentials returns credentials for a gRPC client that presents the
// source's certificate and accepts a server only if its certificate chains to
// the source's roots and carries one of the given identities.
//
// The identity is what is checked, not the address dialled: a replica reached
// by pod IP is still verified to be a replica.
func ClientCredentials(src Source, identities []string) credentials.TransportCredentials {
	return credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return src.Certificate()
		},
		// Verified below: by identity rather than by the dialled host name,
		// and against roots that may have been rotated.
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			leaf, err := verify(src, raw, x509.ExtKeyUsageServerAuth)
			if err != nil {
				return err
			}
			got := Identities(leaf)
			for _, id := range got {
				if slices.Contains(identities, id) {
					return nil
				}
			}
			return fmt.Errorf("the server identifies as %q, want one of %q", got, identities)
		},
	})
}

// Insecure returns credentials that neither encrypt nor authenticate. For
// tests and for deployments that explicitly opt out.
func Insecure() credentials.TransportCredentials { return insecure.NewCredentials() }

// Authorize reports whether the caller of a gRPC method presented a
// certificate carrying one of the allowed identities. The error is a gRPC
// status. A nil allowed list means the server runs without TLS, and allows
// everyone.
func Authorize(ctx context.Context, allowed []string) error {
	if allowed == nil {
		return nil
	}
	p, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "no peer information")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.PeerCertificates) == 0 {
		return status.Error(codes.Unauthenticated, "the caller presented no certificate")
	}
	got := Identities(info.State.PeerCertificates[0])
	for _, id := range got {
		if slices.Contains(allowed, id) {
			return nil
		}
	}
	return status.Errorf(codes.PermissionDenied, "identity %q may not call this", got)
}

// Authorizer returns server options that apply Authorize to every call, with
// the identities allowed for the method called. allowed returns nil for a
// method anyone may call.
func Authorizer(allowed func(fullMethod string) []string) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := Authorize(ctx, allowed(info.FullMethod)); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := Authorize(ss.Context(), allowed(info.FullMethod)); err != nil {
				return err
			}
			return handler(srv, ss)
		}),
	}
}
