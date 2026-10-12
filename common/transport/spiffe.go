package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/spiffe/go-spiffe/v2/workloadapi"
)

// SPIFFE is a Source backed by a SPIFFE Workload API, such as a SPIRE agent's
// socket. The certificate is the workload's X.509 SVID and the roots are its
// trust domain's bundle; both are kept current by the Workload API, so there
// is nothing to rotate and no Secret to manage. Peers are then named by
// SPIFFE ID.
type SPIFFE struct {
	source *workloadapi.X509Source
}

// NewSPIFFE connects to the Workload API at addr, for example
// unix:///spiffe-workload-api/spire-agent.sock, and waits until it has issued
// this workload an identity or ctx ends.
func NewSPIFFE(ctx context.Context, addr string) (*SPIFFE, error) {
	source, err := workloadapi.NewX509Source(ctx, workloadapi.WithClientOptions(workloadapi.WithAddr(addr)))
	if err != nil {
		return nil, fmt.Errorf("get an identity from the SPIFFE Workload API at %s: %w", addr, err)
	}
	return &SPIFFE{source: source}, nil
}

// ID returns this workload's SPIFFE ID.
func (s *SPIFFE) ID() (string, error) {
	svid, err := s.source.GetX509SVID()
	if err != nil {
		return "", err
	}
	return svid.ID.String(), nil
}

// Certificate implements Source.
func (s *SPIFFE) Certificate() (*tls.Certificate, error) {
	svid, err := s.source.GetX509SVID()
	if err != nil {
		return nil, fmt.Errorf("SPIFFE identity: %w", err)
	}
	cert := &tls.Certificate{PrivateKey: svid.PrivateKey, Leaf: svid.Certificates[0]}
	for _, c := range svid.Certificates {
		cert.Certificate = append(cert.Certificate, c.Raw)
	}
	return cert, nil
}

// Roots implements Source: the authorities of this workload's own trust
// domain. A peer from another trust domain is not trusted.
func (s *SPIFFE) Roots() (*x509.CertPool, error) {
	svid, err := s.source.GetX509SVID()
	if err != nil {
		return nil, fmt.Errorf("SPIFFE identity: %w", err)
	}
	bundle, err := s.source.GetX509BundleForTrustDomain(svid.ID.TrustDomain())
	if err != nil {
		return nil, fmt.Errorf("SPIFFE trust bundle: %w", err)
	}
	pool := x509.NewCertPool()
	for _, authority := range bundle.X509Authorities() {
		pool.AddCert(authority)
	}
	return pool, nil
}

// Close stops following the Workload API.
func (s *SPIFFE) Close() error { return s.source.Close() }
