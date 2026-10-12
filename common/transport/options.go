package transport

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Options is how one process secures its connections, as command-line flags.
type Options struct {
	// Cert, Key and CA are the PEM files of a Files source.
	Cert, Key, CA string
	// SPIFFESocket is the address of a SPIFFE Workload API, used instead of
	// files.
	SPIFFESocket string
	// Insecure runs without TLS: nothing is encrypted and nobody is
	// authenticated.
	Insecure bool
}

// Register adds the flags to the default flag set.
func (o *Options) Register() {
	flag.StringVar(&o.Cert, "tls-cert", "", "PEM file with this component's certificate chain")
	flag.StringVar(&o.Key, "tls-key", "", "PEM file with the certificate's private key")
	flag.StringVar(&o.CA, "tls-ca", "", "PEM file with the certificate authorities the other components must chain to")
	flag.StringVar(&o.SPIFFESocket, "spiffe-socket", "", "address of a SPIFFE Workload API to get this component's identity from instead of files, e.g. unix:///spiffe-workload-api/spire-agent.sock")
	flag.BoolVar(&o.Insecure, "insecure", false, "run without TLS: connections are neither encrypted nor authenticated")
}

// Source returns where certificates come from, or nil when the process was
// told to run without TLS. Running without TLS is never a default: it has to
// be asked for. With a SPIFFE socket it waits for the Workload API to issue
// an identity, until ctx ends.
func (o *Options) Source(ctx context.Context) (Source, error) {
	files := o.Cert != "" || o.Key != "" || o.CA != ""
	switch {
	case o.Insecure && (files || o.SPIFFESocket != ""):
		return nil, errors.New("--insecure cannot be combined with the --tls flags or --spiffe-socket")
	case o.Insecure:
		return nil, nil
	case o.SPIFFESocket != "" && files:
		return nil, errors.New("--spiffe-socket cannot be combined with --tls-cert, --tls-key or --tls-ca")
	case o.SPIFFESocket != "":
		wait, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		return NewSPIFFE(wait, o.SPIFFESocket)
	case o.Cert == "" || o.Key == "" || o.CA == "":
		return nil, errors.New("--tls-cert, --tls-key and --tls-ca are all required, or --spiffe-socket (or --insecure to run without TLS)")
	}
	src := &Files{Cert: o.Cert, Key: o.Key, CA: o.CA}
	// Fail at start rather than at the first handshake.
	if _, err := src.Certificate(); err != nil {
		return nil, err
	}
	if _, err := src.Roots(); err != nil {
		return nil, err
	}
	return src, nil
}

// Dial returns the credentials to dial a server that must carry one of the
// identities. A nil source dials without TLS.
func Dial(src Source, identities []string) credentials.TransportCredentials {
	if src == nil {
		return Insecure()
	}
	return ClientCredentials(src, identities)
}

// Serve returns the server options that make a gRPC server require mutual
// TLS and allow each method only to the identities allowed returns for it. A
// nil source serves without TLS and allows everyone.
func Serve(src Source, allowed func(fullMethod string) []string) []grpc.ServerOption {
	if src == nil {
		return nil
	}
	return append(Authorizer(allowed), grpc.Creds(ServerCredentials(src)))
}

// HostIdentity returns the identity a server at a gRPC target is expected to
// carry when none is configured: the target's host name, which is the DNS
// name a certificate issued for a Kubernetes Service carries.
func HostIdentity(target string) string {
	if i := strings.LastIndex(target, "///"); i >= 0 {
		target = target[i+3:] // dns:///host:port
	}
	host, _, err := net.SplitHostPort(target)
	if err != nil {
		return target
	}
	return host
}

// Health serves the gRPC health service, and nothing else, without TLS on its
// own port. kubelet's gRPC probes cannot present a certificate, and the
// health of a process is not a secret.
func Health(listen string) (*health.Server, func(), error) {
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, nil, fmt.Errorf("listen on %s: %w", listen, err)
	}
	server := grpc.NewServer()
	checks := health.NewServer()
	healthpb.RegisterHealthServer(server, checks)
	go server.Serve(listener)
	return checks, server.Stop, nil
}
