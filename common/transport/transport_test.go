package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// authority is a test certificate authority.
type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newAuthority(t *testing.T, name string) *authority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &authority{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue writes a certificate for the identities (DNS names, or URIs when they
// contain "://") into a new directory and returns a Files source over it.
func (a *authority) issue(t *testing.T, identities ...string) *Files {
	t.Helper()
	dir := t.TempDir()
	a.write(t, dir, identities...)
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), a.pem, 0o600); err != nil {
		t.Fatal(err)
	}
	return &Files{Cert: filepath.Join(dir, "tls.crt"), Key: filepath.Join(dir, "tls.key"), CA: filepath.Join(dir, "ca.crt")}
}

func (a *authority) write(t *testing.T, dir string, identities ...string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	for _, id := range identities {
		if strings.Contains(id, "://") {
			u, err := url.Parse(id)
			if err != nil {
				t.Fatal(err)
			}
			template.URIs = append(template.URIs, u)
		} else {
			template.DNSNames = append(template.DNSNames, id)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	// Written beside and renamed, as a rotated Secret is.
	for name, block := range map[string]*pem.Block{
		"tls.crt": {Type: "CERTIFICATE", Bytes: der},
		"tls.key": {Type: "EC PRIVATE KEY", Bytes: keyDER},
	} {
		tmp := filepath.Join(dir, name+".tmp")
		if err := os.WriteFile(tmp, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
}

// serve runs a health service that only the allowed identities may call.
func serve(t *testing.T, src Source, allowed []string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	options := append(Authorizer(func(string) []string { return allowed }), grpc.Creds(ServerCredentials(src)))
	server := grpc.NewServer(options...)
	healthpb.RegisterHealthServer(server, health.NewServer())
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// call makes one call and returns its gRPC status code.
func call(t *testing.T, target string, src Source, serverIdentities ...string) codes.Code {
	t.Helper()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(ClientCredentials(src, serverIdentities)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return status.Code(err)
}

func TestMutualTLS(t *testing.T) {
	ca, other := newAuthority(t, "stevedore"), newAuthority(t, "someone else")
	const server, driver = "stevedore-server.stevedore-system.svc", "spiffe://example.org/ns/stevedore-system/sa/stevedore-csi"
	target := serve(t, ca.issue(t, server), []string{driver})

	// The right identities on both sides: by DNS name or by SPIFFE ID alike.
	if got := call(t, target, ca.issue(t, driver), server); got != codes.OK {
		t.Errorf("an expected client calling the expected server: %s, want OK", got)
	}

	// A valid certificate is not enough: the identity must be one the
	// server allows for the call.
	if got := call(t, target, ca.issue(t, "stevedore-provider-file.stevedore-system.svc"), server); got != codes.PermissionDenied {
		t.Errorf("a client with another identity: %s, want PermissionDenied", got)
	}
	// A client from another authority does not get as far as a call.
	if got := call(t, target, other.issue(t, driver), server); got != codes.Unavailable {
		t.Errorf("a client from another authority: %s, want Unavailable", got)
	}
	// The client refuses a server that is not who it expects, although the
	// certificate is valid and the address is the one it dialled.
	if got := call(t, target, ca.issue(t, driver), "stevedore-provider-s3.stevedore-system.svc"); got != codes.Unavailable {
		t.Errorf("a server with another identity: %s, want Unavailable", got)
	}
	// And one whose certificate it cannot verify.
	untrusting := ca.issue(t, driver)
	untrusting.CA = other.issue(t, "x").CA
	if got := call(t, target, untrusting, server); got != codes.Unavailable {
		t.Errorf("a client that does not trust the server's authority: %s, want Unavailable", got)
	}
}

// A certificate that is replaced on disk is used from the next handshake on,
// with no restart.
func TestFilesReload(t *testing.T) {
	ca := newAuthority(t, "stevedore")
	serverFiles := ca.issue(t, "old-name")
	target := serve(t, serverFiles, []string{"client"})
	client := ca.issue(t, "client")

	if got := call(t, target, client, "old-name"); got != codes.OK {
		t.Fatalf("before the rotation: %s", got)
	}
	// Rotate, with a later modification time whatever the clock's resolution.
	ca.write(t, filepath.Dir(serverFiles.Cert), "new-name")
	later := time.Now().Add(time.Minute)
	for _, f := range []string{serverFiles.Cert, serverFiles.Key} {
		if err := os.Chtimes(f, later, later); err != nil {
			t.Fatal(err)
		}
	}
	if got := call(t, target, client, "new-name"); got != codes.OK {
		t.Errorf("after the rotation, expecting the new identity: %s, want OK", got)
	}
	if got := call(t, target, client, "old-name"); got != codes.Unavailable {
		t.Errorf("after the rotation, expecting the old identity: %s, want Unavailable", got)
	}

	// A half-written rotation keeps the certificate that works.
	if err := os.WriteFile(serverFiles.Key, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := call(t, target, client, "new-name"); got != codes.OK {
		t.Errorf("with a broken key on disk: %s, want the last good certificate to serve", got)
	}

	missing := &Files{Cert: "/nonexistent/tls.crt", Key: "/nonexistent/tls.key", CA: "/nonexistent/ca.crt"}
	if _, err := missing.Certificate(); err == nil {
		t.Error("a missing certificate was loaded")
	}
	if _, err := missing.Roots(); err == nil {
		t.Error("missing roots were loaded")
	}
}

func TestAuthorizeWithoutTLS(t *testing.T) {
	if err := Authorize(context.Background(), nil); err != nil {
		t.Errorf("with no identities configured: %v, want everyone allowed", err)
	}
	if err := Authorize(context.Background(), []string{"x"}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("with identities configured and no peer: %v, want Unauthenticated", err)
	}
}
