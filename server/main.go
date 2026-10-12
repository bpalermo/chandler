// Command chandler-server is the Chandler config server. CSI drivers
// subscribe to it for the configuration of the pods on their node, and it
// watches the registered providers on their behalf.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/common/version"
	"github.com/bpalermo/chandler/server/internal/hub"
)

// providerFlags collects repeated --provider name=target flags.
type providerFlags []string

func (p *providerFlags) String() string     { return strings.Join(*p, ",") }
func (p *providerFlags) Set(v string) error { *p = append(*p, v); return nil }

func main() {
	var providers providerFlags
	var (
		listen         = flag.String("listen", ":9000", "address the server's gRPC server listens on")
		serverID       = flag.String("server-id", os.Getenv("POD_NAME"), "identity of this replica (default $POD_NAME, else the hostname)")
		maxBundleBytes = flag.Uint64("max-bundle-bytes", bundle.DefaultMaxBytes, "largest bundle accepted from a provider, in bytes")
		maxBundleFiles = flag.Int("max-bundle-files", bundle.DefaultMaxFiles, "largest number of files in a bundle")
		showVersion    = flag.Bool("version", false, "print the version and exit")
	)
	flag.Var(&providers, "provider", "a provider as <name>=<gRPC target>; <name> is the scheme of the URIs it serves. Repeatable")
	flag.Parse()

	if *showVersion {
		fmt.Printf("chandler-server %s\n", version.Version)
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	limits := bundle.Limits{MaxBytes: *maxBundleBytes, MaxFiles: *maxBundleFiles}
	if err := run(log, *listen, *serverID, limits, providers); err != nil {
		log.Error("chandler-server failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, listen, serverID string, limits bundle.Limits, providers []string) error {
	switch {
	case limits.MaxBytes == 0 || limits.MaxBytes > bundle.HardMaxBytes:
		// A bundle travels in one gRPC message.
		return fmt.Errorf("--max-bundle-bytes must be between 1 and %d", bundle.HardMaxBytes)
	case limits.MaxFiles <= 0:
		return errors.New("--max-bundle-files must be positive")
	}
	if serverID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("no --server-id and no hostname: %w", err)
		}
		serverID = hostname
	}

	h := hub.New(hub.Config{ServerID: serverID, Limits: limits, Log: log})
	for _, p := range providers {
		name, target, ok := strings.Cut(p, "=")
		if !ok || name == "" || target == "" {
			return fmt.Errorf("--provider %q is not <name>=<target>", p)
		}
		if name != strings.ToLower(name) {
			return fmt.Errorf("--provider %q: the name is a URI scheme and must be lowercase", p)
		}
		if err := h.AddProvider(name, target); err != nil {
			return err
		}
	}
	if len(providers) == 0 {
		log.Warn("no provider is registered; every subscription will fail")
	}

	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", listen, err)
	}
	server := grpc.NewServer()
	chandlerv1.RegisterConfigDiscoveryServiceServer(server, h)
	healthpb.RegisterHealthServer(server, health.NewServer())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go h.Run(ctx)
	go func() {
		<-ctx.Done()
		log.Info("stopping")
		// Streams never end by themselves, so a graceful stop would wait
		// for ever. Drivers reconnect, to another replica or to this one's
		// successor, and running pods keep their files meanwhile.
		server.Stop()
	}()

	log.Info("serving ConfigDiscoveryService", "server_id", serverID, "version", version.Version,
		"listen", listen, "providers", len(providers))
	return server.Serve(listener)
}
