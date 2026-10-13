// Command chandler-csi is the Chandler CSI node driver. It serves the CSI
// Identity and Node services to kubelet over a Unix socket.
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
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/bpalermo/chandler/common/telemetry"
	"github.com/bpalermo/chandler/common/transport"
	"github.com/bpalermo/chandler/common/version"
	"github.com/bpalermo/chandler/csi/internal/cache"
	"github.com/bpalermo/chandler/csi/internal/client"
	"github.com/bpalermo/chandler/csi/internal/driver"
	"github.com/bpalermo/chandler/csi/internal/events"
	"github.com/bpalermo/chandler/csi/internal/mount"
)

func main() {
	var (
		endpoint       = flag.String("endpoint", "unix:///csi/csi.sock", "CSI endpoint; only unix:// sockets are supported")
		nodeID         = flag.String("node-id", os.Getenv("NODE_NAME"), "Kubernetes node name (default $NODE_NAME)")
		stateDir       = flag.String("state-dir", "/var/lib/chandler", "directory on the host where volumes are kept")
		server         = flag.String("server", "", "gRPC target of the config server, e.g. dns:///chandler-server.chandler-system.svc:9000")
		serverIdentity = flag.String("server-identity", "", "the identity (DNS name or SPIFFE ID) the config server's certificate must carry (default: the host name in --server)")
		tokenAudience  = flag.String("token-audience", driver.DefaultTokenAudience, "the audience of the pod token kubelet supplies for the config server; matches the CSIDriver object's tokenRequests")
		podEvents      = flag.Bool("pod-events", true, "record Kubernetes events on pods when their configuration is refused, rejected, late or served from a cache; needs permission to create events")
		showVersion    = flag.Bool("version", false, "print the version and exit")
	)
	var tls transport.Options
	tls.Register()
	flag.Parse()

	if *showVersion {
		fmt.Printf("chandler-csi %s\n", version.Version)
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	src, err := tls.Source(context.Background())
	if err != nil {
		log.Error("chandler-csi failed", "error", err)
		os.Exit(1)
	}
	if src == nil {
		log.Warn("running without TLS: the connection to the config server is neither encrypted nor authenticated")
	}
	identity := *serverIdentity
	if identity == "" {
		identity = transport.HostIdentity(*server)
	}
	if err := run(log, *endpoint, *nodeID, *stateDir, *server, transport.Dial(src, []string{identity}), *tokenAudience, *podEvents); err != nil {
		log.Error("chandler-csi failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, endpoint, nodeID, stateDir, serverTarget string, creds credentials.TransportCredentials, tokenAudience string, podEvents bool) error {
	socket, ok := strings.CutPrefix(endpoint, "unix://")
	if !ok || socket == "" {
		return fmt.Errorf("endpoint %q is not a unix:// socket", endpoint)
	}
	if serverTarget == "" {
		return errors.New("--server is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	stopTelemetry, err := telemetry.Setup(ctx, "chandler-csi", version.Version)
	if err != nil {
		return err
	}
	defer stopTelemetry()

	configServer := client.New(client.Config{
		Target:      serverTarget,
		NodeID:      nodeID,
		Version:     version.Version,
		Credentials: creds,
		Log:         log,
	})
	var recorder events.Recorder = events.Discard{}
	if podEvents {
		api, err := events.InCluster(nodeID, log)
		if err != nil {
			// The driver works without them; say so and go on.
			log.Warn("not recording events on pods", "error", err)
		} else {
			recorder = api
		}
	}
	nodeCache, err := cache.Open(filepath.Join(stateDir, "cache"), cache.DefaultGrace)
	if err != nil {
		return err
	}
	d, err := driver.New(driver.Config{
		NodeID:        nodeID,
		Version:       version.Version,
		StateDir:      stateDir,
		Mounter:       mount.System{},
		Server:        configServer,
		Cache:         nodeCache,
		TokenAudience: tokenAudience,
		Events:        recorder,
	})
	if err != nil {
		return err
	}
	// Before connecting, so the first stream already carries the volumes
	// that are mounted.
	if err := d.Restore(); err != nil {
		return err
	}
	go configServer.Run(ctx)
	go func() {
		// What live volumes use is never collected; the rest goes after
		// the grace period.
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			inUse := d.InUse()
			nodeCache.Touch(inUse...)
			if err := nodeCache.Collect(inUse); err != nil {
				log.Warn("node cache collection failed", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	// A socket left by a previous run would make Listen fail.
	if err := os.Remove(socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale socket: %w", err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", socket, err)
	}

	server := grpc.NewServer(grpc.UnaryInterceptor(logCalls(log)))
	csi.RegisterIdentityServer(server, d)
	csi.RegisterNodeServer(server, d)

	go func() {
		<-ctx.Done()
		log.Info("stopping")
		server.GracefulStop()
	}()

	log.Info("serving CSI", "driver", driver.Name, "version", version.Version, "endpoint", endpoint, "node", nodeID)
	return server.Serve(listener)
}

// logCalls logs the CSI calls that fail, and the unpublishing of volumes;
// kubelet's periodic probes and republishes would otherwise drown the log.
func logCalls(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		switch {
		case err != nil:
			log.Error("CSI call failed", "method", info.FullMethod, "error", err)
		case strings.HasSuffix(info.FullMethod, "/NodeUnpublishVolume"):
			// Publish is not logged: kubelet repeats it for every mounted
			// volume to refresh the pod's token.
			log.Info("CSI call", "method", info.FullMethod)
		}
		return resp, err
	}
}
