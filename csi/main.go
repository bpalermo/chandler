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
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"

	"github.com/bpalermo/chandler/common/version"
	"github.com/bpalermo/chandler/csi/internal/client"
	"github.com/bpalermo/chandler/csi/internal/driver"
	"github.com/bpalermo/chandler/csi/internal/mount"
)

func main() {
	var (
		endpoint    = flag.String("endpoint", "unix:///csi/csi.sock", "CSI endpoint; only unix:// sockets are supported")
		nodeID      = flag.String("node-id", os.Getenv("NODE_NAME"), "Kubernetes node name (default $NODE_NAME)")
		stateDir    = flag.String("state-dir", "/var/lib/chandler", "directory on the host where volumes are kept")
		server      = flag.String("server", "", "gRPC target of the config server, e.g. dns:///chandler-server.chandler-system.svc:9000")
		showVersion = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("chandler-csi %s\n", version.Version)
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *endpoint, *nodeID, *stateDir, *server); err != nil {
		log.Error("chandler-csi failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, endpoint, nodeID, stateDir, serverTarget string) error {
	socket, ok := strings.CutPrefix(endpoint, "unix://")
	if !ok || socket == "" {
		return fmt.Errorf("endpoint %q is not a unix:// socket", endpoint)
	}
	if serverTarget == "" {
		return errors.New("--server is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	configServer := client.New(client.Config{
		Target:  serverTarget,
		NodeID:  nodeID,
		Version: version.Version,
		Log:     log,
	})
	d, err := driver.New(driver.Config{
		NodeID:   nodeID,
		Version:  version.Version,
		StateDir: stateDir,
		Mounter:  mount.System{},
		Server:   configServer,
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

// logCalls logs every CSI call that changes something or fails; kubelet's
// periodic probes would otherwise drown the log.
func logCalls(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		switch {
		case err != nil:
			log.Error("CSI call failed", "method", info.FullMethod, "error", err)
		case strings.Contains(info.FullMethod, "Publish"):
			log.Info("CSI call", "method", info.FullMethod)
		}
		return resp, err
	}
}
