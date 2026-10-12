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
	"github.com/bpalermo/chandler/csi/internal/driver"
	"github.com/bpalermo/chandler/csi/internal/mount"
)

func main() {
	var (
		endpoint    = flag.String("endpoint", "unix:///csi/csi.sock", "CSI endpoint; only unix:// sockets are supported")
		nodeID      = flag.String("node-id", os.Getenv("NODE_NAME"), "Kubernetes node name (default $NODE_NAME)")
		stateDir    = flag.String("state-dir", "/var/lib/chandler", "directory on the host where volumes are kept")
		showVersion = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("chandler-csi %s\n", version.Version)
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *endpoint, *nodeID, *stateDir); err != nil {
		log.Error("chandler-csi failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, endpoint, nodeID, stateDir string) error {
	socket, ok := strings.CutPrefix(endpoint, "unix://")
	if !ok || socket == "" {
		return fmt.Errorf("endpoint %q is not a unix:// socket", endpoint)
	}

	d, err := driver.New(driver.Config{
		NodeID:   nodeID,
		Version:  version.Version,
		StateDir: stateDir,
		Mounter:  mount.System{},
	})
	if err != nil {
		return err
	}

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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
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
