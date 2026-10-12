// Command chandler-provider-file is a Chandler configuration provider that
// serves the files below a directory. A URI names a path under that
// directory: <name>:///app.yaml is one file, <name>:///flags/ a directory.
//
// It has no upload path of its own: whoever fills the directory (a mounted
// ConfigMap, a git-sync sidecar) is responsible for the validity of what is
// in it.
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
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/provider"
	"github.com/bpalermo/chandler/common/version"
	"github.com/bpalermo/chandler/providers/file/internal/source"
)

func main() {
	var (
		listen       = flag.String("listen", ":9100", "address the provider's gRPC server listens on")
		name         = flag.String("name", "file", "the provider's name: the scheme of the URIs it serves")
		root         = flag.String("root", "", "directory the URIs are resolved against")
		pollInterval = flag.Duration("poll-interval", 2*time.Second, "how often watched resources are re-read")
		showVersion  = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("chandler-provider-file %s\n", version.Version)
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *listen, *name, *root, *pollInterval); err != nil {
		log.Error("chandler-provider-file failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, listen, name, root string, pollInterval time.Duration) error {
	switch {
	case name == "":
		return errors.New("--name is required")
	case root == "":
		return errors.New("--root is required")
	case pollInterval <= 0:
		return errors.New("--poll-interval must be positive")
	}
	if info, err := os.Stat(root); err != nil {
		return fmt.Errorf("--root: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("--root: %s is not a directory", root)
	}

	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", listen, err)
	}
	server := grpc.NewServer()
	chandlerv1.RegisterConfigProviderServer(server, &provider.Server{
		Name:         name,
		Origin:       source.Origin{Root: root},
		PollInterval: pollInterval,
		Log:          log,
	})
	healthpb.RegisterHealthServer(server, health.NewServer())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Info("stopping")
		// Streams never end by themselves, so a graceful stop would wait
		// for ever.
		server.Stop()
	}()

	log.Info("serving ConfigProvider", "name", name, "root", root, "version", version.Version, "listen", listen)
	return server.Serve(listener)
}
