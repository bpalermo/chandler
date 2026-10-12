// Command chandler-provider-s3 is the Chandler reference provider: it serves
// configuration from S3 buckets. <name>://<bucket>/<key> is one object and
// <name>://<bucket>/<prefix>/ every object below a prefix.
//
// It is read-only and has no upload path of its own. Whoever writes to the
// bucket is responsible for the validity of what is there; a production
// provider would validate on upload.
//
// Credentials and region come from the AWS SDK's default chain: the
// environment, a web identity token (IRSA), or the instance role.
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
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/provider"
	"github.com/bpalermo/chandler/common/version"
	"github.com/bpalermo/chandler/providers/s3/internal/origin"
)

// bucketFlags collects repeated --bucket flags.
type bucketFlags []string

func (b *bucketFlags) String() string     { return strings.Join(*b, ",") }
func (b *bucketFlags) Set(v string) error { *b = append(*b, v); return nil }

func main() {
	var buckets bucketFlags
	var (
		listen       = flag.String("listen", ":9100", "address the provider's gRPC server listens on")
		name         = flag.String("name", "s3", "the provider's name: the scheme of the URIs it serves")
		pollInterval = flag.Duration("poll-interval", 30*time.Second, "how often watched objects are checked for change")
		endpointURL  = flag.String("endpoint-url", "", "S3 endpoint, for an S3-compatible store; empty uses AWS")
		pathStyle    = flag.Bool("path-style", false, "address buckets as <endpoint>/<bucket> instead of <bucket>.<endpoint>")
		showVersion  = flag.Bool("version", false, "print the version and exit")
	)
	flag.Var(&buckets, "bucket", "a bucket URIs may name; repeatable. Without it, any bucket the credentials can read")
	flag.Parse()

	if *showVersion {
		fmt.Printf("chandler-provider-s3 %s\n", version.Version)
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *listen, *name, *pollInterval, *endpointURL, *pathStyle, buckets); err != nil {
		log.Error("chandler-provider-s3 failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, listen, name string, pollInterval time.Duration, endpointURL string, pathStyle bool, buckets []string) error {
	switch {
	case name == "":
		return errors.New("--name is required")
	case pollInterval <= 0:
		return errors.New("--poll-interval must be positive")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS configuration: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpointURL != "" {
			o.BaseEndpoint = &endpointURL
		}
		o.UsePathStyle = pathStyle
	})

	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", listen, err)
	}
	server := grpc.NewServer()
	chandlerv1.RegisterConfigProviderServer(server, &provider.Server{
		Name:         name,
		Origin:       origin.Origin{Client: client, Buckets: buckets},
		PollInterval: pollInterval,
		Log:          log,
	})
	healthpb.RegisterHealthServer(server, health.NewServer())

	go func() {
		<-ctx.Done()
		log.Info("stopping")
		// Streams never end by themselves, so a graceful stop would wait
		// for ever.
		server.Stop()
	}()

	log.Info("serving ConfigProvider", "name", name, "version", version.Version, "listen", listen,
		"region", cfg.Region, "endpoint", endpointURL, "buckets", strings.Join(buckets, ","))
	return server.Serve(listener)
}
