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

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/provider"
	"github.com/bpalermo/chandler/common/rollout"
	"github.com/bpalermo/chandler/common/telemetry"
	"github.com/bpalermo/chandler/common/transport"
	"github.com/bpalermo/chandler/common/version"
	"github.com/bpalermo/chandler/providers/s3/internal/origin"
)

// bucketFlags collects repeated --bucket flags.
type bucketFlags []string

func (b *bucketFlags) String() string     { return strings.Join(*b, ",") }
func (b *bucketFlags) Set(v string) error { *b = append(*b, v); return nil }

// rolloutFlags collects repeated --rollout flags.
type rolloutFlags []rollout.Rule

func (r *rolloutFlags) String() string { return fmt.Sprint(len(*r), " rules") }
func (r *rolloutFlags) Set(v string) error {
	rule, err := rollout.ParseRule(v)
	if err != nil {
		return err
	}
	*r = append(*r, rule)
	return nil
}

// identityFlags collects repeated --server-identity flags.
type identityFlags []string

func (i *identityFlags) String() string     { return strings.Join(*i, ",") }
func (i *identityFlags) Set(v string) error { *i = append(*i, v); return nil }

// security is how the provider authenticates itself and the config servers.
type security struct {
	tls          transport.Options
	healthListen string
	servers      identityFlags
}

func main() {
	var rollouts rolloutFlags
	var sec security
	sec.tls.Register()
	flag.StringVar(&sec.healthListen, "health-listen", ":9101", "address of the gRPC health service, served without TLS for kubelet's probes")
	flag.Var(&sec.servers, "server-identity", "an identity (DNS name or SPIFFE ID) a config server's certificate may carry. Repeatable; required with TLS")
	var buckets bucketFlags
	var (
		listen       = flag.String("listen", ":9100", "address the provider's gRPC server listens on")
		name         = flag.String("name", "s3", "the provider's name: the scheme of the URIs it serves")
		pollInterval = flag.Duration("poll-interval", 30*time.Second, "how often watched objects are checked for change")
		endpointURL  = flag.String("endpoint-url", "", "S3 endpoint, for an S3-compatible store; empty uses AWS")
		pathStyle    = flag.Bool("path-style", false, "address buckets as <endpoint>/<bucket> instead of <bucket>.<endpoint>")
		maxRefused   = flag.Int("rollout-max-refused", 0, "pods that may refuse or reject a version before its rollout is held where it is; negative never holds")
		showVersion  = flag.Bool("version", false, "print the version and exit")
	)
	flag.Var(&buckets, "bucket", "a bucket URIs may name; repeatable. Without it, any bucket the credentials can read")
	flag.Var(&rollouts, "rollout", "how new versions are released over time, as <ramp> for every resource or <pattern>=<ramp> for those whose name matches, e.g. '/payments/*=1%:5m,10%:10m,50%:10m'. Repeatable; the first match wins. Without a match a version goes to every pod at once")
	flag.Parse()

	if *showVersion {
		fmt.Printf("chandler-provider-s3 %s\n", version.Version)
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *listen, *name, *pollInterval, *endpointURL, *pathStyle, buckets, rollouts, *maxRefused, sec); err != nil {
		log.Error("chandler-provider-s3 failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, listen, name string, pollInterval time.Duration, endpointURL string, pathStyle bool, buckets []string, rollouts []rollout.Rule, maxRefused int, sec security) error {
	switch {
	case name == "":
		return errors.New("--name is required")
	case pollInterval <= 0:
		return errors.New("--poll-interval must be positive")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	stopTelemetry, err := telemetry.Setup(ctx, "chandler-provider-s3", version.Version)
	if err != nil {
		return err
	}
	defer stopTelemetry()

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
	src, err := sec.tls.Source()
	if err != nil {
		return err
	}
	var servers []string
	if src != nil {
		if len(sec.servers) == 0 {
			return errors.New("--server-identity is required with TLS")
		}
		servers = sec.servers
	} else {
		log.Warn("running without TLS: connections are neither encrypted nor authenticated")
	}
	// Only config servers may watch: a provider holds the origin's
	// credentials and serves whatever it is asked for.
	server := grpc.NewServer(transport.Serve(src, func(string) []string { return servers })...)
	chandlerv1.RegisterConfigProviderServer(server, &provider.Server{
		Name:         name,
		Origin:       origin.Origin{Client: client, Buckets: buckets, Rollouts: rollouts},
		PollInterval: pollInterval,
		MaxRefused:   maxRefused,
		Log:          log,
	})
	_, stopHealth, err := transport.Health(sec.healthListen)
	if err != nil {
		return err
	}
	defer stopHealth()

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
