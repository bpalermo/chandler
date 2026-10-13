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
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/common/telemetry"
	"github.com/bpalermo/chandler/common/transport"
	"github.com/bpalermo/chandler/common/version"
	"github.com/bpalermo/chandler/server/internal/hub"
	"github.com/bpalermo/chandler/server/internal/identity"
)

// providerFlags collects repeated --provider name=target flags.
type providerFlags []string

func (p *providerFlags) String() string     { return strings.Join(*p, ",") }
func (p *providerFlags) Set(v string) error { *p = append(*p, v); return nil }

// security is how the server authenticates itself and the others.
type security struct {
	tls          transport.Options
	healthListen string
	// drivers and peers are the identities allowed to subscribe and to
	// sync; peers is also what a peer's certificate must carry.
	drivers, peers providerFlags
	// providers is "<name>=<identity>": what each provider's certificate
	// must carry, when it is not the host name of its target.
	providers providerFlags
	// How the pod behind a subscription is identified.
	// maxConnectionAge is after how long a driver is made to reconnect.
	maxConnectionAge         time.Duration
	tokenAudience, tokenKeys string
	trustDriver              bool
}

func main() {
	var providers providerFlags
	var sec security
	sec.tls.Register()
	flag.StringVar(&sec.healthListen, "health-listen", ":9001", "address of the gRPC health service, served without TLS for kubelet's probes")
	flag.Var(&sec.drivers, "driver-identity", "an identity (DNS name or SPIFFE ID) a CSI driver's certificate may carry. Repeatable; required with TLS")
	flag.Var(&sec.peers, "peer-identity", "an identity another replica's certificate may carry. Repeatable; required with TLS when peer sync is on")
	flag.StringVar(&sec.tokenAudience, "pod-token-audience", "chandler", "the audience the pods' service account tokens must have been issued for")
	flag.StringVar(&sec.tokenKeys, "pod-token-keys", identity.InClusterJWKS, "where the cluster's token signing keys are: a URL, read with this pod's service account, or a file")
	flag.BoolVar(&sec.trustDriver, "trust-driver-identity", false, "do not verify the pods' tokens: believe the namespace, service account and pod a driver reports")
	flag.DurationVar(&sec.maxConnectionAge, "max-connection-age", 30*time.Minute, "how long a driver's connection lives before it is closed so that the driver reconnects, possibly to another replica; 0 never closes one")
	flag.Var(&sec.providers, "provider-identity", "<name>=<identity>: the identity a provider's certificate must carry, when it is not the host name of its target. Repeatable")
	var (
		listen         = flag.String("listen", ":9000", "address the server's gRPC server listens on")
		serverID       = flag.String("server-id", os.Getenv("POD_NAME"), "identity of this replica (default $POD_NAME, else the hostname)")
		maxBundleBytes = flag.Uint64("max-bundle-bytes", bundle.DefaultMaxBytes, "largest bundle accepted from a provider, in bytes")
		maxBundleFiles = flag.Int("max-bundle-files", bundle.DefaultMaxFiles, "largest number of files in a bundle")
		cacheDir       = flag.String("cache-dir", "", "directory the cache is kept in; empty keeps it in memory only")
		cacheGrace     = flag.Duration("cache-grace", 24*time.Hour, "how long a cache entry no pod uses is kept, unless its room is needed sooner")
		maxKeys        = flag.Int("max-keys-per-provider", hub.DefaultQuotas().MaxKeysPerProvider, "most cache keys held for one provider; entries no pod uses are evicted to stay within it")
		maxBytes       = flag.Uint64("max-bytes-per-provider", hub.DefaultQuotas().MaxBytesPerProvider, "most bytes of content held for one provider; entries no pod uses are evicted to stay within it")
		negativeTTL    = flag.Duration("negative-ttl", 30*time.Second, "how long a not-found or permission-denied answer from a provider is remembered")
		peerService    = flag.String("peer-service", "", "host:port of the headless Service listing the ready replicas; empty disables peer sync")
		peerSelf       = flag.String("peer-self", "", "this replica's own address in that Service (default $POD_IP and the peer service's port)")
		peerInterval   = flag.Duration("peer-sync-interval", 30*time.Second, "how often the cache is synced with one peer")
		peerWarmUp     = flag.Duration("peer-warm-up", time.Minute, "longest the replica stays unready while copying a peer's cache at start")
		showVersion    = flag.Bool("version", false, "print the version and exit")
	)
	flag.Var(&providers, "provider", "a provider as <name>=<gRPC target>; <name> is the scheme of the URIs it serves. Repeatable")
	var providerKeys, providerBytes providerFlags
	flag.Var(&providerKeys, "provider-max-keys", "<name>=<count>: --max-keys-per-provider for one provider. Repeatable")
	flag.Var(&providerBytes, "provider-max-bytes", "<name>=<bytes>: --max-bytes-per-provider for one provider. Repeatable")
	flag.Parse()

	if *showVersion {
		fmt.Printf("chandler-server %s\n", version.Version)
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	limits := bundle.Limits{MaxBytes: *maxBundleBytes, MaxFiles: *maxBundleFiles}
	quotas := hub.DefaultQuotas()
	quotas.MaxKeysPerProvider, quotas.MaxBytesPerProvider = *maxKeys, *maxBytes
	providerQuotas, err := parseProviderQuotas(providerKeys, providerBytes)
	if err == nil && (quotas.MaxKeysPerProvider <= 0 || quotas.MaxBytesPerProvider == 0) {
		err = errors.New("--max-keys-per-provider and --max-bytes-per-provider must be positive")
	}
	if err != nil {
		log.Error("chandler-server failed", "error", err)
		os.Exit(1)
	}
	cfg := hub.Config{
		ServerID:       *serverID,
		Limits:         limits,
		Quotas:         quotas,
		ProviderQuotas: providerQuotas,
		CacheDir:       *cacheDir,
		Grace:          *cacheGrace,
		NegativeTTL:    *negativeTTL,
		Log:            log,
	}
	peers := hub.Peers{Service: *peerService, Self: *peerSelf, Interval: *peerInterval, WarmUp: *peerWarmUp}
	if err := run(log, *listen, cfg, peers, providers, sec); err != nil {
		log.Error("chandler-server failed", "error", err)
		os.Exit(1)
	}
}

// parseProviderQuotas reads the --provider-max-keys and --provider-max-bytes
// flags, each "<name>=<positive number>".
func parseProviderQuotas(keys, bytes []string) (map[string]hub.ProviderQuota, error) {
	quotas := make(map[string]hub.ProviderQuota)
	parse := func(flag, value string) (string, uint64, error) {
		name, number, _ := strings.Cut(value, "=")
		n, err := strconv.ParseUint(number, 10, 63)
		if name == "" || err != nil || n == 0 {
			return "", 0, fmt.Errorf("--%s %q is not <name>=<positive number>", flag, value)
		}
		return name, n, nil
	}
	for _, value := range keys {
		name, n, err := parse("provider-max-keys", value)
		if err != nil {
			return nil, err
		}
		q := quotas[name]
		q.MaxKeys = int(n)
		quotas[name] = q
	}
	for _, value := range bytes {
		name, n, err := parse("provider-max-bytes", value)
		if err != nil {
			return nil, err
		}
		q := quotas[name]
		q.MaxBytes = n
		quotas[name] = q
	}
	return quotas, nil
}

func run(log *slog.Logger, listen string, cfg hub.Config, peers hub.Peers, providers []string, sec security) error {
	limits, serverID := cfg.Limits, cfg.ServerID
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

	src, err := sec.tls.Source(context.Background())
	if err != nil {
		return err
	}
	providerIdentities := make(map[string][]string)
	for _, p := range sec.providers {
		name, identity, ok := strings.Cut(p, "=")
		if !ok || name == "" || identity == "" {
			return fmt.Errorf("--provider-identity %q is not <name>=<identity>", p)
		}
		providerIdentities[name] = append(providerIdentities[name], identity)
	}
	// With TLS, an empty list would allow nobody; say so at start.
	var drivers, peerIdentities []string
	if src != nil {
		drivers, peerIdentities = sec.drivers, sec.peers
		switch {
		case len(drivers) == 0:
			return errors.New("--driver-identity is required with TLS")
		case peers.Service != "" && len(peerIdentities) == 0:
			return errors.New("--peer-identity is required with TLS when peer sync is on")
		}
		if drivers == nil {
			drivers = []string{}
		}
		if peerIdentities == nil {
			peerIdentities = []string{}
		}
	} else {
		log.Warn("running without TLS: connections are neither encrypted nor authenticated")
	}
	peers.Identities = peerIdentities
	if sec.trustDriver {
		log.Warn("not verifying pod tokens: a driver is believed about which pod it speaks for")
	} else {
		keys := &identity.JWKS{Log: log}
		if strings.HasPrefix(sec.tokenKeys, "https://") || strings.HasPrefix(sec.tokenKeys, "http://") {
			keys.URL = sec.tokenKeys
		} else {
			keys.File = sec.tokenKeys
		}
		if err := keys.Load(); err != nil {
			// Not fatal: the API server may be away for a moment, and the
			// keys are fetched again when the first token arrives.
			log.Warn("could not fetch the cluster's token signing keys yet", "error", err)
		}
		verifier := identity.Verifier{Audience: sec.tokenAudience, Keys: keys}
		cfg.Verify = func(token string) (hub.Identity, error) {
			who, err := verifier.Verify(token)
			return hub.Identity(who), err
		}
	}
	cfg.Dial = func(identities []string) credentials.TransportCredentials { return transport.Dial(src, identities) }

	cfg.ServerID = serverID
	h, err := hub.New(cfg)
	if err != nil {
		return fmt.Errorf("open the cache: %w", err)
	}
	for _, p := range providers {
		name, target, ok := strings.Cut(p, "=")
		if !ok || name == "" || target == "" {
			return fmt.Errorf("--provider %q is not <name>=<target>", p)
		}
		if name != strings.ToLower(name) {
			return fmt.Errorf("--provider %q: the name is a URI scheme and must be lowercase", p)
		}
		identities := providerIdentities[name]
		if len(identities) == 0 {
			identities = []string{transport.HostIdentity(target)}
		}
		if err := h.AddProvider(name, target, identities...); err != nil {
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
	if peers.Service != "" {
		switch {
		case peers.Interval <= 0 || peers.WarmUp <= 0:
			return errors.New("--peer-sync-interval and --peer-warm-up must be positive")
		case peers.Self == "":
			_, port, err := net.SplitHostPort(peers.Service)
			if err != nil {
				return fmt.Errorf("--peer-service: %w", err)
			}
			if ip := os.Getenv("POD_IP"); ip != "" {
				peers.Self = net.JoinHostPort(ip, port)
			}
		}
	}

	// Drivers may subscribe, replicas may sync, and neither may do the
	// other's part.
	options := transport.Serve(src, func(method string) []string {
		if strings.HasPrefix(method, "/"+chandlerv1.PeerSync_ServiceDesc.ServiceName+"/") {
			return peerIdentities
		}
		return drivers
	})
	server := grpc.NewServer(append(options, transport.ServerKeepalive(sec.maxConnectionAge)...)...)
	chandlerv1.RegisterConfigDiscoveryServiceServer(server, h)
	chandlerv1.RegisterPeerSyncServer(server, h)
	// Not ready until the cache is warm: a rolling restart must not move on
	// from a replica that holds nothing.
	healthServer, stopHealth, err := transport.Health(sec.healthListen)
	if err != nil {
		return err
	}
	defer stopHealth()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	// Alive is not the same as ready: a liveness probe asks for this
	// service, so a replica that is still warming up is not restarted.
	healthServer.SetServingStatus("live", healthpb.HealthCheckResponse_SERVING)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	stopTelemetry, err := telemetry.Setup(ctx, "chandler-server", version.Version)
	if err != nil {
		return err
	}
	defer stopTelemetry()
	go h.Run(ctx)
	go h.RunPeers(ctx, peers, func() {
		log.Info("ready")
		healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	})
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
