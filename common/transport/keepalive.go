package transport

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	// A stream between two components never ends by itself, so a peer that
	// vanished without closing its connection (a node that lost power, a
	// partition) is noticed only by asking: the client pings when it has
	// heard nothing for keepaliveTime and gives up after keepaliveTimeout.
	keepaliveTime    = 30 * time.Second
	keepaliveTimeout = 10 * time.Second

	// maxAgeGrace is how long streams get to finish after a connection
	// reached its maximum age. Stevedore's streams never finish; this is how
	// long until they are cut.
	maxAgeGrace = 5 * time.Second
)

// ClientKeepalive returns the dial option that makes a client notice a server
// that is gone.
func ClientKeepalive() grpc.DialOption {
	return grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: keepaliveTime, Timeout: keepaliveTimeout})
}

// ServerKeepalive returns the options of a server whose clients use
// ClientKeepalive. With a positive maxAge it also closes each connection
// once it is that old, give or take a tenth: a client that reconnects goes
// through the Service again and may land on another replica, which is what
// spreads long-lived streams over replicas that were added later.
func ServerKeepalive(maxAge time.Duration) []grpc.ServerOption {
	options := []grpc.ServerOption{
		// Accept the clients' pings instead of treating them as abuse.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: keepaliveTime / 2}),
	}
	if maxAge > 0 {
		options = append(options, grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      maxAge,
			MaxConnectionAgeGrace: maxAgeGrace,
		}))
	}
	return options
}
