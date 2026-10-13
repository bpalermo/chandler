// Package provider is the ConfigProvider protocol for a provider whose origin
// is polled: the stream, the watches, the nonces and the rule for what is
// sent when. A provider supplies an Origin, which knows how to name and read
// its resources, and nothing else.
//
// For each watch the server is told the resource's state once, and again
// whenever it changes: a new revision, a deletion, or a failure of a kind not
// already reported.
package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/common/rollout"
	"github.com/bpalermo/chandler/common/telemetry"
	"github.com/bpalermo/chandler/common/uri"
)

var (
	// ErrInvalid reports a URI that can never name a resource of this
	// provider. It ends the watch.
	ErrInvalid = errors.New("invalid resource")
	// ErrNotFound reports that the resource does not exist at the origin.
	ErrNotFound = errors.New("not found")
	// ErrPermission reports that the provider may not read the resource.
	ErrPermission = errors.New("permission denied")
)

// Origin is where a provider's configuration lives.
type Origin interface {
	// Resolve returns the resource a canonical URI names, or an error
	// wrapping ErrInvalid. The scheme has already been checked. An Origin
	// must refuse what it does not understand, a query parameter above all,
	// rather than ignore it: an ignored parameter lets one resource be named
	// in many ways.
	Resolve(u uri.URI) (Resource, error)
}

// Resource is one watched resource.
type Resource interface {
	// CacheKey is the provider's normalised identity for the resource.
	CacheKey() string

	// Fetch returns the resource's current state. known is the revision the
	// server already holds, "" for none; when that is still current, Fetch
	// returns it with a nil Bundle and need not read the content.
	//
	// A resource larger than limits is an error wrapping bundle.ErrTooLarge
	// or bundle.ErrTooManyFiles. A missing one wraps ErrNotFound, an
	// unreadable one ErrPermission or fs.ErrPermission. Any other error
	// means the origin could not be reached.
	Fetch(ctx context.Context, known string, limits bundle.Limits) (State, error)
}

// State is a resource at one revision.
type State struct {
	// Revision is the origin's change marker.
	Revision string
	// Bundle is the content, or nil when Revision is the known one.
	Bundle *chandlerv1.Bundle
	// CacheControl is the resource's caching directives, or nil.
	CacheControl *chandlerv1.CacheControl

	// Since is when the revision appeared at the origin; the ramp is
	// measured from it. The zero time releases the revision to everyone at
	// once.
	Since time.Time
	// Ramp is how this resource is rolled out, when the origin says so for
	// the resource itself; nil uses the provider's.
	Ramp *rollout.Ramp
	// Hold caps the share of subscribers that gets the revision, in basis
	// points, when the origin says to hold the rollout; nil does not.
	Hold *uint32
}

// Server serves ConfigProvider for an Origin.
type Server struct {
	chandlerv1.UnimplementedConfigProviderServer

	// Name is the provider's registered name: the scheme of the URIs it serves.
	Name string
	// Origin resolves and reads the resources.
	Origin Origin
	// PollInterval is how often a watched resource is fetched again. Each
	// wait is spread by a tenth either way, so watches do not fall in step.
	PollInterval time.Duration
	// Ramp is how a new revision is released over time, for resources whose
	// origin does not say. The zero Ramp releases to everyone at once.
	Ramp rollout.Ramp
	// MaxRefused is how many subscriptions may refuse or reject a revision
	// before its rollout is held at the share it has reached. Negative never
	// holds.
	MaxRefused int
	// MaxOverdue is how many applications may be late to report on a
	// revision before its rollout is held where it is. Negative never holds.
	MaxOverdue int
	Log        *slog.Logger
	// Now is the clock; nil means time.Now.
	Now func() time.Time

	mu sync.Mutex
	// feedback is the latest report of each connected server replica, by
	// revision.
	feedback map[revisionKey]map[string]*chandlerv1.Feedback
	// held is the share each revision was held at when too many
	// subscriptions refused it.
	held map[revisionKey]uint32

	instruments sync.Once
	updates     metric.Int64Counter
	failures    metric.Int64Counter
	watching    metric.Int64UpDownCounter
}

// instrument creates the provider's metrics, once. They cost nothing until
// the process sets up an exporter.
func (s *Server) instrument() {
	s.instruments.Do(func() {
		m := telemetry.Meter()
		s.updates = telemetry.Must(m.Int64Counter("chandler.provider.updates",
			metric.WithDescription("Updates sent to config servers, by kind: bundle, not_modified or deleted.")))
		s.failures = telemetry.Must(m.Int64Counter("chandler.provider.failures",
			metric.WithDescription("Watches the provider could not serve, by gRPC code.")))
		s.watching = telemetry.Must(m.Int64UpDownCounter("chandler.provider.watches",
			metric.WithDescription("Watches config servers hold on this provider.")))
	})
}

func (s *Server) count(counter metric.Int64Counter, key, value string) {
	counter.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("provider", s.Name), attribute.String(key, value)))
}

// revisionKey names one revision of one resource.
type revisionKey struct{ cacheKey, revision string }

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// record keeps a server replica's latest report on a revision.
func (s *Server) record(serverID string, f *chandlerv1.Feedback) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := revisionKey{f.GetCacheKey(), f.GetRevision()}
	if s.feedback == nil {
		s.feedback = make(map[revisionKey]map[string]*chandlerv1.Feedback)
	}
	if f.GetAssigned() == 0 {
		delete(s.feedback[key], serverID)
		if len(s.feedback[key]) == 0 {
			delete(s.feedback, key)
		}
		return
	}
	if s.feedback[key] == nil {
		s.feedback[key] = make(map[string]*chandlerv1.Feedback)
	}
	s.feedback[key][serverID] = f
}

// forget drops everything a server replica reported, when it disconnects.
func (s *Server) forget(serverID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, reports := range s.feedback {
		delete(reports, serverID)
		if len(reports) == 0 {
			delete(s.feedback, key)
		}
	}
}

// share returns the share of subscribers a revision gets now, in basis
// points: where its ramp has reached, unless the origin holds it lower or too
// many subscriptions have refused it. last is the share last sent on this
// watch for the revision, 0 for none.
func (s *Server) share(key revisionKey, state State, last uint32) uint32 {
	ramp := s.Ramp
	if state.Ramp != nil {
		ramp = *state.Ramp
	}
	share := uint32(rollout.Everyone)
	if !state.Since.IsZero() {
		share = ramp.At(s.now().Sub(state.Since))
	}
	if state.Hold != nil {
		share = min(share, *state.Hold)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	refused, overdue := 0, 0
	for _, f := range s.feedback[key] {
		refused += int(f.GetRefused() + f.GetRejected())
		overdue += int(f.GetOverdue())
	}
	if (s.MaxRefused >= 0 && refused > s.MaxRefused) || (s.MaxOverdue >= 0 && overdue > s.MaxOverdue) {
		// Hold where it is: the pods that have it keep it, no more get it.
		at, ok := s.held[key]
		if !ok {
			at = last
			if at == 0 {
				at = min(share, ramp.At(0))
			}
			if s.held == nil {
				s.held = make(map[revisionKey]uint32)
			}
			s.held[key] = at
			s.Log.Warn("holding a rollout: pods refused the revision or did not report on it",
				"cache_key", key.cacheKey, "revision", key.revision, "refused", refused, "overdue", overdue, "basis_points", at)
		}
		share = min(share, at)
	} else {
		delete(s.held, key)
	}
	// A share never shrinks: a pod that has the revision keeps it.
	return max(share, last)
}

// StreamConfigs implements chandlerv1.ConfigProviderServer.
func (s *Server) StreamConfigs(stream chandlerv1.ConfigProvider_StreamConfigsServer) error {
	s.instrument()
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if !first.HasHello() {
		return grpcstatus.Error(codes.InvalidArgument, "the first message must be a WatcherHello")
	}
	hello := first.GetHello()
	limits := bundle.DefaultLimits()
	if n := hello.GetMaxBundleBytes(); n > 0 {
		limits.MaxBytes = n
	}
	if n := hello.GetMaxBundleFiles(); n > 0 {
		limits.MaxFiles = int(n)
	}
	log := s.Log.With("server", hello.GetServerId())
	log.Info("server connected")
	defer log.Info("server disconnected")
	defer s.forget(hello.GetServerId())

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	// Watches produce concurrently; one goroutine owns stream.Send.
	out := make(chan *chandlerv1.ProviderResponse)
	sendErr := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-out:
				if err := stream.Send(msg); err != nil {
					sendErr <- err
					cancel()
					return
				}
			}
		}
	}()
	session := &session{
		server: s,
		limits: limits,
		log:    log,
		out:    out,
	}
	session.send(ctx, chandlerv1.ProviderResponse_builder{
		Hello: chandlerv1.ProviderHello_builder{Name: proto.String(s.Name)}.Build(),
	}.Build())

	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	watches := make(map[string]context.CancelFunc)
	for {
		req, err := stream.Recv()
		if err != nil {
			select {
			case sendErr := <-sendErr:
				return sendErr
			default:
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		switch {
		case req.HasWatch():
			watch := req.GetWatch()
			id := watch.GetWatchId()
			if stop, ok := watches[id]; ok {
				stop()
			}
			watchCtx, stop := context.WithCancel(ctx)
			watches[id] = stop
			wg.Add(1)
			go func() {
				defer wg.Done()
				session.watch(watchCtx, id, watch.GetUri(), watch.GetKnownRevision())
			}()
		case req.HasUnwatch():
			id := req.GetUnwatch().GetWatchId()
			if stop, ok := watches[id]; ok {
				stop()
				delete(watches, id)
			}
		case req.HasNack():
			nack := req.GetNack()
			log.Warn("server refused a revision",
				"watch", nack.GetWatchId(), "revision", nack.GetRevision(),
				"code", codes.Code(nack.GetErrorDetail().GetCode()).String(),
				"reason", nack.GetErrorDetail().GetMessage())
		case req.HasFeedback():
			s.record(hello.GetServerId(), req.GetFeedback())
		case req.HasAck():
			// Nothing to do: a polled origin has nobody to tell.
		}
	}
}

// session is one server's stream.
type session struct {
	server *Server
	limits bundle.Limits
	log    *slog.Logger
	out    chan<- *chandlerv1.ProviderResponse

	mu    sync.Mutex
	nonce uint64
}

func (s *session) send(ctx context.Context, msg *chandlerv1.ProviderResponse) {
	select {
	case s.out <- msg:
	case <-ctx.Done():
	}
}

func (s *session) nextNonce() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nonce++
	return strconv.FormatUint(s.nonce, 10)
}

// watch polls one resource until ctx ends, sending its state whenever it
// changes.
func (s *session) watch(ctx context.Context, id, rawURI, known string) {
	provider := metric.WithAttributes(attribute.String("provider", s.server.Name))
	s.server.watching.Add(context.Background(), 1, provider)
	defer s.server.watching.Add(context.Background(), -1, provider)
	fail := func(code codes.Code, msg string) {
		s.server.count(s.server.failures, "code", code.String())
		s.send(ctx, chandlerv1.ProviderResponse_builder{
			Error: chandlerv1.WatchError_builder{
				WatchId: proto.String(id),
				Status:  &status.Status{Code: int32(code), Message: msg},
			}.Build(),
		}.Build())
	}
	update := func(key, revision string) chandlerv1.ResourceUpdate_builder {
		return chandlerv1.ResourceUpdate_builder{
			WatchId:  proto.String(id),
			Nonce:    proto.String(s.nextNonce()),
			CacheKey: proto.String(key),
			Revision: proto.String(revision),
		}
	}
	emit := func(b chandlerv1.ResourceUpdate_builder) {
		kind := "bundle"
		switch {
		case b.NotModified != nil:
			kind = "not_modified"
		case b.Deleted != nil:
			kind = "deleted"
		}
		s.server.count(s.server.updates, "kind", kind)
		s.send(ctx, chandlerv1.ProviderResponse_builder{Update: b.Build()}.Build())
	}

	resource, err := s.server.resolve(rawURI)
	if err != nil {
		fail(codes.InvalidArgument, err.Error())
		return
	}
	key := resource.CacheKey()

	// revision is the last revision the server was told about; "" when it
	// holds none. reported is the last error code sent, so a persisting
	// failure is reported once.
	revision, reported := known, codes.OK
	// share is the share of subscribers last sent for revision, in basis
	// points; 0 when none was sent on this watch.
	share := uint32(0)
	first := true
	for {
		state, err := resource.Fetch(ctx, revision, s.limits)
		switch {
		case ctx.Err() != nil:
			return
		case err == nil:
			if state.Revision != revision {
				share = 0
			}
			now := s.server.share(revisionKey{key, state.Revision}, state, share)
			u := update(key, state.Revision)
			u.CacheControl = state.CacheControl
			if now < rollout.Everyone {
				u.Rollout = chandlerv1.Rollout_builder{BasisPoints: proto.Uint32(now)}.Build()
			}
			switch {
			case state.Bundle != nil:
				u.Bundle = state.Bundle
				emit(u)
			case (first && known != "") || now != share:
				// Confirming a known revision, or moving its rollout on:
				// no bytes either way.
				u.NotModified = &chandlerv1.NotModified{}
				emit(u)
			}
			revision, share, reported = state.Revision, now, codes.OK
		case errors.Is(err, ErrInvalid):
			fail(codes.InvalidArgument, err.Error())
			return
		case errors.Is(err, ErrNotFound) && revision != "":
			// It existed and is gone.
			u := update(key, revision)
			u.Deleted = &chandlerv1.Deleted{}
			emit(u)
			revision, share, reported = "", 0, codes.NotFound
		default:
			code := errorCode(err)
			if code != reported {
				fail(code, err.Error())
				reported = code
			}
			if code != codes.NotFound {
				s.log.Warn("fetch failed", "uri", rawURI, "error", err)
			}
		}
		first = false

		wait := s.server.PollInterval
		wait += time.Duration(rand.Int64N(int64(wait)/5+1)) - wait/10
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// resolve returns the resource a URI names.
func (s *Server) resolve(raw string) (Resource, error) {
	u, err := uri.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Provider != s.Name {
		return nil, fmt.Errorf("%q is not a %s:// URI", raw, s.Name)
	}
	return s.Origin.Resolve(u)
}

func errorCode(err error) codes.Code {
	switch {
	case errors.Is(err, ErrNotFound):
		return codes.NotFound
	case errors.Is(err, ErrPermission), errors.Is(err, fs.ErrPermission):
		return codes.PermissionDenied
	case errors.Is(err, bundle.ErrTooLarge), errors.Is(err, bundle.ErrTooManyFiles):
		return codes.ResourceExhausted
	default:
		return codes.Unavailable
	}
}
