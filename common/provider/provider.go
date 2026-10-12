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

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
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
	Log          *slog.Logger
}

// StreamConfigs implements chandlerv1.ConfigProviderServer.
func (s *Server) StreamConfigs(stream chandlerv1.ConfigProvider_StreamConfigsServer) error {
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
		case req.HasAck(), req.HasFeedback():
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
	fail := func(code codes.Code, msg string) {
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
	first := true
	for {
		state, err := resource.Fetch(ctx, revision, s.limits)
		switch {
		case ctx.Err() != nil:
			return
		case err == nil:
			switch {
			case state.Bundle != nil:
				u := update(key, state.Revision)
				u.Bundle, u.CacheControl = state.Bundle, state.CacheControl
				emit(u)
			case first && known != "":
				u := update(key, state.Revision)
				u.NotModified, u.CacheControl = &chandlerv1.NotModified{}, state.CacheControl
				emit(u)
			}
			revision, reported = state.Revision, codes.OK
		case errors.Is(err, ErrInvalid):
			fail(codes.InvalidArgument, err.Error())
			return
		case errors.Is(err, ErrNotFound) && revision != "":
			// It existed and is gone.
			u := update(key, revision)
			u.Deleted = &chandlerv1.Deleted{}
			emit(u)
			revision, reported = "", codes.NotFound
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
