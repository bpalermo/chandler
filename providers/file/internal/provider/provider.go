// Package provider implements ConfigProvider over a directory on disk. It is
// the smallest provider there can be: no credentials, no network origin, and
// change detection by polling.
package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
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
	"github.com/bpalermo/chandler/providers/file/internal/source"
)

// stableReads is how many times a resource is re-read while two consecutive
// reads disagree, before the newest read is used anyway.
const stableReads = 3

// Server serves ConfigProvider for the files below Root.
type Server struct {
	chandlerv1.UnimplementedConfigProviderServer

	// Name is the provider's registered name: the scheme of the URIs it serves.
	Name string
	// Root is the directory URIs are resolved against.
	Root string
	// PollInterval is how often a watched resource is re-read.
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
		reader: source.Reader{Root: s.Root, Limits: limits},
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
			// Nothing to do: the origin is a directory, with nobody to tell.
		}
	}
}

// session is one server's stream.
type session struct {
	server *Server
	reader source.Reader
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

	path, err := s.server.resourcePath(rawURI)
	if err != nil {
		fail(codes.InvalidArgument, err.Error())
		return
	}

	// revision is the last revision the server was told about; "" when it
	// holds none. reported is the last error code sent, so a persisting
	// failure is reported once.
	revision, reported := known, codes.OK
	first := true
	ticker := time.NewTicker(s.server.PollInterval)
	defer ticker.Stop()
	for {
		b, err := s.readStable(path)
		switch {
		case err == nil:
			current := bundle.Version(b)
			switch {
			case current != revision:
				u := update(path, current)
				u.Bundle = b
				emit(u)
			case first && known != "":
				u := update(path, current)
				u.NotModified = &chandlerv1.NotModified{}
				emit(u)
			}
			revision, reported = current, codes.OK
		case errors.Is(err, source.ErrInvalid):
			fail(codes.InvalidArgument, err.Error())
			return
		case errors.Is(err, source.ErrNotFound) && revision != "":
			// It existed and is gone.
			u := update(path, revision)
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
				s.log.Warn("read failed", "uri", rawURI, "error", err)
			}
		}
		first = false

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// readStable reads a resource until two consecutive reads agree, so that a
// directory being rewritten is not emitted half old and half new. A directory
// swapped atomically (a ConfigMap mount) agrees on the second read.
func (s *session) readStable(path string) (*chandlerv1.Bundle, error) {
	b, err := s.reader.Read(path)
	for i := 0; err == nil && i < stableReads; i++ {
		var again *chandlerv1.Bundle
		again, err = s.reader.Read(path)
		if err != nil || bundle.Version(again) == bundle.Version(b) {
			break
		}
		b = again
	}
	return b, err
}

// resourcePath returns the path a URI names below the root. The provider
// understands nothing but a path: an authority or any query parameter is
// refused rather than ignored, so that decorating a URI cannot name the same
// file under another spelling.
func (s *Server) resourcePath(raw string) (string, error) {
	u, err := uri.Parse(raw)
	switch {
	case err != nil:
		return "", err
	case u.Provider != s.Name:
		return "", fmt.Errorf("%q is not a %s:// URI", raw, s.Name)
	case u.Authority() != "":
		return "", fmt.Errorf("%q has an authority; write %s:///<path>", raw, s.Name)
	case len(u.Query()) != 0:
		return "", fmt.Errorf("%q has query parameters, which this provider does not take", raw)
	case u.Path() == "":
		return "", fmt.Errorf("%q names no path", raw)
	}
	return u.Path(), nil
}

func errorCode(err error) codes.Code {
	switch {
	case errors.Is(err, source.ErrNotFound):
		return codes.NotFound
	case errors.Is(err, fs.ErrPermission):
		return codes.PermissionDenied
	case errors.Is(err, bundle.ErrTooLarge), errors.Is(err, bundle.ErrTooManyFiles):
		return codes.ResourceExhausted
	default:
		return codes.Unavailable
	}
}
