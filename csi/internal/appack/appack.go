// Package appack serves the application acknowledgement API of one volume,
// stevedore.v1.ApplicationService, on a Unix socket inside the volume.
//
// The socket answers the same service in two ways, told apart by the
// Content-Type of the request:
//
//	GET  /v1/config    the current generation, and the state of each source
//	POST /v1/reports   whether the application loaded or rejected a generation
//
// as plain HTTP with JSON bodies, so that an application needs no client
// library and curl will do; and gRPC, for one that prefers a generated
// client. The HTTP paths come from the google.api.http options in the proto.
//
// Each volume has its own socket, so the driver knows which pod is speaking
// without authentication; any container that mounts the volume may report,
// which lets a sidecar report on behalf of an application that cannot be
// changed. That is also why this is the one place that serves without
// common/transport: there is no peer to identify and no network to cross.
package appack

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	stevedorev1 "github.com/bpalermo/stevedore/api/stevedore/v1"
	"github.com/bpalermo/stevedore/csi/internal/volume"
)

// maxReportBytes bounds a request.
const maxReportBytes = 64 << 10

// Volume is what the API serves.
type Volume interface {
	Config() *stevedorev1.Manifest
	Report(*stevedorev1.ReportRequest) ([]volume.Outcome, *stevedorev1.Manifest, error)
}

// Server is the API of one volume.
type Server struct {
	http     *http.Server
	grpc     *grpc.Server
	listener net.Listener
	socket   string
}

// Serve starts the API on the socket in dir, the volume's data directory.
// forward is called with the verdicts of every accepted report.
func Serve(dir string, v Volume, forward func([]volume.Outcome)) (*Server, error) {
	api := &service{volume: v, forward: forward}

	// gRPC, served through the HTTP server below rather than on a listener
	// of its own.
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(maxReportBytes))
	stevedorev1.RegisterApplicationServiceServer(grpcServer, api)

	// HTTP with JSON. The gateway calls the service in this process. Unknown
	// fields are refused, so that a misspelt one is not silently ignored.
	gateway := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions:   protojson.MarshalOptions{},
			UnmarshalOptions: protojson.UnmarshalOptions{},
		}),
		runtime.WithForwardResponseOption(noContent),
	)
	if err := stevedorev1.RegisterApplicationServiceHandlerServer(context.Background(), gateway, api); err != nil {
		return nil, fmt.Errorf("register the HTTP API: %w", err)
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxReportBytes)
		gateway.ServeHTTP(&bodiless{ResponseWriter: w}, r)
	})
	// HTTP/1.1 for curl and the like, and HTTP/2 without TLS for gRPC.
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	socket := filepath.Join(dir, volume.SocketName)
	if err := os.Remove(socket); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("remove stale socket: %w", err)
	}
	// The host path of a pod volume is far longer than the 108 bytes a
	// socket address holds, so bind through a descriptor of the directory.
	fd, err := unix.Open(dir, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open volume directory: %w", err)
	}
	defer unix.Close(fd)
	short := fmt.Sprintf("/proc/self/fd/%d/%s", fd, volume.SocketName)
	listener, err := net.Listen("unix", short)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", volume.SocketName, err)
	}
	// The address stops meaning anything when the descriptor closes; the
	// socket is removed by its real path in Close.
	listener.(*net.UnixListener).SetUnlinkOnClose(false)
	// Connecting needs write permission on the socket, and the application
	// does not run as the driver's user.
	if err := os.Chmod(short, 0o666); err != nil {
		listener.Close()
		return nil, fmt.Errorf("chmod %s: %w", volume.SocketName, err)
	}

	s := &Server{
		http:     &http.Server{Handler: handler, Protocols: protocols, ReadHeaderTimeout: 10 * time.Second},
		grpc:     grpcServer,
		listener: listener,
		socket:   socket,
	}
	go s.http.Serve(listener)
	return s, nil
}

// Close stops the API and removes the socket.
func (s *Server) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = s.http.Shutdown(ctx)
	_ = s.http.Close()
	s.grpc.Stop()
	_ = os.Remove(s.socket)
}

// service is stevedore.v1.ApplicationService for one volume.
type service struct {
	stevedorev1.UnimplementedApplicationServiceServer

	volume  Volume
	forward func([]volume.Outcome)
}

func (s *service) GetConfig(context.Context, *stevedorev1.GetConfigRequest) (*stevedorev1.GetConfigResponse, error) {
	return stevedorev1.GetConfigResponse_builder{Config: s.volume.Config()}.Build(), nil
}

func (s *service) Report(_ context.Context, r *stevedorev1.ReportRequest) (*stevedorev1.ReportResponse, error) {
	outcomes, current, err := s.volume.Report(r)
	switch {
	case errors.Is(err, volume.ErrMalformedReport):
		return nil, status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, volume.ErrUnknownSource):
		return nil, status.Error(codes.NotFound, err.Error())
	case errors.Is(err, volume.ErrStaleGeneration):
		st := status.New(codes.Aborted, err.Error())
		if current != nil {
			// The current configuration, to reload and report on.
			if detailed, derr := st.WithDetails(current); derr == nil {
				st = detailed
			}
		}
		return nil, st.Err()
	case err != nil:
		return nil, status.Error(codes.Internal, err.Error())
	}
	if len(outcomes) > 0 {
		s.forward(outcomes)
	}
	return &stevedorev1.ReportResponse{}, nil
}

// noContent answers an accepted report over HTTP with 204: there is nothing
// to say, and a client need not read a body to know it.
func noContent(_ context.Context, w http.ResponseWriter, m proto.Message) error {
	if _, ok := m.(*stevedorev1.ReportResponse); ok {
		w.WriteHeader(http.StatusNoContent)
	}
	return nil
}

// bodiless drops the body the gateway writes after noContent: a 204 has
// none, and net/http reports an error for the attempt.
type bodiless struct {
	http.ResponseWriter
	empty bool
}

func (b *bodiless) WriteHeader(code int) {
	b.empty = code == http.StatusNoContent
	b.ResponseWriter.WriteHeader(code)
}

func (b *bodiless) Write(p []byte) (int, error) {
	if b.empty {
		return len(p), nil
	}
	return b.ResponseWriter.Write(p)
}
