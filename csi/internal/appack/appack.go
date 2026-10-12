// Package appack serves the application acknowledgement API of one volume: a
// plain HTTP/1.1 API with JSON bodies, on a Unix socket inside the volume.
//
//	GET  /v1/config    the current generation, and the state of each source
//	POST /v1/reports   whether the application loaded or rejected a generation
//
// There is no gRPC and no client library: any HTTP client that can use a Unix
// socket will do, curl included. Each volume has its own socket, so the
// driver knows which pod is speaking without authentication; any container
// that mounts the volume may report, which lets a sidecar report on behalf of
// an application that cannot be changed.
package appack

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"

	"github.com/bpalermo/chandler/csi/internal/volume"
)

// maxReportBytes bounds a report's body.
const maxReportBytes = 64 << 10

// Volume is what the API serves.
type Volume interface {
	Config() volume.Manifest
	Report(volume.Report) ([]volume.Outcome, volume.Manifest, error)
}

// Server is the API of one volume.
type Server struct {
	http     *http.Server
	listener net.Listener
	socket   string
}

// Serve starts the API on the socket in dir, the volume's data directory.
// forward is called with the verdicts of every accepted report.
func Serve(dir string, v Volume, forward func([]volume.Outcome)) (*Server, error) {
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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/config", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, v.Config())
	})
	mux.HandleFunc("POST /v1/reports", func(w http.ResponseWriter, r *http.Request) {
		var report volume.Report
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxReportBytes))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&report); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("malformed report: %v", err))
			return
		}
		outcomes, current, err := v.Report(report)
		switch {
		case errors.Is(err, volume.ErrMalformedReport):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, volume.ErrUnknownSource):
			writeError(w, http.StatusNotFound, err.Error())
		case errors.Is(err, volume.ErrStaleGeneration):
			// The current configuration, to reload and report on.
			writeJSON(w, http.StatusConflict, current)
		case err != nil:
			writeError(w, http.StatusInternalServerError, err.Error())
		default:
			if len(outcomes) > 0 {
				forward(outcomes)
			}
			w.WriteHeader(http.StatusNoContent)
		}
	})

	s := &Server{
		http:     &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second},
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
	_ = os.Remove(s.socket)
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}
