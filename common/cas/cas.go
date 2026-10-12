// Package cas stores bundles by their version, on disk. File contents are
// content-addressed blobs, shared by every bundle that contains the same
// bytes; a manifest per bundle lists its files and the blob of each.
//
// Layout below the store's directory:
//
//	blobs/sha256/<aa>/<hex>     file contents
//	manifests/<hex>.json        one bundle
//
// A bundle is verified against its version every time it is read, so a
// damaged store yields a miss, never wrong bytes. Nothing here opens or
// interprets file contents.
package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
)

const (
	dirMode  = 0o700
	fileMode = 0o600
)

// ErrNotFound reports a version the store does not hold, or holds damaged.
var ErrNotFound = errors.New("version is not stored")

// Store is a bundle store. Its methods are safe for concurrent use.
type Store struct {
	dir string
	mu  sync.Mutex
}

type manifest struct {
	Files []manifestFile `json:"files"`
}

type manifestFile struct {
	Path string `json:"path"`
	Mode uint32 `json:"mode,omitempty"`
	// HasMode tells an unset mode from mode 0.
	HasMode bool   `json:"hasMode,omitempty"`
	Blob    string `json:"blob"`
}

// Open opens the store in dir, creating it if needed.
func Open(dir string) (*Store, error) {
	for _, sub := range []string{"blobs/sha256", "manifests"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), dirMode); err != nil {
			return nil, fmt.Errorf("create bundle store: %w", err)
		}
	}
	return &Store{dir: dir}, nil
}

// Put stores a bundle under version. The bundle must already be verified
// against it.
func (s *Store) Put(version string, b *chandlerv1.Bundle) error {
	name, err := manifestName(version)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var m manifest
	for _, f := range b.GetFiles() {
		sum := sha256.Sum256(f.GetContent())
		blob := hex.EncodeToString(sum[:])
		if err := s.writeBlob(blob, f.GetContent()); err != nil {
			return err
		}
		m.Files = append(m.Files, manifestFile{Path: f.GetPath(), Mode: f.GetMode(), HasMode: f.HasMode(), Blob: blob})
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return WriteAtomic(filepath.Join(s.dir, "manifests", name), raw)
}

// Get returns the bundle with the given version, verified against it. The
// error wraps ErrNotFound when the store does not hold it intact.
func (s *Store) Get(version string) (*chandlerv1.Bundle, error) {
	name, err := manifestName(version)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	m, err := s.readManifest(name)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, version)
	}
	files := make([]*chandlerv1.File, 0, len(m.Files))
	for _, mf := range m.Files {
		if len(mf.Blob) != sha256.Size*2 {
			return nil, fmt.Errorf("%w: %s: damaged manifest", ErrNotFound, version)
		}
		content, err := os.ReadFile(s.blobPath(mf.Blob))
		if err != nil {
			return nil, fmt.Errorf("%w: %s: missing blob", ErrNotFound, version)
		}
		f := chandlerv1.File_builder{Path: proto.String(mf.Path), Content: content}
		if mf.HasMode {
			f.Mode = proto.Uint32(mf.Mode)
		}
		files = append(files, f.Build())
	}
	b := chandlerv1.Bundle_builder{Files: files}.Build()
	// Disks rot: never hand out bytes that are not the version asked for.
	if !bundle.Verify(b, version) {
		return nil, fmt.Errorf("%w: %s: content does not match", ErrNotFound, version)
	}
	return b, nil
}

// Collect removes every bundle whose version is not in keep, then every blob
// no remaining bundle uses.
func (s *Store) Collect(keep []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	manifests, blobs := make(map[string]bool), make(map[string]bool)
	for _, version := range keep {
		name, err := manifestName(version)
		if err != nil {
			continue
		}
		manifests[name] = true
		if m, err := s.readManifest(name); err == nil {
			for _, f := range m.Files {
				blobs[f.Blob] = true
			}
		}
	}
	if err := sweep(filepath.Join(s.dir, "manifests"), manifests); err != nil {
		return err
	}
	return sweep(filepath.Join(s.dir, "blobs", "sha256"), blobs)
}

func (s *Store) readManifest(name string) (manifest, error) {
	var m manifest
	raw, err := os.ReadFile(filepath.Join(s.dir, "manifests", name))
	if err != nil {
		return m, err
	}
	return m, json.Unmarshal(raw, &m)
}

// sweep removes every file below dir whose name is not in keep.
func sweep(dir string, keep map[string]bool) error {
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if !keep[d.Name()] {
			if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}
		return nil
	})
}

func (s *Store) blobPath(blob string) string {
	return filepath.Join(s.dir, "blobs", "sha256", blob[:2], blob)
}

func (s *Store) writeBlob(blob string, content []byte) error {
	p := s.blobPath(blob)
	if _, err := os.Stat(p); err == nil {
		return nil // content-addressed: already there
	}
	if err := os.MkdirAll(filepath.Dir(p), dirMode); err != nil {
		return fmt.Errorf("write bundle store: %w", err)
	}
	return WriteAtomic(p, content)
}

// WriteAtomic writes a file so that a reader sees all of it or none.
func WriteAtomic(path string, content []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := tmp.Chmod(fileMode); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	return nil
}

// manifestName returns the file name of a version's manifest. It refuses
// anything that is not a well-formed version, so a version string can never
// name a path outside the store.
func manifestName(version string) (string, error) {
	digest, ok := strings.CutPrefix(version, bundle.VersionPrefix)
	if !ok || len(digest) != sha256.Size*2 {
		return "", fmt.Errorf("%w: %q is not a version", ErrNotFound, version)
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return "", fmt.Errorf("%w: %q is not a version", ErrNotFound, version)
	}
	return digest + ".json", nil
}
