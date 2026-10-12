// Package cache is the node cache: the bundles this node has written, kept so
// that a version the server names can be applied without its bytes being sent
// again, and so that a pod can start from what the node already holds while
// the server is unreachable.
//
// Layout below the cache directory:
//
//	blobs/sha256/<aa>/<hex>     file contents, content-addressed
//	manifests/<hex>.json        one bundle: its files and the blob of each
//	index.json                  URI -> the versions held for it, newest first
//
// A blob is shared by every bundle that contains the same bytes. Nothing here
// opens or interprets file contents.
package cache

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
	"time"

	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
)

const (
	// VersionsPerURI is how many versions are kept for one URI: the current
	// one and the one before it, so a rollback needs no bytes.
	VersionsPerURI = 2

	// DefaultGrace is how long the versions of a URI no volume uses are
	// kept, so pods can restart through an outage.
	DefaultGrace = 24 * time.Hour

	dirMode  = 0o700
	fileMode = 0o600
)

// ErrNotCached reports a version the cache does not hold, or holds damaged.
var ErrNotCached = errors.New("version is not in the node cache")

// Cache is the node cache. Its methods are safe for concurrent use.
type Cache struct {
	dir   string
	grace time.Duration
	now   func() time.Time

	mu    sync.Mutex
	index map[string]*entry
}

// entry is what is held for one URI.
type entry struct {
	// Versions is newest first, at most VersionsPerURI.
	Versions []string `json:"versions"`
	// LastUsed is when a volume last used the URI.
	LastUsed time.Time `json:"lastUsed"`
}

type manifest struct {
	Files []manifestFile `json:"files"`
}

type manifestFile struct {
	Path string `json:"path"`
	Mode uint32 `json:"mode,omitempty"`
	// HasMode tells an unset mode from mode 0, which hash differently.
	HasMode bool   `json:"hasMode,omitempty"`
	Blob    string `json:"blob"`
}

// Open opens the cache in dir, creating it if needed. An unreadable index is
// treated as empty: the cache is only ever a copy of what the server has.
func Open(dir string, grace time.Duration) (*Cache, error) {
	for _, sub := range []string{"blobs/sha256", "manifests"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), dirMode); err != nil {
			return nil, fmt.Errorf("create node cache: %w", err)
		}
	}
	c := &Cache{dir: dir, grace: grace, now: time.Now, index: make(map[string]*entry)}
	raw, err := os.ReadFile(filepath.Join(dir, "index.json"))
	if err == nil {
		if json.Unmarshal(raw, &c.index) != nil {
			c.index = make(map[string]*entry)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("read node cache index: %w", err)
	}
	return c, nil
}

// Put stores a bundle as the newest version of a URI. The bundle must already
// be verified against version.
func (c *Cache) Put(uri, version string, b *chandlerv1.Bundle) error {
	name, err := manifestName(version)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	var m manifest
	for _, f := range b.GetFiles() {
		sum := sha256.Sum256(f.GetContent())
		blob := hex.EncodeToString(sum[:])
		if err := c.writeBlob(blob, f.GetContent()); err != nil {
			return err
		}
		m.Files = append(m.Files, manifestFile{Path: f.GetPath(), Mode: f.GetMode(), HasMode: f.HasMode(), Blob: blob})
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := writeAtomic(filepath.Join(c.dir, "manifests", name), raw); err != nil {
		return err
	}
	return c.recordLocked(uri, version)
}

// Use marks version as the newest of a URI without storing anything: the
// version is already held, under this URI or another.
func (c *Cache) Use(uri, version string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recordLocked(uri, version)
}

func (c *Cache) recordLocked(uri, version string) error {
	e := c.index[uri]
	if e == nil {
		e = &entry{}
		c.index[uri] = e
	}
	versions := []string{version}
	for _, v := range e.Versions {
		if v != version && len(versions) < VersionsPerURI {
			versions = append(versions, v)
		}
	}
	e.Versions, e.LastUsed = versions, c.now()
	return c.saveLocked()
}

// Get returns the bundle with the given version, verified against it. The
// error wraps ErrNotCached when the cache does not hold it intact.
func (c *Cache) Get(version string) (*chandlerv1.Bundle, error) {
	name, err := manifestName(version)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	raw, err := os.ReadFile(filepath.Join(c.dir, "manifests", name))
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrNotCached, version)
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("%w: %s: damaged manifest", ErrNotCached, version)
	}
	files := make([]*chandlerv1.File, 0, len(m.Files))
	for _, mf := range m.Files {
		content, err := os.ReadFile(c.blobPath(mf.Blob))
		if err != nil {
			return nil, fmt.Errorf("%w: %s: missing blob", ErrNotCached, version)
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
		return nil, fmt.Errorf("%w: %s: content does not match", ErrNotCached, version)
	}
	return b, nil
}

// Versions returns the versions held for a URI, newest first.
func (c *Cache) Versions(uri string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.index[uri]; e != nil {
		return append([]string(nil), e.Versions...)
	}
	return nil
}

// Touch records that the given URIs are in use now, so that Collect keeps
// them however old they are.
func (c *Cache) Touch(uris ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	changed := false
	for _, uri := range uris {
		if e := c.index[uri]; e != nil {
			e.LastUsed, changed = c.now(), true
		}
	}
	if changed {
		_ = c.saveLocked()
	}
}

// Collect removes what is no longer needed: URIs unused for longer than the
// grace period, then every manifest and blob nothing refers to. inUse is the
// URIs of the volumes that exist now; they are never removed.
func (c *Cache) Collect(inUse []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	live := make(map[string]bool, len(inUse))
	for _, uri := range inUse {
		live[uri] = true
	}
	cutoff := c.now().Add(-c.grace)
	for uri, e := range c.index {
		if !live[uri] && e.LastUsed.Before(cutoff) {
			delete(c.index, uri)
		}
	}
	if err := c.saveLocked(); err != nil {
		return err
	}

	manifests, blobs := make(map[string]bool), make(map[string]bool)
	for _, e := range c.index {
		for _, version := range e.Versions {
			name, err := manifestName(version)
			if err != nil {
				continue
			}
			manifests[name] = true
			raw, err := os.ReadFile(filepath.Join(c.dir, "manifests", name))
			if err != nil {
				continue
			}
			var m manifest
			if json.Unmarshal(raw, &m) == nil {
				for _, f := range m.Files {
					blobs[f.Blob] = true
				}
			}
		}
	}
	if err := sweep(filepath.Join(c.dir, "manifests"), manifests); err != nil {
		return err
	}
	return sweep(filepath.Join(c.dir, "blobs", "sha256"), blobs)
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

func (c *Cache) blobPath(blob string) string {
	return filepath.Join(c.dir, "blobs", "sha256", blob[:2], blob)
}

func (c *Cache) writeBlob(blob string, content []byte) error {
	p := c.blobPath(blob)
	if _, err := os.Stat(p); err == nil {
		return nil // content-addressed: already there
	}
	if err := os.MkdirAll(filepath.Dir(p), dirMode); err != nil {
		return fmt.Errorf("write node cache: %w", err)
	}
	return writeAtomic(p, content)
}

func (c *Cache) saveLocked() error {
	raw, err := json.Marshal(c.index)
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(c.dir, "index.json"), raw)
}

// writeAtomic writes a file so that a reader sees all of it or none.
func writeAtomic(path string, content []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("write node cache: %w", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		return fmt.Errorf("write node cache: %w", err)
	}
	if err := tmp.Chmod(fileMode); err != nil {
		tmp.Close()
		return fmt.Errorf("write node cache: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("write node cache: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("write node cache: %w", err)
	}
	return nil
}

// manifestName returns the file name of a version's manifest. It refuses
// anything that is not a well-formed version, so a version string can never
// name a path outside the cache.
func manifestName(version string) (string, error) {
	digest, ok := strings.CutPrefix(version, bundle.VersionPrefix)
	if !ok || len(digest) != sha256.Size*2 {
		return "", fmt.Errorf("%w: %q is not a version", ErrNotCached, version)
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return "", fmt.Errorf("%w: %q is not a version", ErrNotCached, version)
	}
	return digest + ".json", nil
}
