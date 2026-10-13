// Package cache is the node cache: the bundles this node has written, kept so
// that a version the server names can be applied without its bytes being sent
// again, and so that a pod can start from what the node already holds while
// the server is unreachable.
//
// It is a bundle store (common/cas) and, beside it, index.json: for each
// canonical URI, the versions held, newest first.
package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/cas"
)

const (
	// VersionsPerURI is how many versions are kept for one URI: the current
	// one and the one before it, so a rollback needs no bytes.
	VersionsPerURI = 2

	// DefaultGrace is how long the versions of a URI no volume uses are
	// kept, so pods can restart through an outage.
	DefaultGrace = 24 * time.Hour
)

// ErrNotCached reports a version the cache does not hold, or holds damaged.
var ErrNotCached = cas.ErrNotFound

// Cache is the node cache. Its methods are safe for concurrent use.
type Cache struct {
	dir   string
	store *cas.Store
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

// Open opens the cache in dir, creating it if needed. An unreadable index is
// treated as empty: the cache is only ever a copy of what the server has.
func Open(dir string, grace time.Duration) (*Cache, error) {
	store, err := cas.Open(dir)
	if err != nil {
		return nil, err
	}
	c := &Cache{dir: dir, store: store, grace: grace, now: time.Now, index: make(map[string]*entry)}
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
	if err := c.store.Put(version, b); err != nil {
		return err
	}
	return c.Use(uri, version)
}

// Use marks version as the newest of a URI without storing anything: the
// version is already held, under this URI or another.
func (c *Cache) Use(uri, version string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	return c.store.Get(version)
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

// Stats returns how many URIs the cache holds versions for, and how many
// bytes of file content it keeps on disk.
func (c *Cache) Stats() (uris int, bytes int64) {
	c.mu.Lock()
	uris = len(c.index)
	c.mu.Unlock()
	_ = filepath.WalkDir(filepath.Join(c.dir, "blobs"), func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil // a blob collected meanwhile is not an error
		}
		if info, err := d.Info(); err == nil {
			bytes += info.Size()
		}
		return nil
	})
	return uris, bytes
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
// grace period, then every bundle and blob nothing refers to. inUse is the
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
	var keep []string
	for _, e := range c.index {
		keep = append(keep, e.Versions...)
	}
	return c.store.Collect(keep)
}

func (c *Cache) saveLocked() error {
	raw, err := json.Marshal(c.index)
	if err != nil {
		return err
	}
	return cas.WriteAtomic(filepath.Join(c.dir, "index.json"), raw)
}
