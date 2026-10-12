package hub

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/cas"
)

const (
	indexFile = "index.json"
	// keepInterval is how often live entries are stamped and unused ones
	// collected.
	keepInterval = 30 * time.Second
)

// index is the cache directory's index.json: everything about the entries
// except their content, which is in the bundle store.
type index struct {
	Providers map[string]indexProvider `json:"providers"`
	Entries   []indexEntry             `json:"entries"`
	Aliases   map[string]alias         `json:"aliases"`
}

type indexProvider struct {
	VaryOn []string `json:"varyOn"`
}

type indexEntry struct {
	Provider       string    `json:"provider"`
	CacheKey       string    `json:"cacheKey"`
	Versions       []held    `json:"versions"`
	Candidate      *held     `json:"candidate,omitempty"`
	Share          uint32    `json:"share,omitempty"`
	MaxAge         *int64    `json:"maxAgeSeconds,omitempty"`
	StaleIfError   *int64    `json:"staleIfErrorSeconds,omitempty"`
	LastRefreshed  time.Time `json:"lastRefreshed"`
	LastReferenced time.Time `json:"lastReferenced"`
	Orphaned       bool      `json:"orphaned,omitempty"`
}

// load reads the cache directory. An entry whose content is missing or
// damaged is dropped: the provider is asked again.
func (h *Hub) load() error {
	raw, err := os.ReadFile(filepath.Join(h.cacheDir, indexFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var idx index
	if json.Unmarshal(raw, &idx) != nil {
		h.log.Warn("the cache index is unreadable; starting empty")
		return nil
	}
	for name, p := range idx.Providers {
		h.upstreams[name] = &upstream{hub: h, name: name, varyOn: p.VaryOn}
	}
	for _, ie := range idx.Entries {
		e := &entry{
			key:            entryKey{ie.Provider, ie.CacheKey},
			bundles:        make(map[string]*chandlerv1.Bundle),
			lastRefreshed:  ie.LastRefreshed,
			lastReferenced: ie.LastReferenced,
			orphaned:       ie.Orphaned,
			refs:           make(map[*resource]struct{}),
		}
		if ie.MaxAge != nil || ie.StaleIfError != nil {
			var cc chandlerv1.CacheControl_builder
			if ie.MaxAge != nil {
				cc.MaxAge = durationpb.New(time.Duration(*ie.MaxAge) * time.Second)
			}
			if ie.StaleIfError != nil {
				cc.StaleIfError = durationpb.New(time.Duration(*ie.StaleIfError) * time.Second)
			}
			e.cacheControl = cc.Build()
		}
		for _, v := range ie.Versions {
			b, err := h.store.Get(v.Version)
			if err != nil {
				if len(e.versions) == 0 {
					break // without its current version an entry is nothing
				}
				continue
			}
			e.versions, e.bundles[v.Version] = append(e.versions, v), b
		}
		if len(e.versions) == 0 {
			h.log.Warn("dropping a cache entry whose content is gone", "provider", ie.Provider, "cache_key", ie.CacheKey)
			continue
		}
		if c := ie.Candidate; c != nil {
			// Without its content a rollout is forgotten; the provider
			// starts it again when it answers.
			if b, err := h.store.Get(c.Version); err == nil {
				e.candidate, e.share, e.bundles[c.Version] = c, ie.Share, b
			}
		}
		h.entries[e.key] = e
	}
	for id, a := range idx.Aliases {
		if h.entries[entryKey{a.Provider, a.CacheKey}] != nil {
			h.aliases[id] = a
		}
	}
	h.log.Info("loaded the cache", "entries", len(h.entries), "aliases", len(h.aliases))
	return nil
}

// saveLocked writes the index. A failure is logged, not returned: the cache
// is an optimisation over asking the provider again.
func (h *Hub) saveLocked() {
	if h.store == nil {
		return
	}
	idx := index{Providers: make(map[string]indexProvider), Aliases: make(map[string]alias)}
	for name, up := range h.upstreams {
		idx.Providers[name] = indexProvider{VaryOn: up.varyOn}
	}
	for _, e := range h.entries {
		if e.cacheControl.GetNoStore() {
			continue // never written to disk, not even by name
		}
		ie := indexEntry{
			Provider:       e.key.provider,
			CacheKey:       e.key.cacheKey,
			Versions:       e.versions,
			Candidate:      e.candidate,
			Share:          e.share,
			LastRefreshed:  e.lastRefreshed,
			LastReferenced: e.lastReferenced,
			Orphaned:       e.orphaned,
		}
		if e.cacheControl.HasMaxAge() {
			ie.MaxAge = proto.Int64(e.cacheControl.GetMaxAge().GetSeconds())
		}
		if e.cacheControl.HasStaleIfError() {
			ie.StaleIfError = proto.Int64(e.cacheControl.GetStaleIfError().GetSeconds())
		}
		idx.Entries = append(idx.Entries, ie)
	}
	for id, a := range h.aliases {
		if e := h.entries[entryKey{a.Provider, a.CacheKey}]; e != nil && !e.cacheControl.GetNoStore() {
			idx.Aliases[id] = a
		}
	}
	raw, err := json.Marshal(idx)
	if err == nil {
		err = cas.WriteAtomic(filepath.Join(h.cacheDir, indexFile), raw)
	}
	if err != nil {
		h.log.Error("could not write the cache index", "error", err)
	}
}

// keep stamps what is live and collects what is no longer needed, until ctx
// ends.
func (h *Hub) keep(ctx context.Context) {
	ticker := time.NewTicker(keepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.collect()
		}
	}
}

// collect records that live entries are still fresh and referenced, so that a
// crash loses at most one interval of that knowledge, then drops entries
// nothing has referred to for the grace period, and the content only they
// used.
func (h *Hub) collect() {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	var keep []string
	for key, e := range h.entries {
		if len(e.refs) > 0 {
			e.lastReferenced = now
			if e.live() {
				e.lastRefreshed = now
			}
		} else if now.Sub(e.lastReferenced) > h.grace {
			delete(h.entries, key)
			continue
		}
		for _, v := range e.all() {
			keep = append(keep, v.Version)
		}
		h.reportLocked(e)
	}
	for id, a := range h.aliases {
		if h.entries[entryKey{a.Provider, a.CacheKey}] == nil {
			delete(h.aliases, id)
		}
	}
	for id, n := range h.negative {
		if !now.Before(n.until) {
			delete(h.negative, id)
		}
	}
	h.saveLocked()
	if h.store != nil {
		if err := h.store.Collect(keep); err != nil {
			h.log.Error("could not collect the cache", "error", err)
		}
	}
}
