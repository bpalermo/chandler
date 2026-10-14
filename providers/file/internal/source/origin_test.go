package source

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bpalermo/stevedore/common/bundle"
	"github.com/bpalermo/stevedore/common/provider"
	"github.com/bpalermo/stevedore/common/rollout"
	"github.com/bpalermo/stevedore/common/uri"
)

func resolve(t *testing.T, root, raw string) (provider.Resource, error) {
	t.Helper()
	u, err := uri.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return Origin{Root: root}.Resolve(u)
}

func TestOriginResolve(t *testing.T) {
	root := t.TempDir()
	r, err := resolve(t, root, "file:///team/../team/app.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if got := r.CacheKey(); got != "/team/app.yaml" {
		t.Errorf("cache key = %q, want the canonical path", got)
	}
	// Refused, not ignored: each would name the same file a second way.
	for _, raw := range []string{"file://host/app.yaml", "file:///app.yaml?version=2", "file://"} {
		if _, err := resolve(t, root, raw); !errors.Is(err, provider.ErrInvalid) {
			t.Errorf("Resolve(%s) = %v, want ErrInvalid", raw, err)
		}
	}
}

func TestOriginFetch(t *testing.T) {
	root := t.TempDir()
	write(t, root, "app.yaml", "v1")
	ctx, limits := context.Background(), bundle.DefaultLimits()

	r, _ := resolve(t, root, "file:///app.yaml")
	first, err := r.Fetch(ctx, "", limits)
	if err != nil || first.Bundle == nil || first.Revision != bundle.Version(first.Bundle) {
		t.Fatalf("Fetch = %+v, %v; want the bundle under its content address", first, err)
	}
	// Unchanged since the known revision: no content.
	again, err := r.Fetch(ctx, first.Revision, limits)
	if err != nil || again.Bundle != nil || again.Revision != first.Revision {
		t.Errorf("Fetch(known) = %+v, %v; want the same revision and no bundle", again, err)
	}
	write(t, root, "app.yaml", "v2")
	changed, err := r.Fetch(ctx, first.Revision, limits)
	if err != nil || changed.Bundle == nil || changed.Revision == first.Revision {
		t.Errorf("Fetch after a change = %+v, %v; want a new revision with its bundle", changed, err)
	}

	missing, _ := resolve(t, root, "file:///nope.yaml")
	if _, err := missing.Fetch(ctx, "", limits); !errors.Is(err, provider.ErrNotFound) {
		t.Errorf("Fetch of a missing file = %v, want ErrNotFound", err)
	}
	reserved, _ := resolve(t, root, "file:///..data/app.yaml")
	if _, err := reserved.Fetch(ctx, "", limits); !errors.Is(err, provider.ErrInvalid) {
		t.Errorf("Fetch of a reserved path = %v, want ErrInvalid", err)
	}
	if _, err := r.Fetch(ctx, "", bundle.Limits{MaxBytes: 1, MaxFiles: 1}); !errors.Is(err, bundle.ErrTooLarge) {
		t.Errorf("Fetch over the limit = %v, want ErrTooLarge", err)
	}
}

// The rollout of a file is measured from when it was last written, under the
// ramp of the first rule its path matches.
func TestOriginRollout(t *testing.T) {
	root := t.TempDir()
	write(t, root, "staged/app.yaml", "v1")
	write(t, root, "plain.yaml", "v1")
	written := time.Unix(1_700_000_000, 0)
	for _, name := range []string{"staged/app.yaml", "plain.yaml"} {
		if err := os.Chtimes(filepath.Join(root, name), written, written); err != nil {
			t.Fatal(err)
		}
	}
	rule, err := rollout.ParseRule("/staged/*=10%:5m")
	if err != nil {
		t.Fatal(err)
	}
	o := Origin{Root: root, Rollouts: []rollout.Rule{rule}}
	fetch := func(raw string) provider.State {
		t.Helper()
		u, err := uri.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		r, err := o.Resolve(u)
		if err != nil {
			t.Fatal(err)
		}
		state, err := r.Fetch(context.Background(), "", bundle.DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		return state
	}
	if state := fetch("file:///staged/app.yaml"); !state.Since.Equal(written) || state.Ramp.At(0) != 1000 {
		t.Errorf("staged: since %v ramp starting at %d; want %v and 10%%", state.Since, state.Ramp.At(0), written)
	}
	if state := fetch("file:///plain.yaml"); state.Ramp.At(0) != rollout.Everyone {
		t.Errorf("plain: ramp starting at %d; want everyone", state.Ramp.At(0))
	}
}
