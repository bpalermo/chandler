package source

import (
	"context"
	"errors"
	"testing"

	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/common/provider"
	"github.com/bpalermo/chandler/common/uri"
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
