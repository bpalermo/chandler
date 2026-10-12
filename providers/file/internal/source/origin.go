package source

import (
	"context"
	"errors"
	"fmt"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/common/provider"
	"github.com/bpalermo/chandler/common/uri"
)

// stableReads is how many times a resource is re-read while two consecutive
// reads disagree, before the newest read is used anyway.
const stableReads = 3

// Origin is a directory as a provider's origin: <name>:///<path> names the
// file or, with a trailing slash, the directory at that path below Root.
type Origin struct {
	// Root is the directory URIs are resolved against.
	Root string
}

// Resolve implements provider.Origin. The provider understands nothing but a
// path: an authority or any query parameter is refused rather than ignored,
// so that decorating a URI cannot name the same file under another spelling.
func (o Origin) Resolve(u uri.URI) (provider.Resource, error) {
	switch {
	case u.Authority() != "":
		return nil, fmt.Errorf("%w: %s has an authority; write %s:///<path>", provider.ErrInvalid, u, u.Provider)
	case len(u.Query()) != 0:
		return nil, fmt.Errorf("%w: %s has query parameters, which this provider does not take", provider.ErrInvalid, u)
	case u.Path() == "":
		return nil, fmt.Errorf("%w: %s names no path", provider.ErrInvalid, u)
	}
	return resource{root: o.Root, path: u.Path()}, nil
}

type resource struct {
	root string
	path string
}

func (r resource) CacheKey() string { return r.path }

// Fetch implements provider.Resource. The revision is the bundle's content
// address: a directory has no cheaper change marker than its content.
func (r resource) Fetch(_ context.Context, known string, limits bundle.Limits) (provider.State, error) {
	b, err := r.readStable(Reader{Root: r.root, Limits: limits})
	switch {
	case errors.Is(err, ErrInvalid):
		return provider.State{}, fmt.Errorf("%w: %v", provider.ErrInvalid, err)
	case errors.Is(err, ErrNotFound):
		return provider.State{}, fmt.Errorf("%w: %v", provider.ErrNotFound, err)
	case err != nil:
		return provider.State{}, err
	}
	revision := bundle.Version(b)
	if revision == known {
		return provider.State{Revision: revision}, nil
	}
	return provider.State{Revision: revision, Bundle: b}, nil
}

// readStable reads the resource until two consecutive reads agree, so that a
// directory being rewritten is not emitted half old and half new. A directory
// swapped atomically (a ConfigMap mount) agrees on the second read.
func (r resource) readStable(reader Reader) (*chandlerv1.Bundle, error) {
	b, err := reader.Read(r.path)
	for i := 0; err == nil && i < stableReads; i++ {
		var again *chandlerv1.Bundle
		again, err = reader.Read(r.path)
		if err != nil || bundle.Version(again) == bundle.Version(b) {
			break
		}
		b = again
	}
	return b, err
}
