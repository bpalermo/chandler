// Package uri canonicalises the URIs pods use to name configuration:
// <provider-name>://<authority>/<path>?<query>. Two spellings of the same
// resource must canonicalise to the same string, because the canonical form is
// what subscriptions are coalesced and aliased on.
package uri

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
)

// ErrInvalid reports a URI that cannot name a configuration resource.
var ErrInvalid = errors.New("invalid config URI")

// URI is a canonical configuration URI.
type URI struct {
	// Provider is the scheme, which is the registered name of the provider
	// that serves the URI. Always lowercase.
	Provider string
	u        *url.URL
}

// Parse canonicalises raw: the scheme is lowercased, dot segments are removed
// from the path, query parameters are sorted by name and the fragment is
// dropped. A trailing slash is significant (it names a directory) and is kept.
//
// It rejects URIs without a scheme or without the "//" authority form, and
// URIs carrying userinfo, since credentials never belong in a pod spec.
// Everything else is left for the provider to interpret.
func Parse(raw string) (URI, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return URI{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	switch {
	case u.Scheme == "":
		return URI{}, fmt.Errorf("%w: %q has no provider scheme", ErrInvalid, raw)
	case u.Opaque != "" || !strings.HasPrefix(raw[len(u.Scheme):], "://"):
		return URI{}, fmt.Errorf("%w: %q must have the form <provider>://...", ErrInvalid, raw)
	case u.User != nil:
		return URI{}, fmt.Errorf("%w: %q must not contain userinfo", ErrInvalid, raw)
	}

	c := &url.URL{
		Scheme:   strings.ToLower(u.Scheme),
		Host:     u.Host,
		Path:     cleanPath(u.Path),
		RawQuery: u.Query().Encode(),
	}
	return URI{Provider: c.Scheme, u: c}, nil
}

// cleanPath removes dot segments as RFC 3986 section 5.2.4 does, keeping a
// trailing slash.
func cleanPath(p string) string {
	if p == "" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	dir := strings.HasSuffix(p, "/") || strings.HasSuffix(p, "/.") || strings.HasSuffix(p, "/..")
	c := path.Clean(p)
	if dir && c != "/" {
		c += "/"
	}
	return c
}

// String returns the canonical form.
func (u URI) String() string {
	if u.u == nil {
		return ""
	}
	return u.u.String()
}

// Authority returns the part between "//" and the path, e.g. a bucket name.
func (u URI) Authority() string { return u.u.Host }

// Path returns the cleaned, unescaped path, with its leading slash.
func (u URI) Path() string { return u.u.Path }

// Query returns the query parameters.
func (u URI) Query() url.Values { return u.u.Query() }

// IsDir reports whether the URI names a directory, i.e. ends in a slash.
func (u URI) IsDir() bool { return strings.HasSuffix(u.u.Path, "/") }
