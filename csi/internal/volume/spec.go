// Package volume is one pod volume on the node: the sources it is made of,
// the version of each that is currently written, and the composition of all
// of them into one atomically replaced directory.
package volume

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/bpalermo/stevedore/common/bundle"
	"github.com/bpalermo/stevedore/common/uri"
)

const (
	// Volume attributes of the inline volume in the pod spec.
	sourcesKey       = "sources"
	failurePolicyKey = "failurePolicy"
	appAckKey        = "appAck"
	appAckTimeoutKey = "appAckTimeout"
	mediumKey        = "medium"

	// MediumMemory keeps a volume in memory, as it does for an emptyDir.
	MediumMemory = "Memory"

	// FailurePolicyFail fails the mount when a source has no answer, so the
	// pod waits for a version the server can supply.
	FailurePolicyFail = "Fail"
	// FailurePolicyUseCached mounts from the node cache when the server
	// cannot be reached or has no version, and fails only if the node holds
	// nothing for a source. It is the default: the last version the node was
	// given is better than no configuration at all.
	FailurePolicyUseCached = "UseCached"

	maxSources = 32
)

// ErrInvalidSpec reports volume attributes that cannot describe a volume.
var ErrInvalidSpec = errors.New("invalid volume attributes")

// Source is one configuration resource of a volume and where it is placed.
type Source struct {
	// Name identifies the source within the volume; defaults to Path.
	Name string `json:"name" yaml:"name"`
	// URI names the resource, <provider>://...
	URI string `json:"uri" yaml:"uri"`
	// Path is where the resource appears, relative to the volume root. For a
	// URI that names one object it is the file itself; for a directory URI
	// (trailing slash) it is the directory the bundle's files are put in.
	Path string `json:"path" yaml:"path"`
}

// IsDir reports whether the source is a directory bundle.
func (s Source) IsDir() bool { return strings.HasSuffix(s.URI, "/") }

// Spec is what a pod asked for in the volume's attributes.
type Spec struct {
	Sources       []Source `json:"sources"`
	FailurePolicy string   `json:"failurePolicy"`
	// AppAck is set when the application reports whether it could load each
	// version, over the socket in the volume.
	AppAck bool `json:"appAck,omitempty"`
	// AppAckTimeout is how long the application has to report on a version
	// before it is reported overdue; zero never does.
	AppAckTimeout time.Duration `json:"appAckTimeout,omitempty"`
	// Memory is set when the volume is kept in memory rather than on the
	// node's disk. Content a provider marks no_store is only written to
	// such a volume.
	Memory bool `json:"memory,omitempty"`
}

// ParseSpec reads the volume attributes of a pod's inline volume.
func ParseSpec(attributes map[string]string) (Spec, error) {
	spec := Spec{FailurePolicy: attributes[failurePolicyKey]}
	switch spec.FailurePolicy {
	case "":
		spec.FailurePolicy = FailurePolicyUseCached
	case FailurePolicyFail, FailurePolicyUseCached:
	default:
		return Spec{}, fmt.Errorf("%w: %s must be %s or %s, not %q", ErrInvalidSpec,
			failurePolicyKey, FailurePolicyFail, FailurePolicyUseCached, spec.FailurePolicy)
	}

	switch attributes[mediumKey] {
	case "":
	case MediumMemory:
		spec.Memory = true
	default:
		return Spec{}, fmt.Errorf("%w: %s must be %s or unset, not %q", ErrInvalidSpec, mediumKey, MediumMemory, attributes[mediumKey])
	}
	switch attributes[appAckKey] {
	case "", "false":
	case "true":
		spec.AppAck = true
	default:
		return Spec{}, fmt.Errorf("%w: %s must be \"true\" or \"false\", not %q", ErrInvalidSpec, appAckKey, attributes[appAckKey])
	}

	if text, ok := attributes[appAckTimeoutKey]; ok {
		timeout, err := time.ParseDuration(text)
		switch {
		case err != nil || timeout <= 0:
			return Spec{}, fmt.Errorf("%w: %s must be a positive duration such as 2m, not %q", ErrInvalidSpec, appAckTimeoutKey, text)
		case !spec.AppAck:
			return Spec{}, fmt.Errorf("%w: %s needs %s: \"true\"", ErrInvalidSpec, appAckTimeoutKey, appAckKey)
		}
		spec.AppAckTimeout = timeout
	}

	raw, ok := attributes[sourcesKey]
	if !ok || strings.TrimSpace(raw) == "" {
		return Spec{}, fmt.Errorf("%w: the %s attribute is required", ErrInvalidSpec, sourcesKey)
	}
	decoder := yaml.NewDecoder(strings.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&spec.Sources); err != nil {
		return Spec{}, fmt.Errorf("%w: %s is not a list of sources: %v", ErrInvalidSpec, sourcesKey, err)
	}
	switch n := len(spec.Sources); {
	case n == 0:
		return Spec{}, fmt.Errorf("%w: %s lists no source", ErrInvalidSpec, sourcesKey)
	case n > maxSources:
		return Spec{}, fmt.Errorf("%w: %d sources, at most %d are allowed", ErrInvalidSpec, n, maxSources)
	}

	names := make(map[string]bool)
	for i := range spec.Sources {
		s := &spec.Sources[i]
		if _, err := uri.Parse(s.URI); err != nil {
			return Spec{}, fmt.Errorf("%w: source %d: %v", ErrInvalidSpec, i, err)
		}
		if err := bundle.ValidatePath(s.Path); err != nil {
			return Spec{}, fmt.Errorf("%w: source %d: path %q: %v", ErrInvalidSpec, i, s.Path, err)
		}
		if s.Name == "" {
			s.Name = s.Path
		}
		if names[s.Name] {
			return Spec{}, fmt.Errorf("%w: two sources are named %q", ErrInvalidSpec, s.Name)
		}
		names[s.Name] = true
		// Sources own disjoint subtrees, so an update to one can never
		// collide with the files of another.
		for _, other := range spec.Sources[:i] {
			if within(s.Path, other.Path) || within(other.Path, s.Path) {
				return Spec{}, fmt.Errorf("%w: the paths %q and %q overlap", ErrInvalidSpec, other.Path, s.Path)
			}
		}
	}
	return spec, nil
}

// within reports whether path a is b or lies below it.
func within(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/")
}
