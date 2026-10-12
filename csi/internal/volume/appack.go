package volume

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Application acknowledgement: the application says whether it could load
// what the driver wrote. The driver cannot tell a good config from a bad one;
// this is the one stage that can, and the judgement is the application's.

const (
	// ManifestName is the file in the volume that describes the current
	// generation. It is swapped in together with the files it describes.
	ManifestName = "..chandler.json"
	// SocketName is the Unix socket in the volume the application reports on.
	SocketName = "..chandler.sock"

	// The states of a source, as the application sees and reports them.
	StatePending  = "pending"
	StateLoaded   = "loaded"
	StateRejected = "rejected"

	maxMessageBytes = 1024
)

var (
	// ErrMalformedReport reports a report that says nothing usable.
	ErrMalformedReport = errors.New("malformed report")
	// ErrUnknownSource reports a report naming a source the volume lacks.
	ErrUnknownSource = errors.New("unknown source")
	// ErrStaleGeneration reports a report about a generation that is
	// neither the current one nor the one before it.
	ErrStaleGeneration = errors.New("stale generation")
)

// Manifest describes one generation of a volume: which version of each source
// it holds. A volume can combine sources from several providers, so each is
// listed with its own version.
type Manifest struct {
	// Generation identifies this exact combination of source versions.
	Generation string           `json:"generation"`
	Sources    []ManifestSource `json:"sources"`
}

// ManifestSource is one source in a Manifest.
type ManifestSource struct {
	Name    string `json:"name"`
	URI     string `json:"uri"`
	Path    string `json:"path"`
	Version string `json:"version"`
	// State is what the application has said about this version. It is not
	// in the manifest file, which is written before the application reads
	// it; the driver adds it when asked for the configuration.
	State string `json:"state,omitempty"`
}

// Report is what an application says about a generation it read.
type Report struct {
	Generation string `json:"generation"`
	// State applies to every source when Sources is empty.
	State   string         `json:"state,omitempty"`
	Message string         `json:"message,omitempty"`
	Sources []SourceReport `json:"sources,omitempty"`
}

// SourceReport is a Report about one source.
type SourceReport struct {
	Name    string `json:"name"`
	State   string `json:"state"`
	Message string `json:"message,omitempty"`
}

// Outcome is one source's verdict, to be forwarded to the config server.
type Outcome struct {
	// Source is the index of the source in the volume.
	Source  int
	Version string
	// State is StateLoaded or StateRejected.
	State   string
	Message string
	// Applied is the version in the volume once the driver has reacted.
	Applied string
}

// generationID identifies a combination of source versions.
func generationID(versions []string) string {
	sum := sha256.Sum256([]byte(strings.Join(versions, "\n")))
	return "g-" + hex.EncodeToString(sum[:8])
}

// manifest describes a generation. The caller holds v.mu.
func (v *Volume) manifest(g Generation) Manifest {
	m := Manifest{Generation: g.ID, Sources: make([]ManifestSource, len(v.meta.Spec.Sources))}
	for i, source := range v.meta.Spec.Sources {
		m.Sources[i] = ManifestSource{Name: source.Name, URI: source.URI, Path: source.Path, Version: g.Versions[i]}
	}
	return m
}

// AppAck reports whether the volume takes application reports.
func (v *Volume) AppAck() bool { return v.meta.Spec.AppAck }

// Loaded returns the last version of source i the application reported
// loaded, "" for none.
func (v *Volume) Loaded(i int) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.meta.Loaded[i]
}

// Config returns the manifest of the current generation with the state the
// application has reported for each source.
func (v *Volume) Config() Manifest {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.configLocked()
}

func (v *Volume) configLocked() Manifest {
	versions := append([]string(nil), v.meta.Applied...)
	m := v.manifest(Generation{ID: generationID(versions), Versions: versions})
	for i := range m.Sources {
		switch version := m.Sources[i].Version; {
		case version != "" && version == v.meta.Loaded[i]:
			m.Sources[i].State = StateLoaded
		case slices.Contains(v.meta.Rejected[i], version):
			m.Sources[i].State = StateRejected
		default:
			m.Sources[i].State = StatePending
		}
	}
	return m
}

// Report records what the application said about a generation, and reacts.
//
// A loaded version becomes the source's last known good. A rejected version
// is reverted at once, without waiting for the server: the source goes back
// to its last loaded version in a new generation, so that an application that
// restarts does not read the files it refused. If the source never had a
// loaded version there is nothing to go back to and the files stay.
//
// The verdicts are recorded against the versions the named generation held,
// so a report stays meaningful for the sources that did not change when a
// newer generation has arrived meanwhile.
//
// The error wraps ErrMalformedReport, ErrUnknownSource or ErrStaleGeneration;
// with the last, the current configuration is returned to reload from.
func (v *Volume) Report(r Report) ([]Outcome, Manifest, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil, Manifest{}, fmt.Errorf("%w: the volume is being removed", ErrStaleGeneration)
	}

	// Which sources, and what about each.
	type verdict struct{ state, message string }
	verdicts := make(map[int]verdict)
	valid := func(state string) bool { return state == StateLoaded || state == StateRejected }
	if len(r.Sources) == 0 {
		if !valid(r.State) {
			return nil, Manifest{}, fmt.Errorf("%w: state must be %q or %q", ErrMalformedReport, StateLoaded, StateRejected)
		}
		for i := range v.meta.Spec.Sources {
			verdicts[i] = verdict{r.State, r.Message}
		}
	}
	for _, sr := range r.Sources {
		i := slices.IndexFunc(v.meta.Spec.Sources, func(s Source) bool { return s.Name == sr.Name })
		switch {
		case i < 0:
			return nil, Manifest{}, fmt.Errorf("%w: %q", ErrUnknownSource, sr.Name)
		case !valid(sr.State):
			return nil, Manifest{}, fmt.Errorf("%w: the state of %q must be %q or %q", ErrMalformedReport, sr.Name, StateLoaded, StateRejected)
		}
		verdicts[i] = verdict{sr.State, sr.Message}
	}

	g := slices.IndexFunc(v.meta.Generations, func(g Generation) bool { return g.ID == r.Generation })
	if g < 0 {
		return nil, v.configLocked(), fmt.Errorf("%w: %q", ErrStaleGeneration, r.Generation)
	}
	versions := v.meta.Generations[g].Versions

	var outcomes []Outcome
	for i := range v.meta.Spec.Sources {
		vd, ok := verdicts[i]
		version := versions[i]
		if !ok || version == "" {
			continue
		}
		if len(vd.message) > maxMessageBytes {
			vd.message = vd.message[:maxMessageBytes]
		}
		switch vd.state {
		case StateLoaded:
			v.meta.Loaded[i] = version
			// The application changed its mind, or two components disagree:
			// the newest word counts.
			v.meta.Rejected[i] = slices.DeleteFunc(v.meta.Rejected[i], func(r string) bool { return r == version })
		case StateRejected:
			if !slices.Contains(v.meta.Rejected[i], version) {
				v.meta.Rejected[i] = append(v.meta.Rejected[i], version)
			}
			if v.meta.Loaded[i] == version {
				v.meta.Loaded[i] = ""
			}
			v.revertLocked(i, version)
		}
		outcomes = append(outcomes, Outcome{Source: i, Version: version, State: vd.state, Message: vd.message, Applied: v.meta.Applied[i]})
	}
	_ = v.saveLocked()
	return outcomes, Manifest{}, nil
}

// revertLocked puts source i back on its last loaded version, if the rejected
// version is what is written and there is a loaded one to go back to.
func (v *Volume) revertLocked(i int, rejected string) {
	loaded := v.meta.Loaded[i]
	if v.meta.Applied[i] != rejected || loaded == "" || loaded == rejected {
		return
	}
	b, err := v.cache.Get(loaded)
	if err != nil {
		return // no longer on the node: the files stay until the server sends it
	}
	contribution, err := contribute(v.meta.Spec.Sources[i], b)
	if err != nil {
		return
	}
	if v.writeLocked(i, loaded, contribution) == nil {
		_ = v.cache.Use(v.uris[i], loaded)
	}
}
