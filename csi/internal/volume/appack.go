package volume

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
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

	// The states of an Outcome. The first two are the application's verdicts.
	StateLoaded   = "loaded"
	StateRejected = "rejected"
	// StateOverdue is not something an application reports: the driver says
	// it of a version the application was given and stayed silent about.
	StateOverdue = "overdue"

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

// manifest describes a generation: which version of each source it holds. A
// volume can combine sources from several providers, so each is listed with
// its own version. The caller holds v.mu.
func (v *Volume) manifest(g Generation) *chandlerv1.Manifest {
	sources := make([]*chandlerv1.ManifestSource, len(v.meta.Spec.Sources))
	for i, source := range v.meta.Spec.Sources {
		s := chandlerv1.ManifestSource_builder{
			Name: proto.String(source.Name),
			Uri:  proto.String(source.URI),
			Path: proto.String(source.Path),
		}
		if g.Versions[i] != "" {
			s.Version = proto.String(g.Versions[i])
		}
		sources[i] = s.Build()
	}
	return chandlerv1.Manifest_builder{Generation: proto.String(g.ID), Sources: sources}.Build()
}

// manifestFile is the content of the manifest file of a generation: the
// Manifest in protobuf's JSON mapping, as the API returns it.
func (v *Volume) manifestFile(g Generation) ([]byte, error) {
	return protojson.Marshal(v.manifest(g))
}

// OnOverdue sets what is called when the application has not reported on a
// version within the volume's timeout. It is called without the volume's lock
// held, and at most once per source and version. It also starts the clock
// for whatever is written and unreported now, as after a driver restart.
func (v *Volume) OnOverdue(notify func(Outcome)) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.overdue = notify
	for i := range v.meta.Spec.Sources {
		v.awaitReportLocked(i)
	}
}

// awaitReportLocked starts the clock for the application's report on what
// source i now holds.
func (v *Volume) awaitReportLocked(i int) {
	timeout := v.meta.Spec.AppAckTimeout
	version := v.meta.Applied[i]
	if timeout <= 0 || version == "" || v.overdue == nil {
		return
	}
	if v.meta.Loaded[i] == version || slices.Contains(v.meta.Rejected[i], version) {
		return // already reported
	}
	if v.awaiting[i] != nil {
		v.awaiting[i].Stop()
	}
	v.awaiting[i] = time.AfterFunc(timeout, func() {
		v.mu.Lock()
		still := !v.closed && v.meta.Applied[i] == version &&
			v.meta.Loaded[i] != version && !slices.Contains(v.meta.Rejected[i], version)
		if still {
			v.late[i] = version
		}
		notify := v.overdue
		v.mu.Unlock()
		if still && notify != nil {
			notify(Outcome{Source: i, Version: version, State: StateOverdue, Applied: version})
		}
	})
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

// RejectedVersions returns the versions of source i the application
// rejected.
func (v *Volume) RejectedVersions(i int) []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.meta.Rejected[i]...)
}

// OverdueVersion returns the version of source i the application is late to
// report on, "" for none.
func (v *Volume) OverdueVersion(i int) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	version := v.late[i]
	if version == "" || version != v.meta.Applied[i] ||
		v.meta.Loaded[i] == version || slices.Contains(v.meta.Rejected[i], version) {
		return ""
	}
	return version
}

// Config returns the manifest of the current generation with the state the
// application has reported for each source.
func (v *Volume) Config() *chandlerv1.Manifest {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.configLocked()
}

func (v *Volume) configLocked() *chandlerv1.Manifest {
	versions := append([]string(nil), v.meta.Applied...)
	m := v.manifest(Generation{ID: generationID(versions), Versions: versions})
	for i, source := range m.GetSources() {
		switch version := source.GetVersion(); {
		case version != "" && version == v.meta.Loaded[i]:
			source.SetState(chandlerv1.Manifest_STATE_LOADED)
		case slices.Contains(v.meta.Rejected[i], version):
			source.SetState(chandlerv1.Manifest_STATE_REJECTED)
		default:
			source.SetState(chandlerv1.Manifest_STATE_PENDING)
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
func (v *Volume) Report(r *chandlerv1.ReportRequest) ([]Outcome, *chandlerv1.Manifest, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil, nil, fmt.Errorf("%w: the volume is being removed", ErrStaleGeneration)
	}

	// Which sources, and what about each.
	type verdict struct{ state, message string }
	verdicts := make(map[int]verdict)
	// The states an application may report, as an Outcome names them.
	states := map[chandlerv1.Manifest_State]string{
		chandlerv1.Manifest_STATE_LOADED:   StateLoaded,
		chandlerv1.Manifest_STATE_REJECTED: StateRejected,
	}
	const want = "STATE_LOADED or STATE_REJECTED"
	if len(r.GetSources()) == 0 {
		state, ok := states[r.GetState()]
		if !ok {
			return nil, nil, fmt.Errorf("%w: state must be %s", ErrMalformedReport, want)
		}
		for i := range v.meta.Spec.Sources {
			verdicts[i] = verdict{state, r.GetMessage()}
		}
	}
	for _, sr := range r.GetSources() {
		i := slices.IndexFunc(v.meta.Spec.Sources, func(s Source) bool { return s.Name == sr.GetName() })
		state, ok := states[sr.GetState()]
		switch {
		case i < 0:
			return nil, nil, fmt.Errorf("%w: %q", ErrUnknownSource, sr.GetName())
		case !ok:
			return nil, nil, fmt.Errorf("%w: the state of %q must be %s", ErrMalformedReport, sr.GetName(), want)
		}
		verdicts[i] = verdict{state, sr.GetMessage()}
	}

	g := slices.IndexFunc(v.meta.Generations, func(g Generation) bool { return g.ID == r.GetGeneration() })
	if g < 0 {
		return nil, v.configLocked(), fmt.Errorf("%w: %q", ErrStaleGeneration, r.GetGeneration())
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
			// Not in the middle of a character: the message goes on in a
			// protobuf string, which has to be valid UTF-8.
			vd.message = strings.ToValidUTF8(vd.message[:maxMessageBytes], "")
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
	return outcomes, nil, nil
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
		_ = v.cache.Use(v.cacheKeys[i], loaded)
	}
}
