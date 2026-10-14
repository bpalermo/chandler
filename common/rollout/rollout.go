// Package rollout holds the two rules of a staged rollout that every
// component must agree on: which pods are in a share of the fleet, and how a
// share grows over time.
//
// A rollout exists so that a new version does not reach every pod at the same
// moment. Nothing in Stevedore can judge whether a version is good, since the
// content is opaque: the ramp buys time for people and their monitoring to
// notice a bad one before it is everywhere.
package rollout

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"
)

// Everyone is the share that is the whole fleet, in basis points.
const Everyone = 10000

// Placed reports whether a pod is in the share of subscribers that gets a
// version.
//
// It is a pure function of its arguments, so every server replica gives the
// same answer for the same pod with no state and no coordination. The set
// only grows with the share, so a pod never flips back and forth, and the
// version salts the hash, so a different set of pods goes first each time.
func Placed(version, podUID string, basisPoints uint32) bool {
	if basisPoints >= Everyone {
		return true
	}
	h := sha256.New()
	h.Write([]byte(version))
	h.Write([]byte{0})
	h.Write([]byte(podUID))
	return binary.BigEndian.Uint64(h.Sum(nil)[:8])%Everyone < uint64(basisPoints)
}

// Step is one stage of a ramp: a share of the fleet, held for a time.
type Step struct {
	BasisPoints uint32
	Hold        time.Duration
}

// Ramp is how a new version is released over time: each step's share in turn,
// each held for its time, and then everyone. The zero Ramp releases to
// everyone at once.
type Ramp struct {
	Steps []Step
}

// ErrInvalidRamp reports a ramp that cannot be parsed or makes no sense.
var ErrInvalidRamp = errors.New("invalid ramp")

// ParseRamp reads a ramp written as "1%:5m,10%:10m,50%:10m": a
// comma-separated list of <percent>:<hold>. The percent sign is optional, and
// the percentage may have a fraction. The empty string is the zero Ramp.
func ParseRamp(s string) (Ramp, error) {
	var ramp Ramp
	if strings.TrimSpace(s) == "" {
		return ramp, nil
	}
	last := uint32(0)
	for _, part := range strings.Split(s, ",") {
		share, hold, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return Ramp{}, fmt.Errorf("%w: %q is not <percent>:<hold>", ErrInvalidRamp, part)
		}
		percent, err := strconv.ParseFloat(strings.TrimSuffix(share, "%"), 64)
		if err != nil || percent <= 0 || percent >= 100 {
			return Ramp{}, fmt.Errorf("%w: %q is not a percentage between 0 and 100", ErrInvalidRamp, share)
		}
		duration, err := time.ParseDuration(hold)
		if err != nil || duration <= 0 {
			return Ramp{}, fmt.Errorf("%w: %q is not a positive duration", ErrInvalidRamp, hold)
		}
		basisPoints := uint32(percent*100 + 0.5)
		if basisPoints == 0 || basisPoints <= last {
			return Ramp{}, fmt.Errorf("%w: the shares must grow, and by at least 0.01%%", ErrInvalidRamp)
		}
		last = basisPoints
		ramp.Steps = append(ramp.Steps, Step{BasisPoints: basisPoints, Hold: duration})
	}
	return ramp, nil
}

// At returns the share of the fleet that gets a version that appeared elapsed
// ago, in basis points.
func (r Ramp) At(elapsed time.Duration) uint32 {
	for _, step := range r.Steps {
		if elapsed < step.Hold {
			return step.BasisPoints
		}
		elapsed -= step.Hold
	}
	return Everyone
}

// String writes the ramp the way ParseRamp reads it.
func (r Ramp) String() string {
	parts := make([]string, len(r.Steps))
	for i, step := range r.Steps {
		parts[i] = fmt.Sprintf("%s%%:%s", strconv.FormatFloat(float64(step.BasisPoints)/100, 'f', -1, 64), step.Hold)
	}
	return strings.Join(parts, ",")
}

// Rule gives a ramp to the resources whose name matches a pattern.
type Rule struct {
	// Pattern is matched against a resource's name as path.Match does: "*"
	// matches within one path segment. Empty matches every resource.
	Pattern string
	Ramp    Ramp
}

// ParseRule reads "<pattern>=<ramp>", or "<ramp>" alone for every resource.
func ParseRule(s string) (Rule, error) {
	pattern, steps, ok := strings.Cut(s, "=")
	if !ok {
		pattern, steps = "", s
	}
	if _, err := path.Match(pattern, ""); err != nil {
		return Rule{}, fmt.Errorf("%w: pattern %q: %v", ErrInvalidRamp, pattern, err)
	}
	ramp, err := ParseRamp(steps)
	if err != nil {
		return Rule{}, err
	}
	return Rule{Pattern: pattern, Ramp: ramp}, nil
}

// Match returns the ramp of the first rule that matches name. With no match
// the zero Ramp is returned: the resource is released to everyone at once.
func Match(rules []Rule, name string) Ramp {
	for _, rule := range rules {
		if rule.Pattern == "" {
			return rule.Ramp
		}
		if ok, _ := path.Match(rule.Pattern, name); ok {
			return rule.Ramp
		}
	}
	return Ramp{}
}
