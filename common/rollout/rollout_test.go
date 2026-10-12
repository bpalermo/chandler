package rollout

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestPlaced(t *testing.T) {
	const version = "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	pods := make([]string, 20000)
	for i := range pods {
		pods[i] = fmt.Sprintf("pod-%d", i)
	}

	// Nobody at 0, everybody at 100%, and about the right share in between.
	in := func(version string, basisPoints uint32) map[string]bool {
		set := make(map[string]bool)
		for _, pod := range pods {
			if Placed(version, pod, basisPoints) {
				set[pod] = true
			}
		}
		return set
	}
	if n := len(in(version, 0)); n != 0 {
		t.Errorf("%d pods placed at 0%%", n)
	}
	if n := len(in(version, Everyone)); n != len(pods) {
		t.Errorf("%d of %d pods placed at 100%%", n, len(pods))
	}
	if n := len(in(version, Everyone+1)); n != len(pods) {
		t.Errorf("%d of %d pods placed above 100%%", n, len(pods))
	}
	ten := in(version, 1000)
	if n := len(ten); n < 1800 || n > 2200 {
		t.Errorf("%d of %d pods placed at 10%%, want about 2000", n, len(pods))
	}

	// The set only grows: a pod that has the version keeps it.
	fifty := in(version, 5000)
	for pod := range ten {
		if !fifty[pod] {
			t.Fatalf("%s was placed at 10%% but not at 50%%", pod)
		}
	}

	// Another version sends other pods first.
	other := in("sha256:"+version[8:]+"0", 1000)
	shared := 0
	for pod := range ten {
		if other[pod] {
			shared++
		}
	}
	if shared > len(ten)/2 {
		t.Errorf("%d of %d first pods are the same for another version", shared, len(ten))
	}

	// Stable across calls and processes: a fixed expectation.
	if !Placed(version, "pod-0", Everyone) || Placed(version, "pod-0", 0) {
		t.Error("the bounds do not hold")
	}
	a, b := Placed(version, "3c1f0a52-8f0b-4c53-9d1b-2a1f6f0a1b11", 5000), Placed(version, "3c1f0a52-8f0b-4c53-9d1b-2a1f6f0a1b11", 5000)
	if a != b {
		t.Error("the same question got two answers")
	}
}

func TestRamp(t *testing.T) {
	ramp, err := ParseRamp(" 1%:5m, 10:10m ,50.5%:1h ")
	if err != nil {
		t.Fatal(err)
	}
	for elapsed, want := range map[time.Duration]uint32{
		0:                            100,
		5*time.Minute - 1:            100,
		5 * time.Minute:              1000,
		15*time.Minute - 1:           1000,
		15 * time.Minute:             5050,
		75*time.Minute - time.Second: 5050,
		75 * time.Minute:             Everyone,
		24 * time.Hour:               Everyone,
		-time.Minute:                 100, // a clock that runs behind the origin's
	} {
		if got := ramp.At(elapsed); got != want {
			t.Errorf("At(%s) = %d, want %d", elapsed, got, want)
		}
	}
	if got := ramp.String(); got != "1%:5m0s,10%:10m0s,50.5%:1h0m0s" {
		t.Errorf("String() = %q", got)
	}
	if again, err := ParseRamp(ramp.String()); err != nil || again.String() != ramp.String() {
		t.Errorf("the written form does not parse back: %v", err)
	}

	// No ramp: everyone at once.
	none, err := ParseRamp("")
	if err != nil || none.At(0) != Everyone {
		t.Errorf("the empty ramp = %v, %v", none, err)
	}

	for _, bad := range []string{"10", "10%", "ten:5m", "0:5m", "100:5m", "-1:5m", "10:soon", "10:0s", "10:-5m", "50:5m,10:5m", "10:5m,10:5m", "10:5m,", "0.001:5m"} {
		if _, err := ParseRamp(bad); !errors.Is(err, ErrInvalidRamp) {
			t.Errorf("ParseRamp(%q) = %v, want ErrInvalidRamp", bad, err)
		}
	}
}

func TestRules(t *testing.T) {
	var rules []Rule
	for _, text := range []string{"/payments/*.yaml=1%:5m", "/shared/*=10%:1m", "50%:30s"} {
		rule, err := ParseRule(text)
		if err != nil {
			t.Fatal(err)
		}
		rules = append(rules, rule)
	}
	for name, want := range map[string]uint32{
		"/payments/app.yaml":   100,  // the first match wins
		"/shared/flags.json":   1000, // "*" stays within a segment...
		"/shared/nested/flags": 5000, // ...so this falls to the default
		"/other":               5000,
	} {
		if got := Match(rules, name).At(0); got != want {
			t.Errorf("Match(%q) starts at %d, want %d", name, got, want)
		}
	}
	if got := Match(rules[:2], "/other").At(0); got != Everyone {
		t.Errorf("with no matching rule the share starts at %d, want everyone", got)
	}
	for _, bad := range []string{"[=10:5m", "/a=", "/a=ten:5m"} {
		if rule, err := ParseRule(bad); err == nil && len(rule.Ramp.Steps) > 0 || bad == "[=10:5m" && err == nil {
			t.Errorf("ParseRule(%q) = %v, %v", bad, rule, err)
		}
	}
}
