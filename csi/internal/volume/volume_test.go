package volume

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
	"github.com/bpalermo/chandler/csi/internal/cache"
)

func TestParseSpec(t *testing.T) {
	spec, err := ParseSpec(map[string]string{"sources": `
- uri: file:///app.yaml
  path: app.yaml
- name: flags
  uri: s3://bucket/flags/
  path: etc/flags
`})
	if err != nil {
		t.Fatal(err)
	}
	if spec.FailurePolicy != FailurePolicyFail {
		t.Errorf("default failure policy = %q", spec.FailurePolicy)
	}
	if len(spec.Sources) != 2 {
		t.Fatalf("got %d sources", len(spec.Sources))
	}
	if s := spec.Sources[0]; s.Name != "app.yaml" || s.IsDir() {
		t.Errorf("source 0 = %+v, want it named after its path and not a directory", s)
	}
	if s := spec.Sources[1]; s.Name != "flags" || !s.IsDir() {
		t.Errorf("source 1 = %+v, want a directory named flags", s)
	}
}

func TestParseSpecRejects(t *testing.T) {
	for name, attributes := range map[string]map[string]string{
		"no sources":       {},
		"empty sources":    {"sources": "  "},
		"empty list":       {"sources": "[]"},
		"not a list":       {"sources": "uri: file:///a"},
		"unknown field":    {"sources": "- {uri: file:///a, path: a, mode: 420}"},
		"bad uri":          {"sources": "- {uri: /etc/passwd, path: a}"},
		"no path":          {"sources": "- {uri: file:///a}"},
		"absolute path":    {"sources": "- {uri: file:///a, path: /etc/a}"},
		"escaping path":    {"sources": "- {uri: file:///a, path: ../a}"},
		"reserved path":    {"sources": "- {uri: file:///a, path: ..data}"},
		"same path twice":  {"sources": "- {uri: file:///a, path: a}\n- {uri: file:///b, path: a}"},
		"nested paths":     {"sources": "- {uri: file:///a/, path: a}\n- {uri: file:///b, path: a/b}"},
		"same name twice":  {"sources": "- {name: x, uri: file:///a, path: a}\n- {name: x, uri: file:///b, path: b}"},
		"bad policy":       {"sources": "- {uri: file:///a, path: a}", "failurePolicy": "Ignore"},
		"too many sources": {"sources": strings.Repeat("- {uri: file:///a, path: a}\n", maxSources+1)},
	} {
		if _, err := ParseSpec(attributes); !errors.Is(err, ErrInvalidSpec) {
			t.Errorf("%s: got %v, want ErrInvalidSpec", name, err)
		}
	}
}

func file(path, content string) *chandlerv1.File {
	return chandlerv1.File_builder{Path: proto.String(path), Content: []byte(content)}.Build()
}

func bundleUpdate(files ...*chandlerv1.File) *chandlerv1.ConfigUpdate {
	b := chandlerv1.Bundle_builder{Files: files}.Build()
	return chandlerv1.ConfigUpdate_builder{Version: proto.String(bundle.Version(b)), Bundle: b}.Build()
}

func newTestCache(t *testing.T) *cache.Cache {
	t.Helper()
	c, err := cache.Open(t.TempDir(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newTestVolume(t *testing.T) (*Volume, string) {
	t.Helper()
	return newTestVolumeWith(t, newTestCache(t))
}

func newTestVolumeWith(t *testing.T, c Cache) (*Volume, string) {
	t.Helper()
	spec, err := ParseSpec(map[string]string{"sources": `
- uri: file:///app.yaml
  path: app.yaml
- uri: file:///flags/
  path: etc/flags
`})
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "vol")
	v, err := Create(dir, Meta{VolumeID: "vol", TargetPath: "/target", Spec: spec}, c)
	if err != nil {
		t.Fatal(err)
	}
	return v, dir
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func TestApplyComposesSources(t *testing.T) {
	v, dir := newTestVolume(t)
	data := filepath.Join(dir, "data")

	if err := v.Apply(0, bundleUpdate(file("whatever-the-origin-calls-it", "a: 1"))); err != nil {
		t.Fatal(err)
	}
	if err := v.Apply(1, bundleUpdate(file("x.json", "x"), file("sub/y.json", "y"))); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, filepath.Join(data, "app.yaml")); got != "a: 1" {
		t.Errorf("app.yaml = %q", got)
	}
	if got := mustRead(t, filepath.Join(data, "etc/flags/sub/y.json")); got != "y" {
		t.Errorf("etc/flags/sub/y.json = %q", got)
	}

	// A directory update replaces the directory, and only the directory.
	if err := v.Apply(1, bundleUpdate(file("z.json", "z"))); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(data, "etc/flags/x.json")); !os.IsNotExist(err) {
		t.Errorf("a file removed from the bundle is still there: %v", err)
	}
	if got := mustRead(t, filepath.Join(data, "etc/flags/z.json")); got != "z" {
		t.Errorf("etc/flags/z.json = %q", got)
	}
	if got := mustRead(t, filepath.Join(data, "app.yaml")); got != "a: 1" {
		t.Errorf("app.yaml changed with the other source: %q", got)
	}
}

// Every refusal must leave the volume as it was and carry the code the server
// reacts to.
func TestApplyRefusals(t *testing.T) {
	v, dir := newTestVolume(t)
	good := bundleUpdate(file("app.yaml", "good"))
	if err := v.Apply(0, good); err != nil {
		t.Fatal(err)
	}

	tampered := bundleUpdate(file("app.yaml", "tampered"))
	tampered.SetVersion(good.GetVersion())
	unknown := chandlerv1.ConfigUpdate_builder{
		Version:   proto.String("sha256:" + strings.Repeat("0", 64)),
		UseCached: &chandlerv1.UseCached{},
	}.Build()

	for name, tc := range map[string]struct {
		source int
		update *chandlerv1.ConfigUpdate
		want   codes.Code
	}{
		"digest mismatch":                 {0, tampered, codes.DataLoss},
		"UseCached of a version not held": {0, unknown, codes.DataLoss},
		"unsafe path":                     {1, bundleUpdate(file("../escape", "x")), codes.InvalidArgument},
		"two files for one object":        {0, bundleUpdate(file("a", "1"), file("b", "2")), codes.InvalidArgument},
		"no file for one object":          {0, bundleUpdate(), codes.InvalidArgument},
	} {
		err := v.Apply(tc.source, tc.update)
		if grpcstatus.Code(err) != tc.want {
			t.Errorf("%s: got %v, want %s", name, err, tc.want)
		}
		if got := mustRead(t, filepath.Join(dir, "data", "app.yaml")); got != "good" {
			t.Fatalf("%s: the last known good was replaced by %q", name, got)
		}
		if got := v.Applied(0); got != good.GetVersion() {
			t.Fatalf("%s: applied version became %q", name, got)
		}
	}

	// UseCached of the version that is written needs no bytes.
	cached := chandlerv1.ConfigUpdate_builder{
		Version:   proto.String(good.GetVersion()),
		UseCached: &chandlerv1.UseCached{},
	}.Build()
	if err := v.Apply(0, cached); err != nil {
		t.Errorf("UseCached of the written version: %v", err)
	}
}

func TestWaitReady(t *testing.T) {
	v, _ := newTestVolume(t)
	short := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 50*time.Millisecond)
	}

	ctx, cancel := short()
	err := v.WaitReady(ctx)
	cancel()
	if grpcstatus.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "file:///app.yaml") {
		t.Errorf("with no answer: got %v, want Unavailable naming the first source", err)
	}

	if err := v.Apply(0, bundleUpdate(file("app.yaml", "a"))); err != nil {
		t.Fatal(err)
	}
	v.Fail(1, &status.Status{Code: int32(codes.NotFound), Message: "no such directory"})
	ctx, cancel = short()
	err = v.WaitReady(ctx)
	cancel()
	if grpcstatus.Code(err) != codes.NotFound || !strings.Contains(err.Error(), "no such directory") {
		t.Errorf("with a failed source: got %v, want its NotFound", err)
	}

	// An answer that arrives while waiting ends the wait.
	go func() {
		time.Sleep(20 * time.Millisecond)
		v.Apply(1, bundleUpdate())
	}()
	v.Fail(1, nil)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := v.WaitReady(ctx); err != nil {
		t.Errorf("once every source is written: %v", err)
	}
}

func TestLoadRestoresWhatWasWritten(t *testing.T) {
	v, dir := newTestVolume(t)
	app := bundleUpdate(file("app.yaml", "a: 1"))
	if err := v.Apply(0, app); err != nil {
		t.Fatal(err)
	}
	if err := v.Apply(1, bundleUpdate(file("x.json", "x"))); err != nil {
		t.Fatal(err)
	}

	loaded, err := Load(dir, newTestCache(t))
	if err != nil {
		t.Fatal(err)
	}
	if meta := loaded.Meta(); meta.TargetPath != "/target" || meta.Applied[0] != app.GetVersion() {
		t.Errorf("loaded meta = %+v", meta)
	}
	// The other source's files come from disk, so this update keeps them.
	if err := loaded.Apply(0, bundleUpdate(file("app.yaml", "a: 2"))); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, filepath.Join(dir, "data", "etc/flags/x.json")); got != "x" {
		t.Errorf("etc/flags/x.json = %q", got)
	}
	if got := mustRead(t, filepath.Join(dir, "data", "app.yaml")); got != "a: 2" {
		t.Errorf("app.yaml = %q", got)
	}

	if _, err := Load(t.TempDir(), newTestCache(t)); err == nil {
		t.Error("Load of a directory with no volume succeeded")
	}
}

func useCached(version string) *chandlerv1.ConfigUpdate {
	return chandlerv1.ConfigUpdate_builder{Version: proto.String(version), UseCached: &chandlerv1.UseCached{}}.Build()
}

// What one volume wrote, another on the same node can apply without bytes:
// that is what makes a rollback, and a mount during an outage, possible.
func TestNodeCache(t *testing.T) {
	c := newTestCache(t)
	first, _ := newTestVolumeWith(t, c)
	v1 := bundleUpdate(file("app.yaml", "v1"))
	v2 := bundleUpdate(file("app.yaml", "v2"))
	for _, u := range []*chandlerv1.ConfigUpdate{v1, v2} {
		if err := first.Apply(0, u); err != nil {
			t.Fatal(err)
		}
	}
	if got := first.CachedVersions(0); len(got) != 2 || got[0] != v2.GetVersion() || got[1] != v1.GetVersion() {
		t.Fatalf("cached versions = %v, want v2 then v1", got)
	}

	// A rollback to the previous version is a local swap.
	if err := first.Apply(0, useCached(v1.GetVersion())); err != nil {
		t.Fatalf("UseCached of the previous version: %v", err)
	}
	if got := first.Applied(0); got != v1.GetVersion() {
		t.Errorf("applied = %q, want v1", got)
	}

	// Another volume starts from the cache alone.
	second, dir := newTestVolumeWith(t, c)
	if err := second.ApplyCached(); grpcstatus.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "file:///flags/") {
		t.Errorf("with one source uncached: got %v, want Unavailable naming it", err)
	}
	if err := first.Apply(1, bundleUpdate(file("x.json", "x"))); err != nil {
		t.Fatal(err)
	}
	if err := second.ApplyCached(); err != nil {
		t.Fatalf("ApplyCached: %v", err)
	}
	// The newest version held for the URI: v1, which the rollback made so.
	if got := mustRead(t, filepath.Join(dir, "data", "app.yaml")); got != "v1" {
		t.Errorf("app.yaml from the cache = %q, want v1", got)
	}
	if got := mustRead(t, filepath.Join(dir, "data", "etc/flags/x.json")); got != "x" {
		t.Errorf("etc/flags/x.json from the cache = %q", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := second.WaitReady(ctx); err != nil {
		t.Errorf("a volume written from the cache is ready: %v", err)
	}
}

func TestNoStoreBypassesTheCache(t *testing.T) {
	u := bundleUpdate(file("app.yaml", "secret"))
	u.SetCacheControl(chandlerv1.CacheControl_builder{NoStore: proto.Bool(true)}.Build())

	// A volume on the node's disk does not take it at all.
	disk, dir := newTestVolume(t)
	if err := disk.Apply(0, u); grpcstatus.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "medium: Memory") {
		t.Errorf("no_store on a disk volume: got %v, want FailedPrecondition naming the attribute", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "data", "app.yaml")); !os.IsNotExist(err) {
		t.Errorf("no_store content was written to a disk volume: %v", err)
	}

	// One kept in memory does, and the node cache still never sees it.
	spec, err := ParseSpec(map[string]string{"sources": "- uri: file:///app.yaml\n  path: app.yaml\n", "medium": "Memory"})
	if err != nil {
		t.Fatal(err)
	}
	dir = filepath.Join(t.TempDir(), "vol")
	v, err := Create(dir, Meta{VolumeID: "vol", Spec: spec}, newTestCache(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Apply(0, u); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, filepath.Join(dir, "data", "app.yaml")); got != "secret" {
		t.Errorf("app.yaml = %q", got)
	}
	if got := v.CachedVersions(0); len(got) != 0 {
		t.Errorf("a no_store bundle was cached: %v", got)
	}
	if _, err := ParseSpec(map[string]string{"sources": "- {uri: file:///a, path: a}", "medium": "Disk"}); !errors.Is(err, ErrInvalidSpec) {
		t.Errorf("an unknown medium: got %v, want ErrInvalidSpec", err)
	}
}

// An application that says nothing about a version in time is reported
// overdue, once; one that reports is not; and nothing is reverted for it.
func TestOverdueReports(t *testing.T) {
	spec, err := ParseSpec(map[string]string{
		"sources":       "- uri: file:///app.yaml\n  path: app.yaml\n",
		"appAck":        "true",
		"appAckTimeout": "80ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "vol")
	v, err := Create(dir, Meta{VolumeID: "vol", Spec: spec}, newTestCache(t))
	if err != nil {
		t.Fatal(err)
	}
	overdue := make(chan Outcome, 8)
	v.OnOverdue(func(o Outcome) { overdue <- o })

	first := bundleUpdate(file("app.yaml", "v1"))
	if err := v.Apply(0, first); err != nil {
		t.Fatal(err)
	}
	select {
	case o := <-overdue:
		if o.State != StateOverdue || o.Version != first.GetVersion() || o.Applied != first.GetVersion() || o.Source != 0 {
			t.Errorf("outcome = %+v, want v1 overdue and still applied", o)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a silent application was never reported overdue")
	}
	if got := mustRead(t, filepath.Join(dir, "data", "app.yaml")); got != "v1" {
		t.Errorf("the file reads %q: nothing may be reverted for a late report", got)
	}
	select {
	case o := <-overdue:
		t.Fatalf("reported overdue twice: %+v", o)
	case <-time.After(250 * time.Millisecond):
	}

	// A version the application reports on in time is not overdue.
	second := bundleUpdate(file("app.yaml", "v2"))
	if err := v.Apply(0, second); err != nil {
		t.Fatal(err)
	}
	if _, _, err := v.Report(Report{Generation: v.Config().Generation, State: StateLoaded}); err != nil {
		t.Fatal(err)
	}
	select {
	case o := <-overdue:
		t.Fatalf("a version the application loaded was reported overdue: %+v", o)
	case <-time.After(250 * time.Millisecond):
	}

	// After a driver restart the clock starts again for what is unreported.
	third := bundleUpdate(file("app.yaml", "v3"))
	if err := v.Apply(0, third); err != nil {
		t.Fatal(err)
	}
	v.Close()
	loaded, err := Load(dir, newTestCache(t))
	if err != nil {
		t.Fatal(err)
	}
	again := make(chan Outcome, 8)
	loaded.OnOverdue(func(o Outcome) { again <- o })
	select {
	case o := <-again:
		if o.Version != third.GetVersion() {
			t.Errorf("after a restart %s was reported overdue, want v3", o.Version)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("after a restart the unreported version was never reported overdue")
	}

	for name, attributes := range map[string]map[string]string{
		"a timeout without appAck":      {"sources": "- {uri: file:///a, path: a}", "appAckTimeout": "1m"},
		"a timeout that is no duration": {"sources": "- {uri: file:///a, path: a}", "appAck": "true", "appAckTimeout": "soon"},
		"a negative timeout":            {"sources": "- {uri: file:///a, path: a}", "appAck": "true", "appAckTimeout": "-1m"},
		"appAck that is not a boolean":  {"sources": "- {uri: file:///a, path: a}", "appAck": "yes"},
	} {
		if _, err := ParseSpec(attributes); !errors.Is(err, ErrInvalidSpec) {
			t.Errorf("%s: got %v, want ErrInvalidSpec", name, err)
		}
	}
}
