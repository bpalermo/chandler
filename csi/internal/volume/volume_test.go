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

func newTestVolume(t *testing.T) (*Volume, string) {
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
	v, err := Create(dir, Meta{VolumeID: "vol", TargetPath: "/target", Spec: spec})
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
		"digest mismatch":            {0, tampered, codes.DataLoss},
		"UseCached of a version not held": {0, unknown, codes.DataLoss},
		"unsafe path":                {1, bundleUpdate(file("../escape", "x")), codes.InvalidArgument},
		"two files for one object":   {0, bundleUpdate(file("a", "1"), file("b", "2")), codes.InvalidArgument},
		"no file for one object":     {0, bundleUpdate(), codes.InvalidArgument},
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

	loaded, err := Load(dir)
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

	if _, err := Load(t.TempDir()); err == nil {
		t.Error("Load of a directory with no volume succeeded")
	}
}
