package atomic

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/bpalermo/stevedore/common/bundle"
)

func files(kv ...string) map[string]File {
	m := make(map[string]File)
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = File{Content: []byte(kv[i+1])}
	}
	return m
}

func read(t *testing.T, dir, path string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// visible lists the volume root as a pod sees it, without bookkeeping entries.
func visible(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "..") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}

func generations(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() && isGeneration(e.Name()) {
			names = append(names, e.Name())
		}
	}
	return names
}

func TestWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir)

	if cur, err := w.Current(); err != nil || cur != "" {
		t.Fatalf("Current() on empty volume = %q, %v", cur, err)
	}

	in := files("app.yaml", "a: 1", "flags/x.json", "{}", "flags/deep/y", "y")
	in["secret"] = File{Mode: 0o400, Content: []byte("s")}
	if err := w.Write(in); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if got := read(t, dir, "app.yaml"); got != "a: 1" {
		t.Errorf("app.yaml = %q", got)
	}
	if got := read(t, dir, "flags/deep/y"); got != "y" {
		t.Errorf("flags/deep/y = %q", got)
	}
	if got, want := visible(t, dir), []string{"app.yaml", "flags", "secret"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("visible entries = %v, want %v", got, want)
	}

	// Visible entries are symlinks through ..data, so one rename moves them all.
	if target, err := os.Readlink(filepath.Join(dir, "flags")); err != nil || target != filepath.Join("..data", "flags") {
		t.Errorf("flags -> %q, %v", target, err)
	}

	for path, want := range map[string]fs.FileMode{"app.yaml": bundle.DefaultMode, "secret": 0o400} {
		info, err := os.Stat(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != want {
			t.Errorf("%s mode = %v, want %v", path, info.Mode().Perm(), want)
		}
	}

	cur, err := w.Current()
	if err != nil || !isGeneration(cur) {
		t.Errorf("Current() = %q, %v", cur, err)
	}
}

func TestUpdateReplacesWholeVolume(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir)

	if err := w.Write(files("app.yaml", "v1", "old.yaml", "gone soon", "flags/a", "1")); err != nil {
		t.Fatal(err)
	}
	first, _ := w.Current()

	if err := w.Write(files("app.yaml", "v2", "flags/b", "2", "new.yaml", "n")); err != nil {
		t.Fatal(err)
	}
	second, _ := w.Current()

	if first == second {
		t.Fatal("generation did not change")
	}
	if got := read(t, dir, "app.yaml"); got != "v2" {
		t.Errorf("app.yaml = %q, want v2", got)
	}
	if got, want := visible(t, dir), "app.yaml,flags,new.yaml"; strings.Join(got, ",") != want {
		t.Errorf("visible entries = %v, want %s", got, want)
	}
	if _, err := os.Stat(filepath.Join(dir, "flags/a")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("flags/a should be gone, got %v", err)
	}

	// The previous generation stays on disk, intact, as the last known good.
	if got := read(t, dir, filepath.Join(first, "old.yaml")); got != "gone soon" {
		t.Errorf("previous generation old.yaml = %q", got)
	}

	if err := w.Write(files("app.yaml", "v3")); err != nil {
		t.Fatal(err)
	}
	third, _ := w.Current()
	got := generations(t, dir)
	sort.Strings(got)
	want := []string{second, third}
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("generations on disk = %v, want current and previous %v", got, want)
	}
}

func TestFailedWriteLeavesVolumeUntouched(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir)
	if err := w.Write(files("app.yaml", "good")); err != nil {
		t.Fatal(err)
	}
	before, _ := w.Current()

	bad := map[string]map[string]File{
		"unsafe path":            files("../escape", "x"),
		"reserved name":          files("..data", "x"),
		"file used as directory": files("a", "x", "a/b", "y"),
	}
	for name, in := range bad {
		if err := w.Write(in); err == nil {
			t.Errorf("%s: Write succeeded", name)
		}
		if after, _ := w.Current(); after != before {
			t.Errorf("%s: generation changed from %q to %q", name, before, after)
		}
		if got := read(t, dir, "app.yaml"); got != "good" {
			t.Errorf("%s: app.yaml = %q", name, got)
		}
		if got := generations(t, dir); len(got) != 1 {
			t.Errorf("%s: left generations behind: %v", name, got)
		}
	}
	if err := w.Write(files("../escape", "x")); !errors.Is(err, bundle.ErrInvalidPath) {
		t.Errorf("unsafe path error = %v, want ErrInvalidPath", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "escape")); !errors.Is(err, fs.ErrNotExist) {
		t.Error("a file was written outside the volume")
	}
}

func TestForeignReservedEntriesSurvive(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir)

	// Entries the driver places next to the content, such as the
	// acknowledgement socket, are not the writer's to remove.
	if err := os.WriteFile(filepath.Join(dir, "..stevedore.sock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "..not_a_generation"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"1", "2", "3"} {
		if err := w.Write(files("app.yaml", v)); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"..stevedore.sock", "..not_a_generation"} {
		if _, err := os.Lstat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s was removed: %v", name, err)
		}
	}
}

func TestEmptyWriteClearsVolume(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir)
	if err := w.Write(files("app.yaml", "1")); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(nil); err != nil {
		t.Fatal(err)
	}
	if got := visible(t, dir); len(got) != 0 {
		t.Errorf("visible entries = %v, want none", got)
	}
}
