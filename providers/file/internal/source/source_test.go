package source

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bpalermo/chandler/common/bundle"
)

func write(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o640); err != nil {
		t.Fatal(err)
	}
}

func reader(root string) Reader {
	return Reader{Root: root, Limits: bundle.DefaultLimits()}
}

func TestReadFile(t *testing.T) {
	root := t.TempDir()
	write(t, root, "team/app.yaml", "a: 1\n")

	b, err := reader(root).Read("/team/app.yaml")
	if err != nil {
		t.Fatal(err)
	}
	files := b.GetFiles()
	if len(files) != 1 {
		t.Fatalf("got %d files, want 1", len(files))
	}
	if got := files[0].GetPath(); got != "app.yaml" {
		t.Errorf("path = %q, want the base name", got)
	}
	if got := string(files[0].GetContent()); got != "a: 1\n" {
		t.Errorf("content = %q", got)
	}
	if got := files[0].GetMode(); got != 0o640 {
		t.Errorf("mode = %o, want 640", got)
	}
}

func TestReadDirectory(t *testing.T) {
	root := t.TempDir()
	write(t, root, "flags/b.json", "b")
	write(t, root, "flags/a.json", "a")
	write(t, root, "flags/nested/c.json", "c")
	write(t, root, "other/x", "x")

	b, err := reader(root).Read("/flags/")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range b.GetFiles() {
		got = append(got, f.GetPath())
	}
	want := []string{"a.json", "b.json", "nested/c.json"}
	if len(got) != len(want) {
		t.Fatalf("paths = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("paths = %v, want %v", got, want)
		}
	}
	if err := bundle.Validate(b, bundle.DefaultLimits()); err != nil {
		t.Errorf("a directory bundle must be deliverable: %v", err)
	}
}

// A mounted ConfigMap is a tree of links through "..data"; it must read as its
// current content and its bookkeeping must stay out of the bundle.
func TestReadFollowsAtomicWriterLayout(t *testing.T) {
	root := t.TempDir()
	write(t, root, "..2026_01_01/app.yaml", "v1")
	if err := os.Symlink("..2026_01_01", filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..data/app.yaml", filepath.Join(root, "app.yaml")); err != nil {
		t.Fatal(err)
	}

	file, err := reader(root).Read("/app.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(file.GetFiles()[0].GetContent()); got != "v1" {
		t.Errorf("content = %q, want v1", got)
	}

	dir, err := reader(root).Read("/")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(dir.GetFiles()); n != 1 || dir.GetFiles()[0].GetPath() != "app.yaml" {
		t.Errorf("root bundle = %v, want only app.yaml", dir.GetFiles())
	}
}

func TestReadErrors(t *testing.T) {
	root := t.TempDir()
	write(t, root, "app.yaml", "x")
	write(t, root, "flags/a", "a")
	write(t, root, "..hidden/secret", "s")

	for name, tc := range map[string]struct {
		path string
		want error
	}{
		"missing":                 {"/nope.yaml", ErrNotFound},
		"file named as directory": {"/app.yaml/", ErrNotFound},
		"directory named as file": {"/flags", ErrNotFound},
		"relative":                {"app.yaml", ErrInvalid},
		"reserved segment":        {"/..hidden/secret", ErrInvalid},
		"parent":                  {"/../etc/passwd", ErrInvalid},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := reader(root).Read(tc.path); !errors.Is(err, tc.want) {
				t.Errorf("Read(%q) = %v, want %v", tc.path, err, tc.want)
			}
		})
	}
}

func TestReadLimits(t *testing.T) {
	root := t.TempDir()
	write(t, root, "d/a", "12345")
	write(t, root, "d/b", "12345")

	exact := Reader{Root: root, Limits: bundle.Limits{MaxBytes: 10, MaxFiles: 2}}
	if _, err := exact.Read("/d/"); err != nil {
		t.Errorf("a bundle exactly at the limits must be readable: %v", err)
	}
	small := Reader{Root: root, Limits: bundle.Limits{MaxBytes: 9, MaxFiles: 2}}
	if _, err := small.Read("/d/"); !errors.Is(err, bundle.ErrTooLarge) {
		t.Errorf("err = %v, want ErrTooLarge", err)
	}
	few := Reader{Root: root, Limits: bundle.Limits{MaxBytes: 10, MaxFiles: 1}}
	if _, err := few.Read("/d/"); !errors.Is(err, bundle.ErrTooManyFiles) {
		t.Errorf("err = %v, want ErrTooManyFiles", err)
	}
}
