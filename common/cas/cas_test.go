package cas

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
)

func twoFiles(content string) (*chandlerv1.Bundle, string) {
	b := chandlerv1.Bundle_builder{Files: []*chandlerv1.File{
		chandlerv1.File_builder{Path: proto.String("dir/app.yaml"), Mode: proto.Uint32(0o640), Content: []byte(content)}.Build(),
		chandlerv1.File_builder{Path: proto.String("default-mode"), Content: []byte("shared")}.Build(),
	}}.Build()
	return b, bundle.Version(b)
}

func count(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			n++
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPutGetAndReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, version := twoFiles("v1")
	if err := s.Put(version, b); err != nil {
		t.Fatal(err)
	}
	// A new process finds what the last one stored, byte for byte, modes
	// included.
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Get(version)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.Version(got) != version {
		t.Error("the round trip changed the version")
	}

	for _, bad := range []string{"", "v1", "sha256:zz", "sha256:../../etc/passwd", "sha256:" + version[7:20]} {
		if _, err := s.Get(bad); !errors.Is(err, ErrNotFound) {
			t.Errorf("Get(%q) = %v, want ErrNotFound", bad, err)
		}
		if err := s.Put(bad, b); !errors.Is(err, ErrNotFound) {
			t.Errorf("Put(%q) = %v, want it refused", bad, err)
		}
	}
}

// Corruption must surface as a miss, never as wrong bytes.
func TestGetDetectsDamage(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	b, version := twoFiles("v1")
	if err := s.Put(version, b); err != nil {
		t.Fatal(err)
	}
	err := filepath.WalkDir(filepath.Join(dir, "blobs"), func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			return os.WriteFile(p, []byte("flipped"), 0o600)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(version); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get of damaged content = %v, want ErrNotFound", err)
	}
}

func TestCollect(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	var versions []string
	for _, content := range []string{"v1", "v2", "v3"} {
		b, version := twoFiles(content)
		if err := s.Put(version, b); err != nil {
			t.Fatal(err)
		}
		versions = append(versions, version)
	}
	// "shared" is one blob however many bundles contain it.
	if n := count(t, filepath.Join(dir, "blobs")); n != 4 {
		t.Fatalf("%d blobs, want 4", n)
	}
	if err := s.Collect(versions[1:]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(versions[0]); !errors.Is(err, ErrNotFound) {
		t.Errorf("a collected version is still there: %v", err)
	}
	for _, v := range versions[1:] {
		if _, err := s.Get(v); err != nil {
			t.Errorf("a kept version is gone: %v", err)
		}
	}
	if n := count(t, filepath.Join(dir, "blobs")); n != 3 {
		t.Errorf("%d blobs after collection, want 3", n)
	}
}
