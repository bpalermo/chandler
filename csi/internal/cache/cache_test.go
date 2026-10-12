package cache

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
)

func oneFile(content string) (*chandlerv1.Bundle, string) {
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
	c, err := Open(dir, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	b, version := oneFile("v1")
	if err := c.Put("file:///app", version, b); err != nil {
		t.Fatal(err)
	}

	// A new process finds what the last one stored, byte for byte.
	reopened, err := Open(dir, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Get(version)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.Version(got) != version {
		t.Errorf("round trip changed the version")
	}
	if v := reopened.Versions("file:///app"); len(v) != 1 || v[0] != version {
		t.Errorf("versions after reopen = %v", v)
	}

	for _, bad := range []string{"", "v1", "sha256:zz", "sha256:../../etc/passwd", "sha256:" + version[7:20]} {
		if _, err := c.Get(bad); !errors.Is(err, ErrNotCached) {
			t.Errorf("Get(%q) = %v, want ErrNotCached", bad, err)
		}
		if err := c.Put("file:///app", bad, b); !errors.Is(err, ErrNotCached) {
			t.Errorf("Put(%q) = %v, want it refused", bad, err)
		}
	}
}

func TestVersionsPerURI(t *testing.T) {
	c, _ := Open(t.TempDir(), time.Hour)
	var versions []string
	for _, content := range []string{"v1", "v2", "v3"} {
		b, version := oneFile(content)
		if err := c.Put("file:///app", version, b); err != nil {
			t.Fatal(err)
		}
		versions = append(versions, version)
	}
	if got := c.Versions("file:///app"); len(got) != 2 || got[0] != versions[2] || got[1] != versions[1] {
		t.Errorf("versions = %v, want the newest two, newest first", got)
	}
	// Going back makes the old version the newest without duplicating it.
	if err := c.Use("file:///app", versions[1]); err != nil {
		t.Fatal(err)
	}
	if got := c.Versions("file:///app"); len(got) != 2 || got[0] != versions[1] || got[1] != versions[2] {
		t.Errorf("versions after Use = %v", got)
	}
	if got := c.Versions("file:///other"); got != nil {
		t.Errorf("versions of an unknown URI = %v", got)
	}
}

func TestCollect(t *testing.T) {
	dir := t.TempDir()
	c, _ := Open(dir, time.Hour)
	now := time.Now()
	c.now = func() time.Time { return now }

	for _, content := range []string{"v1", "v2", "v3"} {
		b, version := oneFile(content)
		if err := c.Put("file:///live", version, b); err != nil {
			t.Fatal(err)
		}
	}
	idle, idleVersion := oneFile("idle")
	if err := c.Put("file:///idle", idleVersion, idle); err != nil {
		t.Fatal(err)
	}

	// The version that fell off the end is collected; "shared" is one blob
	// however many bundles contain it.
	if err := c.Collect(nil); err != nil {
		t.Fatal(err)
	}
	if n := count(t, filepath.Join(dir, "manifests")); n != 3 {
		t.Errorf("%d manifests, want 3 (two of live, one of idle)", n)
	}
	if n := count(t, filepath.Join(dir, "blobs")); n != 4 {
		t.Errorf("%d blobs, want 4 (three contents and the shared one)", n)
	}

	// Past the grace period an unused URI goes, a used one stays.
	now = now.Add(2 * time.Hour)
	if err := c.Collect([]string{"file:///live"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(idleVersion); !errors.Is(err, ErrNotCached) {
		t.Errorf("an idle URI survived its grace period: %v", err)
	}
	if got := c.Versions("file:///live"); len(got) != 2 {
		t.Errorf("a URI in use was collected: %v", got)
	}
	if _, err := c.Get(c.Versions("file:///live")[0]); err != nil {
		t.Errorf("a version in use is gone: %v", err)
	}

	// Touch is what keeps a URI through the next collection.
	now = now.Add(2 * time.Hour)
	c.Touch("file:///live")
	if err := c.Collect(nil); err != nil {
		t.Fatal(err)
	}
	if got := c.Versions("file:///live"); len(got) != 2 {
		t.Errorf("a touched URI was collected: %v", got)
	}
}
