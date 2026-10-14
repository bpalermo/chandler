package bundle

import (
	"errors"
	"strings"
	"testing"

	stevedorev1 "github.com/bpalermo/stevedore/api/stevedore/v1"
	"google.golang.org/protobuf/proto"
)

func file(path string, mode uint32, content string) *stevedorev1.File {
	b := stevedorev1.File_builder{Path: proto.String(path), Content: []byte(content)}
	if mode != 0 {
		b.Mode = proto.Uint32(mode)
	}
	return b.Build()
}

func bundleOf(files ...*stevedorev1.File) *stevedorev1.Bundle {
	return stevedorev1.Bundle_builder{Files: files}.Build()
}

func TestVersion(t *testing.T) {
	base := bundleOf(file("a.yaml", 0, "a: 1"), file("dir/b.yaml", 0o440, "b: 2"))
	v := Version(base)

	if !strings.HasPrefix(v, VersionPrefix) || len(v) != len(VersionPrefix)+64 {
		t.Fatalf("Version() = %q, want sha256:<64 hex>", v)
	}
	if !Verify(base, v) {
		t.Error("Verify() = false for the bundle's own version")
	}

	same := map[string]*stevedorev1.Bundle{
		"file order":            bundleOf(file("dir/b.yaml", 0o440, "b: 2"), file("a.yaml", 0, "a: 1")),
		"explicit default mode": bundleOf(file("a.yaml", 0o444, "a: 1"), file("dir/b.yaml", 0o440, "b: 2")),
		"non-permission bits":   bundleOf(file("a.yaml", 0o444|0o4000, "a: 1"), file("dir/b.yaml", 0o440, "b: 2")),
	}
	for name, b := range same {
		if got := Version(b); got != v {
			t.Errorf("%s: Version() = %q, want %q", name, got, v)
		}
	}

	different := map[string]*stevedorev1.Bundle{
		"content":      bundleOf(file("a.yaml", 0, "a: 2"), file("dir/b.yaml", 0o440, "b: 2")),
		"mode":         bundleOf(file("a.yaml", 0o400, "a: 1"), file("dir/b.yaml", 0o440, "b: 2")),
		"path":         bundleOf(file("c.yaml", 0, "a: 1"), file("dir/b.yaml", 0o440, "b: 2")),
		"missing file": bundleOf(file("a.yaml", 0, "a: 1")),
		// The path length prefix keeps path and content boundaries distinct.
		"moved boundary": bundleOf(file("a.yamld", 0, "a: 1"), file("ir/b.yaml", 0o440, "b: 2")),
	}
	for name, b := range different {
		if got := Version(b); got == v {
			t.Errorf("%s: Version() did not change", name)
		}
	}

	if Version(bundleOf()) == v {
		t.Error("empty bundle has the same version as a non-empty one")
	}
}

func TestValidatePath(t *testing.T) {
	valid := []string{"a", "a.yaml", "dir/a.yaml", "a/b/c", ".hidden", "a/.b", "a..b", "a/b.."}
	for _, p := range valid {
		if err := ValidatePath(p); err != nil {
			t.Errorf("ValidatePath(%q) = %v, want nil", p, err)
		}
	}

	invalid := []string{
		"", "/a", "a//b", "a/", "./a", "a/./b", "..", "../a", "a/../b",
		"..data", "a/..data", "..2026_10_12", "...a", "a\x00b",
		strings.Repeat("x", 256),
	}
	for _, p := range invalid {
		if err := ValidatePath(p); !errors.Is(err, ErrInvalidPath) {
			t.Errorf("ValidatePath(%q) = %v, want ErrInvalidPath", p, err)
		}
	}
}

func TestValidate(t *testing.T) {
	limits := Limits{MaxBytes: 10, MaxFiles: 2}

	tests := []struct {
		name   string
		bundle *stevedorev1.Bundle
		limits Limits
		want   error
	}{
		{"empty", bundleOf(), limits, nil},
		{"ok", bundleOf(file("a", 0, "12345"), file("d/b", 0, "12345")), limits, nil},
		{"siblings sharing a prefix", bundleOf(file("a", 0, ""), file("a.b", 0, "")), limits, nil},
		{"too large", bundleOf(file("a", 0, "123456"), file("b", 0, "12345")), limits, ErrTooLarge},
		{"too many files", bundleOf(file("a", 0, ""), file("b", 0, ""), file("c", 0, "")), limits, ErrTooManyFiles},
		{"unsafe path", bundleOf(file("../a", 0, "")), limits, ErrInvalidPath},
		{"duplicate path", bundleOf(file("a", 0, ""), file("a", 0, "")), limits, ErrPathConflict},
		{"file used as directory", bundleOf(file("a", 0, ""), file("a/b", 0, "")), limits, ErrPathConflict},
		{"file used as deep directory", bundleOf(file("a/b", 0, ""), file("a/b/c/d", 0, "")), Limits{}, ErrPathConflict},
		{"zero limits mean unlimited", bundleOf(file("a", 0, strings.Repeat("x", 100)), file("b", 0, ""), file("c", 0, "")), Limits{}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.bundle, tt.limits)
			if !errors.Is(err, tt.want) || (tt.want == nil && err != nil) {
				t.Errorf("Validate() = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestModeAndSize(t *testing.T) {
	if got := Mode(file("a", 0, "")); got != DefaultMode {
		t.Errorf("Mode(unset) = %v, want %v", got, DefaultMode)
	}
	if got := Mode(file("a", 0o640, "")); got != 0o640 {
		t.Errorf("Mode(0640) = %v", got)
	}
	if got := Size(bundleOf(file("a", 0, "123"), file("b", 0, "45"))); got != 5 {
		t.Errorf("Size() = %d, want 5", got)
	}
}
