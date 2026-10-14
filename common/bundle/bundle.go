// Package bundle implements the content-independent rules every component
// applies to a Bundle: its canonical version and the envelope checks that make
// it safe to deliver. Nothing here opens or interprets file contents.
package bundle

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	stevedorev1 "github.com/bpalermo/stevedore/api/stevedore/v1"
)

const (
	// VersionPrefix starts every version string.
	VersionPrefix = "sha256:"

	// DefaultMode is the permission a file gets when it carries none.
	DefaultMode fs.FileMode = 0o444

	// DefaultMaxBytes is the default limit on a bundle's total content size.
	DefaultMaxBytes = 1 << 20
	// HardMaxBytes is the largest limit that may be configured: a bundle
	// travels in one gRPC message.
	HardMaxBytes = 3 << 20
	// DefaultMaxFiles is the default limit on the number of files in a bundle.
	DefaultMaxFiles = 256

	maxSegmentBytes = 255
)

var (
	// ErrInvalidPath reports a file path that could escape the volume or
	// collide with the atomic writer's bookkeeping entries.
	ErrInvalidPath = errors.New("invalid path")
	// ErrPathConflict reports two files that cannot both exist: the same path
	// twice, or one file's path used as another's directory.
	ErrPathConflict = errors.New("conflicting paths")
	// ErrTooLarge reports a bundle whose content exceeds the size limit.
	ErrTooLarge = errors.New("bundle too large")
	// ErrTooManyFiles reports a bundle with more files than the limit.
	ErrTooManyFiles = errors.New("too many files")
)

// Limits bounds what a bundle may contain.
type Limits struct {
	// MaxBytes is the largest total content size, in bytes.
	MaxBytes uint64
	// MaxFiles is the largest number of files.
	MaxFiles int
}

// DefaultLimits returns the limits applied when none are configured.
func DefaultLimits() Limits {
	return Limits{MaxBytes: DefaultMaxBytes, MaxFiles: DefaultMaxFiles}
}

// Mode returns the permission bits a file is created with: the low nine bits
// of its mode, or DefaultMode when it has none.
func Mode(f *stevedorev1.File) fs.FileMode {
	if m := fs.FileMode(f.GetMode()) & fs.ModePerm; m != 0 {
		return m
	}
	return DefaultMode
}

// Size returns the total content size of the bundle in bytes.
func Size(b *stevedorev1.Bundle) uint64 {
	var n uint64
	for _, f := range b.GetFiles() {
		n += uint64(len(f.GetContent()))
	}
	return n
}

// Version returns the bundle's content address, "sha256:<hex>".
//
// The digest covers the files sorted by path, each contributing a big-endian
// uint32 path length, the path, a big-endian uint32 of its effective mode and
// the SHA-256 of its content. File order in the message does not matter, and
// an unset mode hashes the same as an explicit DefaultMode.
func Version(b *stevedorev1.Bundle) string {
	files := append([]*stevedorev1.File(nil), b.GetFiles()...)
	sort.SliceStable(files, func(i, j int) bool { return files[i].GetPath() < files[j].GetPath() })

	h := sha256.New()
	var u32 [4]byte
	for _, f := range files {
		path := f.GetPath()
		binary.BigEndian.PutUint32(u32[:], uint32(len(path)))
		h.Write(u32[:])
		h.Write([]byte(path))
		binary.BigEndian.PutUint32(u32[:], uint32(Mode(f)))
		h.Write(u32[:])
		content := sha256.Sum256(f.GetContent())
		h.Write(content[:])
	}
	return VersionPrefix + hex.EncodeToString(h.Sum(nil))
}

// Verify reports whether the bundle's content address equals version.
func Verify(b *stevedorev1.Bundle, version string) bool {
	return Version(b) == version
}

// ValidatePath checks that a file path is clean, relative and stays clear of
// the names reserved for the atomic writer.
func ValidatePath(path string) error {
	if path == "" {
		return fmt.Errorf("%w: empty", ErrInvalidPath)
	}
	if strings.ContainsRune(path, 0) {
		return fmt.Errorf("%w: %q contains a NUL byte", ErrInvalidPath, path)
	}
	if strings.HasPrefix(path, "/") {
		return fmt.Errorf("%w: %q is absolute", ErrInvalidPath, path)
	}
	for _, seg := range strings.Split(path, "/") {
		switch {
		case seg == "":
			return fmt.Errorf("%w: %q has an empty segment", ErrInvalidPath, path)
		case seg == ".":
			return fmt.Errorf("%w: %q has a \".\" segment", ErrInvalidPath, path)
		case strings.HasPrefix(seg, ".."):
			return fmt.Errorf("%w: %q has a segment starting with \"..\"", ErrInvalidPath, path)
		case len(seg) > maxSegmentBytes:
			return fmt.Errorf("%w: %q has a segment longer than %d bytes", ErrInvalidPath, path, maxSegmentBytes)
		}
	}
	return nil
}

// Validate checks the bundle's envelope: every path is safe, no two files
// conflict, and size and file count are within limits. It is the whole of what
// the server and the driver verify before delivering a bundle.
func Validate(b *stevedorev1.Bundle, limits Limits) error {
	files := b.GetFiles()
	if limits.MaxFiles > 0 && len(files) > limits.MaxFiles {
		return fmt.Errorf("%w: %d files, limit %d", ErrTooManyFiles, len(files), limits.MaxFiles)
	}
	if size := Size(b); limits.MaxBytes > 0 && size > limits.MaxBytes {
		return fmt.Errorf("%w: %d bytes, limit %d", ErrTooLarge, size, limits.MaxBytes)
	}

	paths := make(map[string]struct{}, len(files))
	for _, f := range files {
		path := f.GetPath()
		if err := ValidatePath(path); err != nil {
			return err
		}
		if _, dup := paths[path]; dup {
			return fmt.Errorf("%w: %q appears more than once", ErrPathConflict, path)
		}
		paths[path] = struct{}{}
	}
	// A file cannot also be a directory of another file.
	for path := range paths {
		for i := strings.IndexByte(path, '/'); i >= 0; {
			if _, isFile := paths[path[:i]]; isFile {
				return fmt.Errorf("%w: %q is both a file and a directory of %q", ErrPathConflict, path[:i], path)
			}
			next := strings.IndexByte(path[i+1:], '/')
			if next < 0 {
				break
			}
			i += 1 + next
		}
	}
	return nil
}
