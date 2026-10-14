// Package source reads bundles from a directory on disk. It is the origin of
// the file provider: a path under the root is one resource, either a single
// file or a directory tree.
package source

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	stevedorev1 "github.com/bpalermo/stevedore/api/stevedore/v1"
	"github.com/bpalermo/stevedore/common/bundle"
	"google.golang.org/protobuf/proto"
)

// maxDepth bounds directory recursion, which also ends a symlink cycle.
const maxDepth = 16

var (
	// ErrInvalid reports a path that can never name a resource.
	ErrInvalid = errors.New("invalid path")
	// ErrNotFound reports that nothing of the requested kind is at the path.
	ErrNotFound = errors.New("not found")
)

// Reader reads resources below a root directory.
type Reader struct {
	// Root is the directory resources are resolved against.
	Root string
	// Limits bounds what Read returns; a larger resource is an error
	// wrapping bundle.ErrTooLarge or bundle.ErrTooManyFiles.
	Limits bundle.Limits
}

// Read returns the resource at p, a slash-separated absolute path as it
// appears in a URI. A path ending in "/" names a directory and yields one
// file per regular file below it, with paths relative to it. Any other path
// names one file and yields that file under its base name.
//
// Symbolic links are followed, so a directory maintained by an atomic writer
// (a mounted ConfigMap, a git-sync checkout) reads as its current content.
// Entries whose name starts with ".." are that writer's bookkeeping and are
// neither readable nor listed.
func (r Reader) Read(p string) (*stevedorev1.Bundle, error) {
	b, _, err := r.ReadStat(p)
	return b, err
}

// ReadStat is Read, and also returns the newest modification time among the
// files read: when the resource last changed.
func (r Reader) ReadStat(p string) (*stevedorev1.Bundle, time.Time, error) {
	b, modified, err := r.read(p)
	return b, modified, err
}

func (r Reader) read(p string) (*stevedorev1.Bundle, time.Time, error) {
	if !strings.HasPrefix(p, "/") {
		return nil, time.Time{}, fmt.Errorf("%w: %q is not absolute", ErrInvalid, p)
	}
	isDir := strings.HasSuffix(p, "/")
	// Checked before cleaning, so that ".." is refused rather than resolved.
	for _, segment := range strings.Split(p, "/") {
		if strings.HasPrefix(segment, "..") || strings.ContainsRune(segment, 0) {
			return nil, time.Time{}, fmt.Errorf("%w: %q has a reserved segment", ErrInvalid, p)
		}
	}
	clean := path.Clean(p)
	local := filepath.Join(r.Root, filepath.FromSlash(clean))

	info, err := os.Stat(local)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, time.Time{}, fmt.Errorf("%w: %s", ErrNotFound, p)
	case err != nil:
		return nil, time.Time{}, err
	case isDir && !info.IsDir():
		return nil, time.Time{}, fmt.Errorf("%w: %s is not a directory", ErrNotFound, p)
	case !isDir && info.IsDir():
		return nil, time.Time{}, fmt.Errorf("%w: %s is a directory; name it with a trailing slash", ErrNotFound, p)
	case !isDir && !info.Mode().IsRegular():
		return nil, time.Time{}, fmt.Errorf("%w: %s is not a regular file", ErrNotFound, p)
	}

	w := &walk{limits: r.Limits}
	if isDir {
		err = w.dir(local, "", 0)
	} else {
		err = w.file(local, path.Base(clean), info)
	}
	if err != nil {
		return nil, time.Time{}, err
	}
	sort.Slice(w.files, func(i, j int) bool { return w.files[i].GetPath() < w.files[j].GetPath() })
	return stevedorev1.Bundle_builder{Files: w.files}.Build(), w.modified, nil
}

type walk struct {
	limits bundle.Limits
	files  []*stevedorev1.File
	bytes  uint64
	// modified is the newest modification time among the files read.
	modified time.Time
}

func (w *walk) dir(local, rel string, depth int) error {
	if depth > maxDepth {
		return fmt.Errorf("%w: directories nest deeper than %d", ErrInvalid, maxDepth)
	}
	entries, err := os.ReadDir(local)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "..") {
			continue
		}
		child := filepath.Join(local, name)
		// Stat, not entry.Info: links are followed.
		info, err := os.Stat(child)
		if errors.Is(err, fs.ErrNotExist) {
			continue // removed while listing, or a dangling link
		}
		if err != nil {
			return err
		}
		switch {
		case info.IsDir():
			if err := w.dir(child, path.Join(rel, name), depth+1); err != nil {
				return err
			}
		case info.Mode().IsRegular():
			if err := w.file(child, path.Join(rel, name), info); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *walk) file(local, rel string, info fs.FileInfo) error {
	if len(w.files) >= w.limits.MaxFiles {
		return fmt.Errorf("%w: more than %d", bundle.ErrTooManyFiles, w.limits.MaxFiles)
	}
	f, err := os.Open(local)
	if err != nil {
		return err
	}
	defer f.Close()
	// One byte past what is left tells too large from exactly full.
	left := w.limits.MaxBytes - w.bytes
	content, err := io.ReadAll(io.LimitReader(f, int64(left)+1))
	if err != nil {
		return err
	}
	if uint64(len(content)) > left {
		return fmt.Errorf("%w: more than %d bytes", bundle.ErrTooLarge, w.limits.MaxBytes)
	}
	w.bytes += uint64(len(content))
	if info.ModTime().After(w.modified) {
		w.modified = info.ModTime()
	}
	w.files = append(w.files, stevedorev1.File_builder{
		Path:    proto.String(rel),
		Mode:    proto.Uint32(uint32(info.Mode().Perm())),
		Content: content,
	}.Build())
	return nil
}
