// Package atomic replaces the contents of a volume directory atomically, the
// way Kubernetes does for ConfigMap volumes: every generation is written to
// its own hidden directory and a single rename of the "..data" symlink makes
// it current. A reader sees the old tree or the new tree, never a mix.
//
// Layout of a volume directory:
//
//	..2026_10_12_07_51_00.1234567/   generation directory (current)
//	..2026_10_12_07_40_11.7654321/   generation directory (previous, kept)
//	..data -> ..2026_10_12_07_51_00.1234567
//	app.yaml -> ..data/app.yaml
//	flags    -> ..data/flags
package atomic

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bpalermo/stevedore/common/bundle"
)

const (
	dataLink    = "..data"
	dataLinkTmp = "..data_tmp"
	// Generation directories are named with this layout plus a random suffix.
	generationLayout = "..2006_01_02_15_04_05."

	dirMode = 0o755
)

// File is one file of a volume generation.
type File struct {
	Mode    fs.FileMode
	Content []byte
}

// Writer owns one volume directory. It is not safe for concurrent use; the
// caller serialises writes per volume.
type Writer struct {
	dir string
	// now is the clock used to name generations; replaceable in tests.
	now func() time.Time
}

// NewWriter returns a Writer for dir, which must already exist.
func NewWriter(dir string) *Writer {
	return &Writer{dir: dir, now: time.Now}
}

// Write makes files the complete content of the volume. Keys are paths
// relative to the volume root and must satisfy bundle.ValidatePath.
//
// The new generation is fully written and synced before it becomes visible. If
// anything fails before the swap, the volume is left exactly as it was. After
// the swap the previous generation is kept on disk and older ones are removed.
func (w *Writer) Write(files map[string]File) error {
	return w.WriteWith(files, nil)
}

// WriteWith is Write with driver-owned files added to the generation. Their
// names are single path segments starting with "..", which no bundle may use,
// so they can never collide with configuration. Each is swapped in with the
// generation it describes and linked at the volume root like any other entry.
func (w *Writer) WriteWith(files, reserved map[string]File) error {
	for path := range files {
		if err := bundle.ValidatePath(path); err != nil {
			return err
		}
	}
	all := make(map[string]File, len(files)+len(reserved))
	for path, f := range files {
		all[path] = f
	}
	for name, f := range reserved {
		if !strings.HasPrefix(name, "..") || strings.Contains(name, "/") || name == ".." ||
			name == dataLink || name == dataLinkTmp {
			return fmt.Errorf("%q is not a name the driver may reserve", name)
		}
		all[name] = f
	}
	files = all

	previous, err := w.Current()
	if err != nil {
		return err
	}

	genPath, err := os.MkdirTemp(w.dir, w.now().UTC().Format(generationLayout))
	if err != nil {
		return fmt.Errorf("create generation directory: %w", err)
	}
	generation := filepath.Base(genPath)
	if err := os.Chmod(genPath, dirMode); err != nil {
		os.RemoveAll(genPath)
		return fmt.Errorf("create generation directory: %w", err)
	}
	if err := writeGeneration(genPath, files); err != nil {
		os.RemoveAll(genPath)
		return err
	}

	// The swap: rename(2) replaces the ..data symlink in one step.
	tmp := filepath.Join(w.dir, dataLinkTmp)
	_ = os.Remove(tmp)
	if err := os.Symlink(generation, tmp); err != nil {
		os.RemoveAll(genPath)
		return fmt.Errorf("stage %s: %w", dataLink, err)
	}
	if err := os.Rename(tmp, filepath.Join(w.dir, dataLink)); err != nil {
		os.Remove(tmp)
		os.RemoveAll(genPath)
		return fmt.Errorf("swap %s: %w", dataLink, err)
	}

	// From here on the new generation is live; what follows is housekeeping
	// and must not undo it.
	var errs []error
	errs = append(errs, w.syncVisibleLinks(files))
	errs = append(errs, syncDir(w.dir))
	errs = append(errs, w.removeGenerationsExcept(generation, previous))
	return errors.Join(errs...)
}

// Current returns the name of the generation directory ..data points to, or
// "" when the volume has never been written.
func (w *Writer) Current() (string, error) {
	target, err := os.Readlink(filepath.Join(w.dir, dataLink))
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read %s: %w", dataLink, err)
	}
	return target, nil
}

func writeGeneration(genPath string, files map[string]File) error {
	dirs := map[string]struct{}{genPath: {}}
	for path, f := range files {
		full := filepath.Join(genPath, filepath.FromSlash(path))
		parent := filepath.Dir(full)
		if err := os.MkdirAll(parent, dirMode); err != nil {
			return fmt.Errorf("create directory for %q: %w", path, err)
		}
		for d := parent; len(d) >= len(genPath); d = filepath.Dir(d) {
			dirs[d] = struct{}{}
		}
		if err := writeFile(full, f); err != nil {
			return fmt.Errorf("write %q: %w", path, err)
		}
	}
	for d := range dirs {
		if err := syncDir(d); err != nil {
			return err
		}
	}
	return nil
}

func writeFile(path string, f File) error {
	mode := f.Mode.Perm()
	if mode == 0 {
		mode = bundle.DefaultMode
	}
	// O_EXCL: a path that is already there means two entries collided, e.g.
	// one is a directory of the other.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if _, err := file.Write(f.Content); err != nil {
		file.Close()
		return err
	}
	// The mode passed to OpenFile is filtered by the umask.
	if err := file.Chmod(mode); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

// syncVisibleLinks makes the volume root hold exactly one symlink per
// top-level entry of files, each pointing through ..data.
func (w *Writer) syncVisibleLinks(files map[string]File) error {
	want := make(map[string]struct{})
	for path := range files {
		top, _, _ := strings.Cut(path, "/")
		want[top] = struct{}{}
	}

	var errs []error
	for name := range want {
		link := filepath.Join(w.dir, name)
		target := filepath.Join(dataLink, name)
		if existing, err := os.Readlink(link); err == nil && existing == target {
			continue
		}
		_ = os.Remove(link)
		if err := os.Symlink(target, link); err != nil {
			errs = append(errs, fmt.Errorf("link %q: %w", name, err))
		}
	}

	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	for _, e := range entries {
		name := e.Name()
		// Names starting with ".." belong to the writer or the driver.
		if strings.HasPrefix(name, "..") {
			continue
		}
		if _, ok := want[name]; !ok {
			if err := os.Remove(filepath.Join(w.dir, name)); err != nil {
				errs = append(errs, fmt.Errorf("remove stale %q: %w", name, err))
			}
		}
	}
	return errors.Join(errs...)
}

// removeGenerationsExcept deletes every generation directory other than the
// ones named.
func (w *Writer) removeGenerationsExcept(keep ...string) error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}
	var errs []error
	for _, e := range entries {
		if !e.IsDir() || !isGeneration(e.Name()) {
			continue
		}
		kept := false
		for _, k := range keep {
			kept = kept || e.Name() == k
		}
		if !kept {
			errs = append(errs, os.RemoveAll(filepath.Join(w.dir, e.Name())))
		}
	}
	return errors.Join(errs...)
}

// isGeneration reports whether name looks like a generation directory, as
// opposed to another ".."-prefixed entry such as the acknowledgement socket.
func isGeneration(name string) bool {
	stamp := len(generationLayout)
	if len(name) <= stamp {
		return false
	}
	_, err := time.Parse(generationLayout, name[:stamp])
	return err == nil
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", path, err)
	}
	return nil
}
