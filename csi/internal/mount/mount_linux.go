package mount

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// System mounts through the kernel directly; the driver image carries no mount
// binary.
type System struct{}

var _ Mounter = System{}

// BindReadOnly implements Mounter.
func (System) BindReadOnly(source, target string) error {
	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind %s on %s: %w", source, target, err)
	}
	// A bind mount ignores MS_RDONLY when it is created; it takes a remount.
	flags := uintptr(unix.MS_BIND | unix.MS_REMOUNT | unix.MS_RDONLY | unix.MS_NOSUID | unix.MS_NODEV)
	if err := unix.Mount("", target, "", flags, ""); err != nil {
		_ = unix.Unmount(target, 0)
		return fmt.Errorf("remount %s read-only: %w", target, err)
	}
	return nil
}

// Unmount implements Mounter.
func (System) Unmount(target string) error {
	err := unix.Unmount(target, 0)
	// EINVAL: not a mount point. ENOENT: already gone.
	if err == nil || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOENT) {
		return nil
	}
	return fmt.Errorf("unmount %s: %w", target, err)
}

// IsMountPoint implements Mounter.
func (System) IsMountPoint(target string) (bool, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	defer f.Close()
	points, err := mountPoints(f)
	if err != nil {
		return false, err
	}
	_, ok := points[filepath.Clean(target)]
	return ok, nil
}
