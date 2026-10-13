// Package mount is the small set of mount operations the CSI driver needs:
// read-only bind mounts of a volume directory into a pod, and their removal.
package mount

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// Mounter mounts volume directories into pods.
type Mounter interface {
	// BindReadOnly makes source visible at target, read-only. Both must be
	// existing directories.
	BindReadOnly(source, target string) error
	// Memory mounts an empty in-memory filesystem of at most size bytes at
	// target, an existing directory. What is written there never reaches a
	// disk.
	Memory(target string, size uint64) error
	// Unmount removes the mount at target. It is not an error if nothing is
	// mounted there.
	Unmount(target string) error
	// IsMountPoint reports whether something is mounted exactly at target.
	IsMountPoint(target string) (bool, error)
}

// mountPoints returns the mount points listed in a mountinfo file
// (proc_pid_mountinfo(5)).
func mountPoints(mountinfo io.Reader) (map[string]struct{}, error) {
	points := make(map[string]struct{})
	scanner := bufio.NewScanner(mountinfo)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		// Field 5 is the mount point, relative to the process's root.
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}
		points[unescape(fields[4])] = struct{}{}
	}
	return points, scanner.Err()
}

// unescape decodes the octal escapes mountinfo uses for space, tab, newline
// and backslash, e.g. "\040".
func unescape(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
