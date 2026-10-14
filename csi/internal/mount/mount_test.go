package mount

import (
	"strings"
	"testing"
)

const mountinfo = `22 28 0:21 / /sys rw,nosuid,nodev,noexec,relatime shared:7 - sysfs sysfs rw
28 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
950 28 8:1 /var/lib/stevedore/volumes/csi-1/data /var/lib/kubelet/pods/a/volumes/kubernetes.io~csi/config/mount ro,relatime shared:1 - ext4 /dev/sda1 rw
951 28 8:1 /x /mnt/with\040space\011tab\134slash rw - ext4 /dev/sda1 rw
short line
`

func TestMountPoints(t *testing.T) {
	points, err := mountPoints(strings.NewReader(mountinfo))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"/", "/sys",
		"/var/lib/kubelet/pods/a/volumes/kubernetes.io~csi/config/mount",
		"/mnt/with space\ttab\\slash",
	} {
		if _, ok := points[want]; !ok {
			t.Errorf("mount point %q not found in %v", want, points)
		}
	}
	// The mount source is not a mount point.
	if _, ok := points["/var/lib/stevedore/volumes/csi-1/data"]; ok {
		t.Error("a mount source was reported as a mount point")
	}
	if len(points) != 4 {
		t.Errorf("got %d mount points, want 4", len(points))
	}
}

func TestUnescape(t *testing.T) {
	for in, want := range map[string]string{
		`plain`:      "plain",
		`a\040b`:     "a b",
		`a\134b`:     `a\b`,
		`trailing\0`: `trailing\0`,
		`bad\999x`:   `bad\999x`,
	} {
		if got := unescape(in); got != want {
			t.Errorf("unescape(%q) = %q, want %q", in, got, want)
		}
	}
}
