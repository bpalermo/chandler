// Package version exposes the build version shared by all Chandler binaries.
package version

// Version is the build version. Override at link time with
// -X github.com/bpalermo/chandler/common/version.Version=<v>.
var Version = "dev"
