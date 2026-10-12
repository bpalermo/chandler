// Command chandler-server is the Chandler config server (placeholder).
package main

import (
	"fmt"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/version"
)

func main() {
	// Reference the generated package so the proto -> Go path is exercised.
	pkg := (&chandlerv1.Bundle{}).ProtoReflect().Descriptor().ParentFile().Package()
	fmt.Printf("chandler-server %s (api %s)\n", version.Version, pkg)
}
