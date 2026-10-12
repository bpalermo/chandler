module github.com/bpalermo/chandler

go 1.27.2

require (
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261005182115-fad411399dd8
	google.golang.org/grpc v1.84.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.6.2
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/container-storage-interface/spec v1.13.0
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.49.0
	golang.org/x/text v0.40.0 // indirect
)

tool google.golang.org/grpc/cmd/protoc-gen-go-grpc
