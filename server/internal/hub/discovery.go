package hub

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/queue"
)

// StreamConfigs implements chandlerv1.ConfigDiscoveryServiceServer: one stream
// per node, carrying every subscription on that node.
func (h *Hub) StreamConfigs(stream chandlerv1.ConfigDiscoveryService_StreamConfigsServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if !first.HasHello() {
		return grpcstatus.Error(codes.InvalidArgument, "the first message must be a NodeHello")
	}
	sess := &session{
		node: first.GetHello().GetNodeId(),
		cluster: first.GetHello().GetCluster(),
		out:  queue.New[*chandlerv1.ConfigResponse](),
		subs: make(map[string]*subscription),
	}
	log := h.log.With("node", sess.node)
	log.Info("driver connected", "driver_version", first.GetHello().GetDriverVersion())
	defer log.Info("driver disconnected")
	defer h.closeSession(sess)

	sess.out.Push(chandlerv1.ConfigResponse_builder{
		Hello: chandlerv1.ServerHello_builder{
			ServerId:       proto.String(h.id),
			MaxBundleBytes: proto.Uint64(h.limits.MaxBytes),
		}.Build(),
	}.Build())

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-sess.out.Wake():
				for _, msg := range sess.out.Drain() {
					if err := stream.Send(msg); err != nil {
						cancel()
						return
					}
				}
			}
		}
	}()

	for {
		req, err := stream.Recv()
		switch {
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return err
		}
		switch {
		case req.HasSubscribe():
			h.subscribe(sess, req.GetSubscribe())
		case req.HasUnsubscribe():
			h.unsubscribe(sess, req.GetUnsubscribe().GetSubscriptionId())
		case req.HasAck():
			h.ack(sess, req.GetAck())
		case req.HasNack():
			h.nack(sess, req.GetNack())
		case req.HasAppReport():
			// Application acknowledgement is not implemented yet.
		case req.HasHello():
			return grpcstatus.Error(codes.InvalidArgument, "NodeHello sent twice")
		}
	}
}
