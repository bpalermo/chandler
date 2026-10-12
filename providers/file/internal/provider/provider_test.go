package provider

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	chandlerv1 "github.com/bpalermo/chandler/api/chandler/v1"
	"github.com/bpalermo/chandler/common/bundle"
)

const wait = 10 * time.Second

type watcher struct {
	t      *testing.T
	stream chandlerv1.ConfigProvider_StreamConfigsClient
	in     chan *chandlerv1.ProviderResponse
}

// start serves root as provider "file" and opens a stream to it with the
// given size limit.
func start(t *testing.T, root string, maxBytes uint64) *watcher {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	chandlerv1.RegisterConfigProviderServer(server, &Server{
		Name:         "file",
		Root:         root,
		PollInterval: 10 * time.Millisecond,
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := chandlerv1.NewConfigProviderClient(conn).StreamConfigs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w := &watcher{t: t, stream: stream, in: make(chan *chandlerv1.ProviderResponse, 64)}
	w.send(chandlerv1.ProviderRequest_builder{
		Hello: chandlerv1.WatcherHello_builder{
			ServerId:       proto.String("server-1"),
			MaxBundleBytes: proto.Uint64(maxBytes),
		}.Build(),
	}.Build())
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			w.in <- msg
		}
	}()
	if hello := w.next(); !hello.HasHello() || hello.GetHello().GetName() != "file" {
		t.Fatalf("first message = %v, want the ProviderHello", hello)
	}
	return w
}

func (w *watcher) send(req *chandlerv1.ProviderRequest) {
	w.t.Helper()
	if err := w.stream.Send(req); err != nil {
		w.t.Fatal(err)
	}
}

func (w *watcher) watch(id, uri, known string) {
	w.t.Helper()
	watch := chandlerv1.Watch_builder{WatchId: proto.String(id), Uri: proto.String(uri)}
	if known != "" {
		watch.KnownRevision = proto.String(known)
	}
	w.send(chandlerv1.ProviderRequest_builder{Watch: watch.Build()}.Build())
}

func (w *watcher) next() *chandlerv1.ProviderResponse {
	w.t.Helper()
	select {
	case msg := <-w.in:
		return msg
	case <-time.After(wait):
		w.t.Fatal("the provider sent nothing")
		return nil
	}
}

func (w *watcher) update(id string) *chandlerv1.ResourceUpdate {
	w.t.Helper()
	msg := w.next()
	if !msg.HasUpdate() || msg.GetUpdate().GetWatchId() != id {
		w.t.Fatalf("got %v, want an update of watch %s", msg, id)
	}
	return msg.GetUpdate()
}

func (w *watcher) failure(id string, code codes.Code) {
	w.t.Helper()
	msg := w.next()
	if !msg.HasError() || msg.GetError().GetWatchId() != id || codes.Code(msg.GetError().GetStatus().GetCode()) != code {
		w.t.Fatalf("got %v, want a %s error for watch %s", msg, code, id)
	}
}

func (w *watcher) quiet() {
	w.t.Helper()
	select {
	case msg := <-w.in:
		w.t.Fatalf("unexpected %v", msg)
	case <-time.After(100 * time.Millisecond): // ten poll intervals
	}
}

func put(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	// Written beside and renamed, as any writer that cares about readers does.
	if err := os.WriteFile(p+".tmp", []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(p+".tmp", p); err != nil {
		t.Fatal(err)
	}
}

func TestWatchFollowsAFile(t *testing.T) {
	root := t.TempDir()
	put(t, root, "app.yaml", "v1")
	w := start(t, root, 0)

	w.watch("1", "file:///app.yaml", "")
	first := w.update("1")
	if !first.HasBundle() || string(first.GetBundle().GetFiles()[0].GetContent()) != "v1" {
		t.Fatalf("first update = %v", first)
	}
	if first.GetCacheKey() != "/app.yaml" || first.GetRevision() != bundle.Version(first.GetBundle()) || first.GetNonce() == "" {
		t.Errorf("key %q revision %q nonce %q", first.GetCacheKey(), first.GetRevision(), first.GetNonce())
	}
	w.quiet() // unchanged: nothing more

	put(t, root, "app.yaml", "v2")
	second := w.update("1")
	if string(second.GetBundle().GetFiles()[0].GetContent()) != "v2" || second.GetNonce() == first.GetNonce() {
		t.Errorf("second update = %v", second)
	}

	// Gone: reported once, and the content returns when the file does.
	if err := os.Remove(filepath.Join(root, "app.yaml")); err != nil {
		t.Fatal(err)
	}
	if deleted := w.update("1"); !deleted.HasDeleted() {
		t.Errorf("after removal got %v, want Deleted", deleted)
	}
	w.quiet()
	put(t, root, "app.yaml", "v3")
	if back := w.update("1"); string(back.GetBundle().GetFiles()[0].GetContent()) != "v3" {
		t.Errorf("after the file returned got %v", back)
	}

	// After an unwatch the provider says nothing more about it.
	w.send(chandlerv1.ProviderRequest_builder{
		Unwatch: chandlerv1.Unwatch_builder{WatchId: proto.String("1")}.Build(),
	}.Build())
	time.Sleep(50 * time.Millisecond)
	put(t, root, "app.yaml", "v4")
	w.quiet()
}

func TestWatchWithKnownRevision(t *testing.T) {
	root := t.TempDir()
	put(t, root, "flags/a.json", "a")
	w := start(t, root, 0)

	w.watch("1", "file:///flags/", "")
	revision := w.update("1").GetRevision()

	// The server already holds this revision: no bytes are sent again.
	w.watch("2", "file:///flags/", revision)
	if again := w.update("2"); !again.HasNotModified() || again.GetRevision() != revision {
		t.Errorf("got %v, want NotModified at %s", again, revision)
	}
	// It holds an older one: the bundle follows.
	w.watch("3", "file:///flags/", "sha256:stale")
	if fresh := w.update("3"); !fresh.HasBundle() {
		t.Errorf("got %v, want the bundle", fresh)
	}
}

func TestWatchErrors(t *testing.T) {
	root := t.TempDir()
	put(t, root, "big.bin", "0123456789")
	w := start(t, root, 4)

	// Missing: reported once, and delivered when it appears.
	w.watch("1", "file:///later.yaml", "")
	w.failure("1", codes.NotFound)
	w.quiet()
	put(t, root, "later.yaml", "ok")
	if u := w.update("1"); !u.HasBundle() {
		t.Errorf("got %v, want the bundle once the file exists", u)
	}

	// Over the server's limit: never read into a bundle.
	w.watch("2", "file:///big.bin", "")
	w.failure("2", codes.ResourceExhausted)

	// URIs the provider cannot interpret end the watch at once. A query
	// parameter is refused, not ignored: it would name the same file twice.
	for id, uri := range map[string]string{
		"3": "file:///later.yaml?version=2",
		"4": "file://host/later.yaml",
		"5": "s3:///later.yaml",
		"6": "file:///..data/later.yaml",
		"7": "not a uri",
	} {
		w.watch(id, uri, "")
		w.failure(id, codes.InvalidArgument)
	}
}
