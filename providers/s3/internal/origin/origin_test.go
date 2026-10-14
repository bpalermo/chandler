package origin

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/bpalermo/stevedore/common/bundle"
	"github.com/bpalermo/stevedore/common/provider"
	"github.com/bpalermo/stevedore/common/rollout"
	"github.com/bpalermo/stevedore/common/uri"
)

type stored struct {
	content      string
	cacheControl string
	versionID    string
	modified     time.Time
	metadata     map[string]string
}

// fakeS3 is one bucket, "config", held in memory.
type fakeS3 struct {
	objects map[string]stored
	// pageSize makes listings paginate.
	pageSize int
	// beforeGet runs before every GetObject, to change the bucket mid-read.
	beforeGet func(key string)
	denied    bool
	gets      int
}

func etag(content string) string {
	sum := md5.Sum([]byte(content))
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func (f *fakeS3) check(bucket *string) error {
	switch {
	case f.denied:
		return &smithy.GenericAPIError{Code: "AccessDenied", Message: "no"}
	case aws.ToString(bucket) != "config":
		return &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "no such bucket"}
	}
	return nil
}

func (f *fakeS3) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if err := f.check(in.Bucket); err != nil {
		return nil, err
	}
	o, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &types.NotFound{}
	}
	out := &s3.HeadObjectOutput{
		ETag: aws.String(etag(o.content)), ContentLength: aws.Int64(int64(len(o.content))),
		LastModified: aws.Time(o.modified), Metadata: o.metadata,
	}
	if o.cacheControl != "" {
		out.CacheControl = aws.String(o.cacheControl)
	}
	if o.versionID != "" {
		out.VersionId = aws.String(o.versionID)
	}
	return out, nil
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if err := f.check(in.Bucket); err != nil {
		return nil, err
	}
	f.gets++
	if f.beforeGet != nil {
		f.beforeGet(aws.ToString(in.Key))
	}
	o, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	if in.IfMatch != nil && *in.IfMatch != etag(o.content) {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "changed"}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader([]byte(o.content)))}, nil
}

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if err := f.check(in.Bucket); err != nil {
		return nil, err
	}
	var keys []string
	for key := range f.objects {
		if strings.HasPrefix(key, aws.ToString(in.Prefix)) && key > aws.ToString(in.ContinuationToken) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(false)}
	for i, key := range keys {
		if f.pageSize > 0 && i == f.pageSize {
			out.IsTruncated = aws.Bool(true)
			out.NextContinuationToken = aws.String(keys[i-1])
			break
		}
		o := f.objects[key]
		out.Contents = append(out.Contents, types.Object{
			Key: aws.String(key), ETag: aws.String(etag(o.content)), Size: aws.Int64(int64(len(o.content))),
			LastModified: aws.Time(o.modified),
		})
	}
	return out, nil
}

func resolve(t *testing.T, o Origin, raw string) provider.Resource {
	t.Helper()
	u, err := uri.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	r, err := o.Resolve(u)
	if err != nil {
		t.Fatalf("Resolve(%s): %v", raw, err)
	}
	return r
}

func TestResolve(t *testing.T) {
	o := Origin{Client: &fakeS3{}, Buckets: []string{"config"}}
	for raw, key := range map[string]string{
		"s3://config/app.yaml":         "config/app.yaml",
		"s3://config/team/../app.yaml": "config/app.yaml",
		"s3://config/flags/":           "config/flags/",
		"s3://config/":                 "config/",
	} {
		if got := resolve(t, o, raw).CacheKey(); got != key {
			t.Errorf("cache key of %s = %q, want %q", raw, got, key)
		}
	}
	// Refused, not ignored.
	for _, raw := range []string{
		"s3:///app.yaml",                   // no bucket
		"s3://config",                      // no key, and not a directory
		"s3://config/app.yaml?versionId=3", // a second name for the object
		"s3://secrets/app.yaml",            // not a bucket this provider serves
	} {
		u, err := uri.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := o.Resolve(u); !errors.Is(err, provider.ErrInvalid) {
			t.Errorf("Resolve(%s) = %v, want ErrInvalid", raw, err)
		}
	}
	// With no allow-list, any bucket may be named.
	open := Origin{Client: &fakeS3{}}
	resolve(t, open, "s3://anything/app.yaml")
}

func TestObject(t *testing.T) {
	fake := &fakeS3{objects: map[string]stored{
		"team/app.yaml": {content: "v1", cacheControl: "max-age=60, stale-if-error=3600, public"},
	}}
	r := resolve(t, Origin{Client: fake}, "s3://config/team/app.yaml")
	ctx, limits := context.Background(), bundle.DefaultLimits()

	first, err := r.Fetch(ctx, "", limits)
	if err != nil {
		t.Fatal(err)
	}
	files := first.Bundle.GetFiles()
	if len(files) != 1 || files[0].GetPath() != "app.yaml" || string(files[0].GetContent()) != "v1" {
		t.Errorf("bundle = %v, want the object under its base name", files)
	}
	if first.Revision != etag("v1") {
		t.Errorf("revision = %q, want the ETag", first.Revision)
	}
	if cc := first.CacheControl; cc.GetMaxAge().GetSeconds() != 60 || cc.GetStaleIfError().GetSeconds() != 3600 || cc.GetNoStore() {
		t.Errorf("cache control = %v", cc)
	}

	// Unchanged: the content is not read again.
	gets := fake.gets
	again, err := r.Fetch(ctx, first.Revision, limits)
	if err != nil || again.Bundle != nil || again.Revision != first.Revision || fake.gets != gets {
		t.Errorf("Fetch(known) = %+v, %v after %d more reads; want no bundle and no read", again, err, fake.gets-gets)
	}

	// In a versioned bucket the version ID is the revision.
	fake.objects["team/app.yaml"] = stored{content: "v2", versionID: "3HL4kqtJlcpXroDTDmJ"}
	versioned, err := r.Fetch(ctx, first.Revision, limits)
	if err != nil || versioned.Revision != "3HL4kqtJlcpXroDTDmJ" || string(versioned.Bundle.GetFiles()[0].GetContent()) != "v2" {
		t.Errorf("Fetch of a versioned object = %+v, %v", versioned, err)
	}
	if versioned.CacheControl != nil {
		t.Errorf("an object with no Cache-Control got %v", versioned.CacheControl)
	}

	if _, err := r.Fetch(ctx, "", bundle.Limits{MaxBytes: 1, MaxFiles: 1}); !errors.Is(err, bundle.ErrTooLarge) {
		t.Errorf("over the limit: %v, want ErrTooLarge", err)
	}
	delete(fake.objects, "team/app.yaml")
	if _, err := r.Fetch(ctx, "", limits); !errors.Is(err, provider.ErrNotFound) {
		t.Errorf("deleted: %v, want ErrNotFound", err)
	}
	if _, err := resolve(t, Origin{Client: fake}, "s3://other/app.yaml").Fetch(ctx, "", limits); !errors.Is(err, provider.ErrNotFound) {
		t.Errorf("missing bucket: %v, want ErrNotFound", err)
	}
	fake.denied = true
	if _, err := r.Fetch(ctx, "", limits); !errors.Is(err, provider.ErrPermission) {
		t.Errorf("denied: %v, want ErrPermission", err)
	}
}

func TestPrefix(t *testing.T) {
	fake := &fakeS3{pageSize: 2, objects: map[string]stored{
		"flags/a.json":        {content: "a"},
		"flags/b.json":        {content: "b"},
		"flags/nested/":       {content: ""}, // a console "folder"
		"flags/nested/c.json": {content: "c"},
		"flagship.json":       {content: "not under the prefix"},
		"other/x":             {content: "x"},
	}}
	r := resolve(t, Origin{Client: fake}, "s3://config/flags/")
	ctx, limits := context.Background(), bundle.DefaultLimits()

	first, err := r.Fetch(ctx, "", limits)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range first.Bundle.GetFiles() {
		got = append(got, f.GetPath()+"="+string(f.GetContent()))
	}
	if want := "a.json=a b.json=b nested/c.json=c"; strings.Join(got, " ") != want {
		t.Errorf("bundle = %v, want %s", got, want)
	}
	if err := bundle.Validate(first.Bundle, limits); err != nil {
		t.Errorf("the bundle must be deliverable: %v", err)
	}

	gets := fake.gets
	again, err := r.Fetch(ctx, first.Revision, limits)
	if err != nil || again.Bundle != nil || again.Revision != first.Revision || fake.gets != gets {
		t.Errorf("Fetch(known) = %+v, %v; want no bundle and no read", again, err)
	}

	// Any object changing, appearing or going changes the revision.
	fake.objects["flags/b.json"] = stored{content: "b2"}
	changed, err := r.Fetch(ctx, first.Revision, limits)
	if err != nil || changed.Bundle == nil || changed.Revision == first.Revision {
		t.Errorf("after a change: %+v, %v", changed, err)
	}
	delete(fake.objects, "flags/a.json")
	shrunk, err := r.Fetch(ctx, changed.Revision, limits)
	if err != nil || len(shrunk.Bundle.GetFiles()) != 2 || shrunk.Revision == changed.Revision {
		t.Errorf("after a removal: %+v, %v", shrunk, err)
	}

	if _, err := r.Fetch(ctx, "", bundle.Limits{MaxBytes: 1 << 20, MaxFiles: 1}); !errors.Is(err, bundle.ErrTooManyFiles) {
		t.Errorf("too many objects: %v, want ErrTooManyFiles", err)
	}
	if _, err := r.Fetch(ctx, "", bundle.Limits{MaxBytes: 2, MaxFiles: 10}); !errors.Is(err, bundle.ErrTooLarge) {
		t.Errorf("too many bytes: %v, want ErrTooLarge", err)
	}
	if _, err := resolve(t, Origin{Client: fake}, "s3://config/empty/").Fetch(ctx, "", limits); !errors.Is(err, provider.ErrNotFound) {
		t.Errorf("empty prefix: %v, want ErrNotFound", err)
	}
}

// A bundle must never mix two uploads: an object that changes after it was
// listed restarts the read.
func TestPrefixReadIsConsistent(t *testing.T) {
	fake := &fakeS3{objects: map[string]stored{
		"flags/a.json": {content: "a1"},
		"flags/b.json": {content: "b1"},
	}}
	uploaded := false
	fake.beforeGet = func(key string) {
		// While a.json is being read, a writer replaces both files.
		if key == "flags/a.json" && !uploaded {
			uploaded = true
			fake.objects["flags/a.json"] = stored{content: "a2"}
			fake.objects["flags/b.json"] = stored{content: "b2"}
		}
	}
	r := resolve(t, Origin{Client: fake}, "s3://config/flags/")
	state, err := r.Fetch(context.Background(), "", bundle.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range state.Bundle.GetFiles() {
		got = append(got, string(f.GetContent()))
	}
	if strings.Join(got, " ") != "a2 b2" {
		t.Errorf("bundle = %v, want the second upload whole", got)
	}

	// A bucket that never holds still is reported, not served half-read.
	n := 0
	fake.beforeGet = func(string) {
		n++
		fake.objects["flags/a.json"] = stored{content: fmt.Sprintf("churn-%d", n)}
	}
	if _, err := r.Fetch(context.Background(), "", bundle.DefaultLimits()); !errors.Is(err, errChanged) {
		t.Errorf("under constant change: %v, want errChanged", err)
	}
}

func TestParseCacheControl(t *testing.T) {
	if cc := parseCacheControl("no-store"); !cc.GetNoStore() || cc.HasMaxAge() {
		t.Errorf("no-store = %v", cc)
	}
	if cc := parseCacheControl(`MAX-AGE="30"`); cc.GetMaxAge().GetSeconds() != 30 {
		t.Errorf("quoted, upper-case max-age = %v", cc)
	}
	for _, header := range []string{"", "public, immutable", "max-age=soon", "max-age=-1"} {
		if cc := parseCacheControl(header); cc != nil {
			t.Errorf("parseCacheControl(%q) = %v, want nil", header, cc)
		}
	}
}

// What the origin knows about a rollout: when the revision appeared, which
// ramp applies, and whether it is held.
func TestRolloutFromTheOrigin(t *testing.T) {
	uploaded := time.Unix(1_700_000_000, 0).UTC()
	fake := &fakeS3{objects: map[string]stored{
		"team/app.yaml":  {content: "v1", modified: uploaded},
		"team/own.yaml":  {content: "v1", modified: uploaded, metadata: map[string]string{RampMetadata: "5%:1h", HoldMetadata: "2.5%"}},
		"team/typo.yaml": {content: "v1", modified: uploaded, metadata: map[string]string{RampMetadata: "five percent"}},
		"team/hold.yaml": {content: "v1", modified: uploaded, metadata: map[string]string{HoldMetadata: "lots"}},
		"flags/a.json":   {content: "a", modified: uploaded.Add(-time.Hour)},
		"flags/b.json":   {content: "b", modified: uploaded},
		"other/x":        {content: "x", modified: uploaded},
	}}
	rule, err := rollout.ParseRule("config/team/*=10%:5m")
	if err != nil {
		t.Fatal(err)
	}
	o := Origin{Client: fake, Rollouts: []rollout.Rule{rule}}
	ctx, limits := context.Background(), bundle.DefaultLimits()
	fetch := func(raw string) (provider.State, error) {
		t.Helper()
		return resolve(t, o, raw).Fetch(ctx, "", limits)
	}

	// The provider's rule, measured from the upload.
	state, err := fetch("s3://config/team/app.yaml")
	if err != nil || !state.Since.Equal(uploaded) || state.Ramp.At(0) != 1000 || state.Hold != nil {
		t.Errorf("app.yaml: %+v, %v; want the rule's ramp from the upload time", state, err)
	}
	// The object's own metadata wins, and can hold the rollout.
	state, err = fetch("s3://config/team/own.yaml")
	if err != nil || state.Ramp.At(0) != 500 || state.Hold == nil || *state.Hold != 250 {
		t.Errorf("own.yaml: %+v, %v; want its own ramp held at 2.5%%", state, err)
	}
	// Bad metadata is not delivered some other way.
	for _, raw := range []string{"s3://config/team/typo.yaml", "s3://config/team/hold.yaml"} {
		if _, err := fetch(raw); err == nil {
			t.Errorf("%s was delivered despite its metadata", raw)
		}
	}
	// A prefix is as new as its newest object; no rule matches it here.
	state, err = fetch("s3://config/flags/")
	if err != nil || !state.Since.Equal(uploaded) || state.Ramp.At(0) != rollout.Everyone {
		t.Errorf("flags/: %+v, %v; want the newest object's time and no ramp", state, err)
	}
	state, err = fetch("s3://config/other/x")
	if err != nil || state.Ramp.At(0) != rollout.Everyone {
		t.Errorf("other/x: %+v, %v; want no ramp", state, err)
	}
}
