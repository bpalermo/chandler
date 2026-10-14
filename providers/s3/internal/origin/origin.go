// Package origin reads configuration from S3 buckets. It is the origin of the
// S3 provider: <name>://<bucket>/<key> is one object, and
// <name>://<bucket>/<prefix>/ every object below a prefix.
//
// It only reads. Whoever writes to the bucket is responsible for the validity
// of what is there.
package origin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	stevedorev1 "github.com/bpalermo/stevedore/api/stevedore/v1"
	"github.com/bpalermo/stevedore/common/bundle"
	"github.com/bpalermo/stevedore/common/provider"
	"github.com/bpalermo/stevedore/common/rollout"
	"github.com/bpalermo/stevedore/common/uri"
)

// consistentReads is how often a read is started again because an object
// changed while it was in progress.
const consistentReads = 3

// errChanged reports that an object changed between being listed (or
// inspected) and being read.
var errChanged = errors.New("the origin changed during the read")

// API is the part of the S3 client the origin uses.
type API interface {
	HeadObject(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// Origin serves objects from S3.
type Origin struct {
	Client API
	// Buckets is the buckets that may be named in a URI. Empty allows any
	// bucket the credentials can read.
	Buckets []string
	// Rollouts gives ramps to resources by "<bucket>/<key>"; a resource no
	// rule matches is released to everyone at once. A single object can
	// carry its own in its metadata, see RampMetadata.
	Rollouts []rollout.Rule
}

const (
	// RampMetadata is the user metadata of an object that sets its ramp, as
	// rollout.ParseRamp reads it: "1%:5m,10%:10m". It overrides the
	// provider's rules for that object.
	RampMetadata = "stevedore-rollout"
	// HoldMetadata is the user metadata of an object that caps the share of
	// pods that gets it, as a percentage: "10" holds the rollout at 10%.
	// Setting it is how a rollout is held at the origin.
	HoldMetadata = "stevedore-rollout-hold"
)

// Resolve implements provider.Origin. The authority is the bucket and the
// path the key; a query parameter is refused rather than ignored, so that
// decorating a URI cannot name the same object under another spelling.
func (o Origin) Resolve(u uri.URI) (provider.Resource, error) {
	bucket := u.Authority()
	key := strings.TrimPrefix(u.Path(), "/")
	switch {
	case bucket == "":
		return nil, fmt.Errorf("%w: %s names no bucket; write %s://<bucket>/<key>", provider.ErrInvalid, u, u.Provider)
	case len(u.Query()) != 0:
		return nil, fmt.Errorf("%w: %s has query parameters, which this provider does not take", provider.ErrInvalid, u)
	case key == "" && !u.IsDir():
		return nil, fmt.Errorf("%w: %s names no key; write %s://%s/ for the whole bucket", provider.ErrInvalid, u, u.Provider, bucket)
	case len(o.Buckets) > 0 && !slices.Contains(o.Buckets, bucket):
		return nil, fmt.Errorf("%w: this provider does not serve bucket %q", provider.ErrInvalid, bucket)
	}
	ramp := rollout.Match(o.Rollouts, bucket+"/"+key)
	if u.IsDir() {
		return prefix{client: o.Client, bucket: bucket, prefix: key, ramp: &ramp}, nil
	}
	return object{client: o.Client, bucket: bucket, key: key, ramp: &ramp}, nil
}

// object is a single S3 object.
type object struct {
	client API
	bucket string
	key    string
	ramp   *rollout.Ramp
}

func (o object) CacheKey() string { return o.bucket + "/" + o.key }

// Fetch implements provider.Resource. The revision is the object's version ID
// when the bucket is versioned, else its ETag. The content is read only when
// the revision is not the known one, and only if the object is still the one
// that was inspected.
func (o object) Fetch(ctx context.Context, known string, limits bundle.Limits) (provider.State, error) {
	var err error
	for range consistentReads {
		var state provider.State
		state, err = o.fetch(ctx, known, limits)
		if !errors.Is(err, errChanged) {
			return state, err
		}
	}
	return provider.State{}, fmt.Errorf("s3://%s/%s: %w", o.bucket, o.key, err)
}

func (o object) fetch(ctx context.Context, known string, limits bundle.Limits) (provider.State, error) {
	where := fmt.Sprintf("s3://%s/%s", o.bucket, o.key)
	head, err := o.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &o.bucket, Key: &o.key})
	if err != nil {
		return provider.State{}, classify(where, err)
	}
	state := provider.State{
		Revision:     revision(head.VersionId, head.ETag),
		CacheControl: parseCacheControl(aws.ToString(head.CacheControl)),
		Since:        aws.ToTime(head.LastModified),
		Ramp:         o.ramp,
	}
	// The SDK lowercases metadata keys.
	if text, ok := head.Metadata[RampMetadata]; ok {
		ramp, err := rollout.ParseRamp(text)
		if err != nil {
			// Not delivered rather than delivered some other way: the
			// version pods have stays until the metadata is fixed.
			return provider.State{}, fmt.Errorf("%s: metadata %s: %w", where, RampMetadata, err)
		}
		state.Ramp = &ramp
	}
	if text, ok := head.Metadata[HoldMetadata]; ok {
		percent, err := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSpace(text), "%"), 64)
		if err != nil || percent < 0 || percent > 100 {
			return provider.State{}, fmt.Errorf("%s: metadata %s: %q is not a percentage", where, HoldMetadata, text)
		}
		hold := uint32(percent*100 + 0.5)
		state.Hold = &hold
	}
	if state.Revision == "" {
		return provider.State{}, fmt.Errorf("%s has neither a version ID nor an ETag", where)
	}
	if state.Revision == known {
		return state, nil
	}
	if size := aws.ToInt64(head.ContentLength); size < 0 || uint64(size) > limits.MaxBytes {
		return provider.State{}, fmt.Errorf("%w: %s is %d bytes, the limit is %d", bundle.ErrTooLarge, where, size, limits.MaxBytes)
	}
	content, err := read(ctx, o.client, o.bucket, o.key, head.ETag, limits.MaxBytes)
	if err != nil {
		return provider.State{}, err
	}
	state.Bundle = stevedorev1.Bundle_builder{Files: []*stevedorev1.File{
		stevedorev1.File_builder{Path: proto.String(path.Base(o.key)), Content: content}.Build(),
	}}.Build()
	return state, nil
}

// prefix is every object below a key prefix, as one directory bundle.
type prefix struct {
	client API
	bucket string
	// prefix ends in "/", or is "" for the whole bucket.
	prefix string
	ramp   *rollout.Ramp
}

func (p prefix) CacheKey() string { return p.bucket + "/" + p.prefix }

// Fetch implements provider.Resource. The revision is a hash of the sorted
// (key, ETag) list. Each object is read on the condition that it still has
// the ETag it was listed with; if one does not, the read starts again, so a
// bundle is never a mix of two uploads.
func (p prefix) Fetch(ctx context.Context, known string, limits bundle.Limits) (provider.State, error) {
	var err error
	for range consistentReads {
		var state provider.State
		state, err = p.fetch(ctx, known, limits)
		if !errors.Is(err, errChanged) {
			return state, err
		}
	}
	return provider.State{}, fmt.Errorf("s3://%s/%s: %w", p.bucket, p.prefix, err)
}

type listed struct {
	key  string
	etag *string
}

// A prefix is rolled out from the time its newest object was written.

func (p prefix) fetch(ctx context.Context, known string, limits bundle.Limits) (provider.State, error) {
	where := fmt.Sprintf("s3://%s/%s", p.bucket, p.prefix)

	var objects []listed
	var total uint64
	var since time.Time
	var token *string
	for {
		page, err := p.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &p.bucket,
			Prefix:            &p.prefix,
			ContinuationToken: token,
		})
		if err != nil {
			return provider.State{}, classify(where, err)
		}
		for _, entry := range page.Contents {
			key := aws.ToString(entry.Key)
			if strings.HasSuffix(key, "/") {
				continue // a folder marker, not a file
			}
			if len(objects) >= limits.MaxFiles {
				return provider.State{}, fmt.Errorf("%w: %s has more than %d objects", bundle.ErrTooManyFiles, where, limits.MaxFiles)
			}
			total += uint64(max(aws.ToInt64(entry.Size), 0))
			if total > limits.MaxBytes {
				return provider.State{}, fmt.Errorf("%w: %s is more than %d bytes", bundle.ErrTooLarge, where, limits.MaxBytes)
			}
			objects = append(objects, listed{key: key, etag: entry.ETag})
			if modified := aws.ToTime(entry.LastModified); modified.After(since) {
				since = modified
			}
		}
		if !aws.ToBool(page.IsTruncated) || page.NextContinuationToken == nil {
			break
		}
		token = page.NextContinuationToken
	}
	if len(objects) == 0 {
		return provider.State{}, fmt.Errorf("%w: no object under %s", provider.ErrNotFound, where)
	}

	sort.Slice(objects, func(i, j int) bool { return objects[i].key < objects[j].key })
	hash := sha256.New()
	for _, o := range objects {
		fmt.Fprintf(hash, "%s\x00%s\n", o.key, aws.ToString(o.etag))
	}
	state := provider.State{Revision: "sha256:" + hex.EncodeToString(hash.Sum(nil)), Since: since, Ramp: p.ramp}
	if state.Revision == known {
		return state, nil
	}

	files := make([]*stevedorev1.File, 0, len(objects))
	left := limits.MaxBytes
	for _, o := range objects {
		content, err := read(ctx, p.client, p.bucket, o.key, o.etag, left)
		if errors.Is(err, provider.ErrNotFound) {
			// Removed since it was listed: the listing is stale, the
			// prefix is not gone.
			return provider.State{}, fmt.Errorf("%w: %s/%s was removed", errChanged, p.bucket, o.key)
		}
		if err != nil {
			return provider.State{}, err
		}
		left -= uint64(len(content))
		files = append(files, stevedorev1.File_builder{
			Path:    proto.String(strings.TrimPrefix(o.key, p.prefix)),
			Content: content,
		}.Build())
	}
	state.Bundle = stevedorev1.Bundle_builder{Files: files}.Build()
	return state, nil
}

// read returns the content of an object, provided it still has the given ETag
// and is no larger than limit.
func read(ctx context.Context, client API, bucket, key string, etag *string, limit uint64) ([]byte, error) {
	where := fmt.Sprintf("s3://%s/%s", bucket, key)
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key, IfMatch: etag})
	if err != nil {
		return nil, classify(where, err)
	}
	defer out.Body.Close()
	// One byte past the limit tells too large from exactly full.
	content, err := io.ReadAll(io.LimitReader(out.Body, int64(limit)+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", where, err)
	}
	if uint64(len(content)) > limit {
		return nil, fmt.Errorf("%w: %s exceeds the bundle limit", bundle.ErrTooLarge, where)
	}
	return content, nil
}

// revision returns an object's change marker: its version ID when the bucket
// is versioned, else its ETag.
func revision(versionID, etag *string) string {
	// An unversioned bucket reports no version ID, or the literal "null".
	if v := aws.ToString(versionID); v != "" && v != "null" {
		return v
	}
	return aws.ToString(etag)
}

// classify turns an S3 error into the error the provider protocol reports.
func classify(where string, err error) error {
	status := 0
	var response *smithyhttp.ResponseError
	if errors.As(err, &response) {
		status = response.HTTPStatusCode()
	}
	code := ""
	var api smithy.APIError
	if errors.As(err, &api) {
		code = api.ErrorCode()
	}
	switch {
	case status == http.StatusPreconditionFailed || code == "PreconditionFailed":
		return fmt.Errorf("%w: %s", errChanged, where)
	case status == http.StatusNotFound || code == "NoSuchKey" || code == "NoSuchBucket" || code == "NotFound":
		return fmt.Errorf("%w: %s", provider.ErrNotFound, where)
	case status == http.StatusForbidden || code == "AccessDenied":
		return fmt.Errorf("%w: %s", provider.ErrPermission, where)
	default:
		return fmt.Errorf("%s: %w", where, err)
	}
}

// parseCacheControl reads the directives Stevedore honours from an object's
// Cache-Control header. Anything else in the header is ignored. It returns
// nil when none is present, so the provider's defaults apply.
func parseCacheControl(header string) *stevedorev1.CacheControl {
	var b stevedorev1.CacheControl_builder
	found := false
	for _, directive := range strings.Split(header, ",") {
		name, value, _ := strings.Cut(strings.TrimSpace(directive), "=")
		seconds, err := strconv.ParseInt(strings.Trim(value, `"`), 10, 64)
		valid := err == nil && seconds >= 0
		switch strings.ToLower(name) {
		case "max-age":
			if valid {
				b.MaxAge, found = durationpb.New(time.Duration(seconds)*time.Second), true
			}
		case "stale-if-error":
			if valid {
				b.StaleIfError, found = durationpb.New(time.Duration(seconds)*time.Second), true
			}
		case "no-store":
			b.NoStore, found = proto.Bool(true), true
		}
	}
	if !found {
		return nil
	}
	return b.Build()
}
