package uri

import (
	"errors"
	"testing"
)

func TestParseCanonicalises(t *testing.T) {
	tests := []struct{ in, want string }{
		{"s3://bucket/app.yaml", "s3://bucket/app.yaml"},
		{"S3://bucket/app.yaml", "s3://bucket/app.yaml"},
		{"s3-prod://Bucket/Key", "s3-prod://Bucket/Key"},
		{"s3://bucket/a/./b/../c.yaml", "s3://bucket/a/c.yaml"},
		{"s3://bucket/../../a", "s3://bucket/a"},
		{"s3://bucket/dir/", "s3://bucket/dir/"},
		{"s3://bucket/dir/sub/..", "s3://bucket/dir/"},
		{"s3://bucket/dir/.", "s3://bucket/dir/"},
		{"s3://bucket//a///b", "s3://bucket/a/b"},
		{"s3://bucket", "s3://bucket"},
		{"s3://bucket/", "s3://bucket/"},
		{"s3://bucket/a#frag", "s3://bucket/a"},
		{"x://h/p?b=2&a=1", "x://h/p?a=1&b=2"},
		{"x://h/p?b=2&a=1&b=1", "x://h/p?a=1&b=2&b=1"},
		{"x://h/p?a=%20b", "x://h/p?a=+b"},
		{"x://h/a%20b", "x://h/a%20b"},
		{"x://h:8443/p", "x://h:8443/p"},
		{"x:///only/path", "x:///only/path"},
	}
	for _, tt := range tests {
		got, err := Parse(tt.in)
		if err != nil {
			t.Errorf("Parse(%q) error: %v", tt.in, err)
			continue
		}
		if got.String() != tt.want {
			t.Errorf("Parse(%q) = %q, want %q", tt.in, got, tt.want)
		}
		// Canonical form is a fixed point.
		again, err := Parse(got.String())
		if err != nil || again.String() != got.String() {
			t.Errorf("Parse(%q) is not idempotent: %q, %v", got, again, err)
		}
	}
}

func TestParseRejects(t *testing.T) {
	for _, in := range []string{
		"",
		"bucket/app.yaml",
		"/just/a/path",
		"s3:bucket/app.yaml",
		"mailto:someone@example.com",
		"s3://user@bucket/app.yaml",
		"s3://user:secret@bucket/app.yaml",
		"s3://bucket/%zz",
		"://bucket/a",
	} {
		if _, err := Parse(in); !errors.Is(err, ErrInvalid) {
			t.Errorf("Parse(%q) = %v, want ErrInvalid", in, err)
		}
	}
}

func TestAccessors(t *testing.T) {
	u, err := Parse("S3-Prod://acme-config/payments/flags/?region=eu")
	if err != nil {
		t.Fatal(err)
	}
	if u.Provider != "s3-prod" {
		t.Errorf("Provider = %q", u.Provider)
	}
	if u.Authority() != "acme-config" {
		t.Errorf("Authority() = %q", u.Authority())
	}
	if u.Path() != "/payments/flags/" {
		t.Errorf("Path() = %q", u.Path())
	}
	if !u.IsDir() {
		t.Error("IsDir() = false for a trailing slash")
	}
	if u.Query().Get("region") != "eu" {
		t.Errorf("Query() = %v", u.Query())
	}
	if f, _ := Parse("s3://b/file"); f.IsDir() {
		t.Error("IsDir() = true for a file")
	}
	if (URI{}).String() != "" {
		t.Error("zero URI should print empty")
	}
}

func TestLocationFollowsFromTheURI(t *testing.T) {
	for raw, want := range map[string]string{
		"s3://acme-config/payments/app.yaml": "s3/acme-config/payments/app.yaml",
		"s3://acme-config/shared/flags/":     "s3/acme-config/shared/flags",
		"file:///payments/app.yaml":          "file/payments/app.yaml",
		"FILE:///a/./b/../c.yaml":            "file/a/c.yaml",
		"file:///app.yaml?variant=eu":        "file/app.yaml",
		"file:///":                           "file",
	} {
		u, err := Parse(raw)
		if err != nil {
			t.Fatalf("Parse(%q): %v", raw, err)
		}
		if got := u.Location(); got != want {
			t.Errorf("Location(%q) = %q, want %q", raw, got, want)
		}
	}
}
