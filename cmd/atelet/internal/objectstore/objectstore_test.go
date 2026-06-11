// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectstore_test

import (
	"context"
	"os"
	"testing"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/objectstore"
)

// TestParseURL covers the exported ParseURL function.
func TestParseURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		url        string
		wantBucket string
		wantObject string
		wantErr    bool
	}{
		{
			name:       "s3 scheme",
			url:        "s3://my-bucket/path/to/object",
			wantBucket: "my-bucket",
			wantObject: "path/to/object",
		},
		{
			name:       "gs scheme",
			url:        "gs://my-gcs-bucket/some/nested/path",
			wantBucket: "my-gcs-bucket",
			wantObject: "some/nested/path",
		},
		{
			name:       "s3 root object",
			url:        "s3://bucket/obj",
			wantBucket: "bucket",
			wantObject: "obj",
		},
		{
			name:       "gs empty object path",
			url:        "gs://bucket/",
			wantBucket: "bucket",
			wantObject: "",
		},
		{
			name:    "unsupported scheme",
			url:     "https://example.com/bucket/object",
			wantErr: true,
		},
		{
			name:    "no scheme",
			url:     "bucket/object",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bucket, object, err := objectstore.ParseURL(tc.url)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseURL(%q) = nil error, want error", tc.url)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseURL(%q) unexpected error: %v", tc.url, err)
			}
			if bucket != tc.wantBucket {
				t.Errorf("ParseURL(%q) bucket = %q, want %q", tc.url, bucket, tc.wantBucket)
			}
			if object != tc.wantObject {
				t.Errorf("ParseURL(%q) object = %q, want %q", tc.url, object, tc.wantObject)
			}
		})
	}
}

// TestNewClientEnvMapping verifies that ATE_OBJECTSTORE_* vars are forwarded
// to the corresponding AWS SDK env vars before the S3 client is created.
func TestNewClientEnvMapping(t *testing.T) {
	// Isolate env mutations; restore originals after test.
	restore := func(key, original string, existed bool) {
		if existed {
			os.Setenv(key, original) //nolint:errcheck
		} else {
			os.Unsetenv(key) //nolint:errcheck
		}
	}

	awsVars := []string{
		"AWS_ENDPOINT_URL_S3",
		"AWS_REGION",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"ATE_OBJECTSTORE_ENDPOINT",
		"ATE_OBJECTSTORE_REGION",
		"ATE_OBJECTSTORE_ACCESS_KEY_ID",
		"ATE_OBJECTSTORE_SECRET_ACCESS_KEY",
		"ATE_STORAGE_BACKEND",
	}
	originals := make(map[string]string, len(awsVars))
	existed := make(map[string]bool, len(awsVars))
	for _, k := range awsVars {
		v, ok := os.LookupEnv(k)
		originals[k] = v
		existed[k] = ok
		os.Unsetenv(k) //nolint:errcheck
	}
	t.Cleanup(func() {
		for _, k := range awsVars {
			restore(k, originals[k], existed[k])
		}
	})

	// Set ATE_OBJECTSTORE_* vars.
	os.Setenv("ATE_OBJECTSTORE_ENDPOINT", "https://s3.example.com")
	os.Setenv("ATE_OBJECTSTORE_REGION", "ap-northeast-1")
	os.Setenv("ATE_OBJECTSTORE_ACCESS_KEY_ID", "mykey")
	os.Setenv("ATE_OBJECTSTORE_SECRET_ACCESS_KEY", "mysecret")

	ctx := context.Background()
	// NewClient should not error (it configures via env vars; no actual network call).
	client, err := objectstore.NewClient(ctx)
	if err != nil {
		t.Fatalf("NewClient() unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("NewClient() returned nil client")
	}

	// Verify that the AWS SDK env vars were populated.
	checks := map[string]string{
		"AWS_ENDPOINT_URL_S3":    "https://s3.example.com",
		"AWS_REGION":             "ap-northeast-1",
		"AWS_ACCESS_KEY_ID":      "mykey",
		"AWS_SECRET_ACCESS_KEY":  "mysecret",
	}
	for k, want := range checks {
		if got := os.Getenv(k); got != want {
			t.Errorf("env %s = %q, want %q", k, got, want)
		}
	}
}

// TestNewClientGCSBackwardCompat verifies that ATE_STORAGE_BACKEND=gcs
// auto-configures the GCS S3-compatible endpoint when no explicit endpoint
// is set.
func TestNewClientGCSBackwardCompat(t *testing.T) {
	awsVars := []string{
		"AWS_ENDPOINT_URL_S3",
		"ATE_OBJECTSTORE_ENDPOINT",
		"ATE_STORAGE_BACKEND",
		"AWS_REGION",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
	}
	originals := make(map[string]string, len(awsVars))
	existed := make(map[string]bool, len(awsVars))
	for _, k := range awsVars {
		v, ok := os.LookupEnv(k)
		originals[k] = v
		existed[k] = ok
		os.Unsetenv(k) //nolint:errcheck
	}
	t.Cleanup(func() {
		for _, k := range awsVars {
			if existed[k] {
				os.Setenv(k, originals[k]) //nolint:errcheck
			} else {
				os.Unsetenv(k) //nolint:errcheck
			}
		}
	})

	os.Setenv("ATE_STORAGE_BACKEND", "gcs")

	ctx := context.Background()
	client, err := objectstore.NewClient(ctx)
	if err != nil {
		t.Fatalf("NewClient() with ATE_STORAGE_BACKEND=gcs: unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("NewClient() returned nil client")
	}

	// The GCS S3-compatible endpoint must be auto-configured.
	if got := os.Getenv("AWS_ENDPOINT_URL_S3"); got != "https://storage.googleapis.com" {
		t.Errorf("AWS_ENDPOINT_URL_S3 = %q, want %q", got, "https://storage.googleapis.com")
	}
}
