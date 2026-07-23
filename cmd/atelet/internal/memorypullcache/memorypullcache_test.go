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

package memorypullcache

import (
	"io"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"k8s.io/utils/lru"
)

func TestIsLocalRegistry(t *testing.T) {
	tests := []struct {
		ref  string
		want bool
	}{
		{ref: "localhost/foo", want: true},
		{ref: "localhost:5001/foo", want: true},
		{ref: "127.0.0.1/foo", want: true},
		{ref: "127.0.0.1:5001/foo", want: true},
		{ref: "127.0.0.2/foo", want: true},
		{ref: "127.0.0.2:8080/foo", want: true},
		{ref: "kind-registry/foo", want: false},
		{ref: "kind-registry:5000/foo", want: false},
		{ref: "my-registry.local/foo", want: false},
		{ref: "my-registry.local:8080/foo", want: false},
		{ref: "gcr.io/foo", want: false},
		{ref: "example.com/foo", want: false},
		{ref: "foo", want: false},
		{ref: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got := isLocalRegistry(tt.ref)
			if got != tt.want {
				t.Errorf("isLocalRegistry(%q) = %v; want %v", tt.ref, got, tt.want)
			}
		})
	}
}

func TestRewriteLocalRegistry(t *testing.T) {
	c := &MemoryPullCache{
		localhostRegistryReplacement: "kind-registry:5000",
	}

	tests := []struct {
		ref  string
		want string
	}{
		{ref: "localhost/foo", want: "kind-registry:5000/foo"},
		{ref: "localhost:5001/foo", want: "kind-registry:5000/foo"},
		{ref: "localhost:8080/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.1/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.1:3000/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.2/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.2:8080/foo", want: "kind-registry:5000/foo"},
		{ref: "kind-registry/foo", want: "kind-registry/foo"},
		{ref: "kind-registry:5000/foo", want: "kind-registry:5000/foo"},
		{ref: "my-registry.local/foo", want: "my-registry.local/foo"},
		{ref: "gcr.io/foo", want: "gcr.io/foo"},
		{ref: "foo", want: "foo"},
		{ref: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got := c.rewriteLocalRegistry(tt.ref)
			if got != tt.want {
				t.Errorf("rewriteLocalRegistry(%q) = %q; want %q", tt.ref, got, tt.want)
			}
		})
	}
}

func TestDiskCacheSurvivesNewMemoryPullCache(t *testing.T) {
	dir := t.TempDir()
	digest := "sha256:0123456789abcdef"
	tarBytes := []byte("rootfs tar bytes")
	cfg := v1.Config{Entrypoint: []string{"/ko-app/counter"}}

	c1 := &MemoryPullCache{cache: lru.New(1), diskCacheDir: dir}
	if err := c1.writeDiskCache(digest, tarBytes, cfg); err != nil {
		t.Fatalf("writeDiskCache: %v", err)
	}

	c2 := &MemoryPullCache{cache: lru.New(1), diskCacheDir: dir}
	rc, gotCfg, ok, err := c2.readDiskCache(digest)
	if err != nil {
		t.Fatalf("readDiskCache: %v", err)
	}
	if !ok {
		t.Fatal("readDiskCache ok=false, want true")
	}
	defer rc.Close()
	gotTar, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(gotTar) != string(tarBytes) {
		t.Fatalf("tar bytes = %q, want %q", gotTar, tarBytes)
	}
	if gotCfg.Entrypoint[0] != cfg.Entrypoint[0] {
		t.Fatalf("config entrypoint = %v, want %v", gotCfg.Entrypoint, cfg.Entrypoint)
	}
	if _, ok := c2.cache.Get(digest); !ok {
		t.Fatal("readDiskCache did not populate memory cache")
	}
}
