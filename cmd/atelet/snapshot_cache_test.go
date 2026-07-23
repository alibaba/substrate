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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

func TestSnapshotCacheConfigurationIsExplicitAndBounded(t *testing.T) {
	t.Setenv("ATE_SNAPSHOT_CACHE_MAX_BYTES", "")
	if got := configuredSnapshotCache(); got != nil {
		t.Fatalf("cache enabled without an explicit byte limit: %+v", got)
	}
	t.Setenv("ATE_SNAPSHOT_CACHE_MAX_BYTES", "1048576")
	got := configuredSnapshotCache()
	if got == nil || got.maxBytes != 1048576 || got.root != ateompath.SnapshotCacheDir() {
		t.Fatalf("configured cache = %+v", got)
	}
	t.Setenv("ATE_SNAPSHOT_CACHE_MAX_BYTES", "invalid")
	if got := configuredSnapshotCache(); got != nil {
		t.Fatalf("cache enabled with invalid limit: %+v", got)
	}
}

func TestSnapshotCachePublishesAndFindsImmutableEntry(t *testing.T) {
	root := t.TempDir()
	cache := newSnapshotCache(filepath.Join(root, "cache"), filepath.Join(root, "actors"), 1<<20)
	source := filepath.Join(root, "checkpoint")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "checkpoint.img"), []byte("decoded snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := []byte(`{"snapshotFiles":["checkpoint.img"],"snapshotFormat":"sparse-zstd-v1"}`)

	published, err := cache.publish("gs://bucket/actor/snapshot-1", source, []string{"checkpoint.img"}, manifest)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !published {
		t.Fatal("publish reported no cache entry")
	}
	if _, err := os.Stat(filepath.Join(source, "checkpoint.img")); !os.IsNotExist(err) {
		t.Fatalf("source file still exists after same-filesystem publication: %v", err)
	}

	entry, hit, err := cache.lookup("gs://bucket/actor/snapshot-1", []string{"checkpoint.img"}, manifest)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if !hit {
		t.Fatal("lookup missed published entry")
	}
	got, err := os.ReadFile(filepath.Join(entry, "checkpoint.img"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "decoded snapshot" {
		t.Fatalf("cached data = %q", got)
	}

	otherSource := filepath.Join(root, "other-checkpoint")
	if err := os.MkdirAll(otherSource, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(otherSource, "checkpoint.img"), []byte("must not replace"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.publish("gs://bucket/actor/snapshot-1", otherSource, []string{"checkpoint.img"}, manifest); err != nil {
		t.Fatalf("republish immutable entry: %v", err)
	}
	got, err = os.ReadFile(filepath.Join(entry, "checkpoint.img"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "decoded snapshot" {
		t.Fatalf("immutable entry was replaced: %q", got)
	}
}

func TestSnapshotCacheRejectsUnsafeOrIncompleteEntries(t *testing.T) {
	root := t.TempDir()
	cache := newSnapshotCache(filepath.Join(root, "cache"), filepath.Join(root, "actors"), 1<<20)
	source := filepath.Join(root, "checkpoint")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "outside.img"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.publish("gs://bucket/snapshot", source, []string{"../outside.img"}, []byte("manifest")); err == nil {
		t.Fatal("publish accepted path traversal")
	}

	entryDir := cache.entryPath("gs://bucket/incomplete")
	if err := os.MkdirAll(entryDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(entryDir, "checkpoint.img"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, hit, err := cache.lookup("gs://bucket/incomplete", []string{"checkpoint.img"}, []byte("manifest")); err != nil || hit {
		t.Fatalf("incomplete lookup = hit %t, err %v; want clean miss", hit, err)
	}
}

func TestSnapshotCacheEvictsOldestUnreferencedEntryOnly(t *testing.T) {
	root := t.TempDir()
	cache := newSnapshotCache(filepath.Join(root, "cache"), filepath.Join(root, "actors"), 10)
	publish := func(uri, contents string) string {
		t.Helper()
		source := filepath.Join(root, filepath.Base(uri))
		if err := os.MkdirAll(source, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(source, "checkpoint.img"), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		manifest := []byte(`{"snapshotFiles":["checkpoint.img"]}`)
		if _, err := cache.publish(uri, source, []string{"checkpoint.img"}, manifest); err != nil {
			t.Fatalf("publish %s: %v", uri, err)
		}
		return cache.entryPath(uri)
	}

	first := publish("gs://bucket/first", "123456")
	restoreDir := filepath.Join(root, "actors", "space:actor", "restore-state")
	if err := os.MkdirAll(filepath.Dir(restoreDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(first, restoreDir); err != nil {
		t.Fatal(err)
	}
	refs, err := cache.referencedEntries()
	if err != nil {
		t.Fatalf("scan referenced entries: %v", err)
	}
	if !refs[first] {
		t.Fatalf("restore-state symlink target %q was not recognized as referenced: %v", first, refs)
	}
	second := publish("gs://bucket/second", "abcdef")
	if _, err := os.Stat(first); err != nil {
		t.Fatalf("referenced oldest entry was evicted: %v", err)
	}
	if _, err := os.Stat(second); !os.IsNotExist(err) {
		t.Fatalf("new entry should be rejected when only victim is referenced: %v", err)
	}

	if err := os.Remove(restoreDir); err != nil {
		t.Fatal(err)
	}
	third := publish("gs://bucket/third", "uvwxyz")
	if _, err := os.Stat(first); !os.IsNotExist(err) {
		t.Fatalf("oldest unreferenced entry was not evicted: %v", err)
	}
	if _, err := os.Stat(third); err != nil {
		t.Fatalf("new entry missing after eviction: %v", err)
	}
}

func TestSnapshotCacheLinksRestoreOnHitAndPreservesDirectoryOnMiss(t *testing.T) {
	root := t.TempDir()
	cache := newSnapshotCache(filepath.Join(root, "cache"), filepath.Join(root, "actors"), 1<<20)
	restoreDir := filepath.Join(root, "actors", "space:actor", "restore-state")
	if err := os.MkdirAll(restoreDir, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := []byte(`{"snapshotFiles":["checkpoint.img"]}`)
	if hit, err := cache.linkRestore("gs://bucket/missing", restoreDir, []string{"checkpoint.img"}, manifest); err != nil || hit {
		t.Fatalf("missing cache link = hit %t, err %v", hit, err)
	}
	if info, err := os.Stat(restoreDir); err != nil || !info.IsDir() {
		t.Fatalf("cache miss damaged restore directory: info=%v err=%v", info, err)
	}

	source := filepath.Join(root, "checkpoint")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "checkpoint.img"), []byte("snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	if published, err := cache.publish("gs://bucket/hit", source, []string{"checkpoint.img"}, manifest); err != nil || !published {
		t.Fatalf("publish = %t, %v", published, err)
	}
	if hit, err := cache.linkRestore("gs://bucket/hit", restoreDir, []string{"checkpoint.img"}, manifest); err != nil || !hit {
		t.Fatalf("cache link = hit %t, err %v", hit, err)
	}
	info, err := os.Lstat(restoreDir)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("restore path is not a cache symlink: info=%v err=%v", info, err)
	}
}
