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

package ategcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// memStore is an in-memory ObjectStorage for round-trip tests.
type memStore struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMemStore() *memStore { return &memStore{m: map[string][]byte{}} }

func (s *memStore) PutObject(_ context.Context, bucket, object string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[bucket+"/"+object] = b
	return nil
}

func (s *memStore) GetObject(_ context.Context, bucket, object string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.m[bucket+"/"+object]
	if !ok {
		return nil, fmt.Errorf("object %q/%q not found", bucket, object)
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), b...))), nil
}

// streamingMemStore is a memStore that advertises streaming PutObject support, so
// sendZstd takes the pipe (compress∥upload overlap) path used for GCS
// instead of staging a seekable temp file.
type streamingMemStore struct{ *memStore }

func (s *streamingMemStore) supportsStreamingPut() {}

// TestSparseUploadStreamingRoundTrip drives the STREAMING upload path (GCS-like
// backend) end-to-end through the real entry points: the object must still be the
// sparse-extent format (magic) and download byte-exact. This guards the pipe path
// that overlaps compression with the upload.
func TestSparseUploadStreamingRoundTrip(t *testing.T) {
	const size = 8 << 20
	want := make([]byte, size)
	regions := [][2]int{{0, 4096}, {2 << 20, 70000}, {size - 9000, 5000}}
	for _, e := range regions {
		for i := e[0]; i < e[0]+e[1]; i++ {
			want[i] = byte((i*7)%251 + 1)
		}
	}
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "memory-ranges")
	src, err := os.Create(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Truncate(size); err != nil {
		t.Fatal(err)
	}
	for _, e := range regions {
		if _, err := src.WriteAt(want[e[0]:e[0]+e[1]], int64(e[0])); err != nil {
			t.Fatal(err)
		}
	}
	src.Close()

	store := &streamingMemStore{newMemStore()}
	ctx := context.Background()
	const gsURL = "gs://bucket/snap/memory-ranges.zstd"
	if err := SendLocalFileToGCSWithZstd(ctx, store, gsURL, srcPath); err != nil {
		t.Fatalf("streaming upload: %v", err)
	}
	stored := store.m["bucket/snap/memory-ranges.zstd"]
	if len(stored) < len(sparseMagic) || string(stored[:len(sparseMagic)]) != sparseMagic {
		t.Fatalf("streaming-stored object is not sparse-extent format (magic=%q)", stored[:min(len(stored), len(sparseMagic))])
	}
	if int64(len(stored)) >= size/2 {
		t.Errorf("stored %d bytes; expected far less than logical %d (holes not skipped)", len(stored), size)
	}
	dstPath := filepath.Join(dir, "restored")
	if err := FetchLocalFileFromGCSWithZstd(ctx, store, gsURL, dstPath); err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("streaming round-trip mismatch: len(got)=%d len(want)=%d", len(got), len(want))
	}
}

func TestSnapshotFSRoundTrip(t *testing.T) {
	const size = 8 << 20
	want := make([]byte, size)
	for _, e := range [][2]int{{0, 4096}, {2 << 20, 70000}, {size - 9000, 5000}} {
		for i := e[0]; i < e[0]+e[1]; i++ {
			want[i] = byte((i*7)%251 + 1)
		}
	}
	dir := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", filepath.Join(dir, "snapshots"))
	t.Setenv("ATE_SNAPSHOT_FS_FALLBACK", "")

	srcPath := filepath.Join(dir, "memory-ranges")
	src, err := os.Create(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Truncate(size); err != nil {
		t.Fatal(err)
	}
	for _, e := range [][2]int{{0, 4096}, {2 << 20, 70000}, {size - 9000, 5000}} {
		if _, err := src.WriteAt(want[e[0]:e[0]+e[1]], int64(e[0])); err != nil {
			t.Fatal(err)
		}
	}
	src.Close()

	ctx := context.Background()
	const manifestURL = "gs://bucket/snap/sandbox-manifest.json"
	if err := SendBytesToGCS(ctx, nil, manifestURL, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("write manifest through snapshot fs: %v", err)
	}
	gotManifest, err := FetchFromGCS(ctx, nil, manifestURL)
	if err != nil {
		t.Fatalf("read manifest through snapshot fs: %v", err)
	}
	if string(gotManifest) != `{"ok":true}` {
		t.Fatalf("manifest = %q", gotManifest)
	}

	const gsURL = "gs://bucket/snap/memory-ranges.zstd"
	if err := SendLocalFileToGCSWithZstd(ctx, nil, gsURL, srcPath); err != nil {
		t.Fatalf("snapshot fs upload: %v", err)
	}
	storedPath := filepath.Join(dir, "snapshots", "bucket", "snap", "memory-ranges.zstd")
	stored, err := os.ReadFile(storedPath)
	if err != nil {
		t.Fatalf("read stored compressed snapshot: %v", err)
	}
	if len(stored) < len(sparseMagic) || string(stored[:len(sparseMagic)]) != sparseMagic {
		t.Fatalf("stored object is not sparse-extent format (magic=%q)", stored[:min(len(stored), len(sparseMagic))])
	}
	dstPath := filepath.Join(dir, "restored")
	if err := FetchLocalFileFromGCSWithZstd(ctx, nil, gsURL, dstPath); err != nil {
		t.Fatalf("snapshot fs download: %v", err)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("snapshot fs round-trip mismatch: len(got)=%d len(want)=%d", len(got), len(want))
	}
}

func TestRawSparseSnapshotFSRoundTrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", filepath.Join(dir, "snapshots"))
	srcPath := filepath.Join(dir, "source.img")
	want := make([]byte, 2<<20)
	for i := 0; i < 4096; i++ {
		want[(1<<20)+i] = byte(i%251 + 1)
	}
	if err := os.WriteFile(srcPath, want, 0o600); err != nil {
		t.Fatal(err)
	}
	const rawURL = "gs://bucket/run/pages.img"
	writeResult, err := SendLocalFileToSnapshotFSRaw(context.Background(), rawURL, srcPath)
	if err != nil {
		t.Fatalf("SendLocalFileToSnapshotFSRaw: %v", err)
	}
	if writeResult.LogicalBytes != int64(len(want)) || writeResult.StoredBytes != int64(len(want)) {
		t.Fatalf("write result=%+v", writeResult)
	}
	stored, err := SnapshotFSObjectPath(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	dstPath := filepath.Join(dir, "restored.img")
	readResult, err := FetchLocalFileFromSnapshotFSRaw(context.Background(), rawURL, dstPath)
	if err != nil {
		t.Fatalf("FetchLocalFileFromSnapshotFSRaw: %v", err)
	}
	if readResult.LogicalBytes != int64(len(want)) {
		t.Fatalf("read result=%+v", readResult)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("raw round-trip differs")
	}
	if stored == dstPath {
		t.Fatal("materialized destination unexpectedly aliases source")
	}
}

func TestRawSparseObjectRoundTrip(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.img")
	want := make([]byte, 2<<20)
	for i := 0; i < 4096; i++ {
		want[(1<<20)+i] = byte(i%251 + 1)
	}
	if err := os.WriteFile(srcPath, want, 0o600); err != nil {
		t.Fatal(err)
	}
	store := newMemStore()
	const rawURL = "gs://bucket/run/pages.img"
	writeResult, err := SendLocalFileToGCSRawResult(context.Background(), store, rawURL, srcPath)
	if err != nil {
		t.Fatalf("SendLocalFileToGCSRawResult: %v", err)
	}
	if writeResult.LogicalBytes != int64(len(want)) || writeResult.StoredBytes != int64(len(want)) {
		t.Fatalf("write result=%+v", writeResult)
	}
	dstPath := filepath.Join(dir, "restored.img")
	readResult, err := FetchLocalFileFromGCSRaw(context.Background(), store, rawURL, dstPath)
	if err != nil {
		t.Fatalf("FetchLocalFileFromGCSRaw: %v", err)
	}
	if readResult.LogicalBytes != int64(len(want)) {
		t.Fatalf("read result=%+v", readResult)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("raw object round-trip differs")
	}
}

func TestSnapshotFSCopyUsesConfiguredLargeBuffer(t *testing.T) {
	t.Setenv("ATE_SNAPSHOT_FS_COPY_BUFFER_BYTES", strconv.Itoa(1<<20))
	reader := &maxReadSizeReader{remaining: 2 << 20}
	n, err := copySnapshotFSFile(io.Discard, reader)
	if err != nil {
		t.Fatalf("copySnapshotFSFile: %v", err)
	}
	if n != 2<<20 {
		t.Fatalf("copied bytes=%d, want %d", n, 2<<20)
	}
	if reader.maxReadSize != 1<<20 {
		t.Fatalf("max read size=%d, want %d", reader.maxReadSize, 1<<20)
	}
}

type maxReadSizeReader struct {
	remaining   int
	maxReadSize int
}

func (r *maxReadSizeReader) Read(p []byte) (int, error) {
	if len(p) > r.maxReadSize {
		r.maxReadSize = len(p)
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	clear(p[:n])
	r.remaining -= n
	return n, nil
}

func TestRawSnapshotFSChunkedRoundTrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", filepath.Join(dir, "snapshots"))
	srcPath := filepath.Join(dir, "source.img")
	want := make([]byte, 5<<20)
	for i := range want {
		want[i] = byte((i * 17) % 251)
	}
	if err := os.WriteFile(srcPath, want, 0o600); err != nil {
		t.Fatal(err)
	}

	const rawURL = "gs://bucket/run/pages.img"
	chunks, writeResult, err := SendLocalFileToSnapshotFSRawChunked(context.Background(), rawURL, srcPath, 2<<20, 3)
	if err != nil {
		t.Fatalf("SendLocalFileToSnapshotFSRawChunked: %v", err)
	}
	if len(chunks) != 3 {
		t.Fatalf("chunk count=%d, want 3", len(chunks))
	}
	if chunks[0].Name != "pages.img.parts/00000000" || chunks[0].Offset != 0 || chunks[0].Size != 2<<20 {
		t.Fatalf("first chunk=%+v", chunks[0])
	}
	if chunks[2].Name != "pages.img.parts/00000002" || chunks[2].Offset != 4<<20 || chunks[2].Size != 1<<20 {
		t.Fatalf("last chunk=%+v", chunks[2])
	}
	if writeResult.LogicalBytes != int64(len(want)) || writeResult.WrittenBytes != int64(len(want)) {
		t.Fatalf("write result=%+v", writeResult)
	}
	for _, chunk := range chunks {
		path, err := SnapshotFSObjectPath("gs://bucket/run/" + chunk.Name)
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat chunk %q: %v", chunk.Name, err)
		}
		if info.Size() != chunk.Size {
			t.Fatalf("chunk %q size=%d, want %d", chunk.Name, info.Size(), chunk.Size)
		}
	}

	dstPath := filepath.Join(dir, "restored.img")
	readResult, err := FetchLocalFileFromSnapshotFSRawChunked(context.Background(), rawURL, dstPath, chunks, 3)
	if err != nil {
		t.Fatalf("FetchLocalFileFromSnapshotFSRawChunked: %v", err)
	}
	if readResult.LogicalBytes != int64(len(want)) || readResult.WrittenBytes != int64(len(want)) {
		t.Fatalf("read result=%+v", readResult)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("chunked raw round-trip differs")
	}
}

func TestRawObjectChunkedRoundTrip(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.img")
	want := make([]byte, 5<<20)
	for i := range want {
		want[i] = byte((i * 19) % 251)
	}
	if err := os.WriteFile(srcPath, want, 0o600); err != nil {
		t.Fatal(err)
	}

	store := newMemStore()
	const rawURL = "gs://bucket/run/pages.img"
	chunks, writeResult, err := SendLocalFileToGCSRawChunked(context.Background(), store, rawURL, srcPath, 2<<20, 3)
	if err != nil {
		t.Fatalf("SendLocalFileToGCSRawChunked: %v", err)
	}
	if len(chunks) != 3 {
		t.Fatalf("chunk count=%d, want 3", len(chunks))
	}
	if chunks[0].Name != "pages.img.parts/00000000" || chunks[0].Offset != 0 || chunks[0].Size != 2<<20 {
		t.Fatalf("first chunk=%+v", chunks[0])
	}
	if chunks[2].Name != "pages.img.parts/00000002" || chunks[2].Offset != 4<<20 || chunks[2].Size != 1<<20 {
		t.Fatalf("last chunk=%+v", chunks[2])
	}
	if writeResult.LogicalBytes != int64(len(want)) || writeResult.WrittenBytes != int64(len(want)) {
		t.Fatalf("write result=%+v", writeResult)
	}
	for _, chunk := range chunks {
		key := "bucket/run/" + chunk.Name
		if got := int64(len(store.m[key])); got != chunk.Size {
			t.Fatalf("chunk %q size=%d, want %d", key, got, chunk.Size)
		}
	}

	dstPath := filepath.Join(dir, "restored.img")
	readResult, err := FetchLocalFileFromGCSRawChunked(context.Background(), store, rawURL, dstPath, chunks, 3)
	if err != nil {
		t.Fatalf("FetchLocalFileFromGCSRawChunked: %v", err)
	}
	if readResult.LogicalBytes != int64(len(want)) || readResult.WrittenBytes != int64(len(want)) {
		t.Fatalf("read result=%+v", readResult)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("chunked raw object round-trip differs")
	}
}

func TestRawSnapshotFSDirectWriteBypassesRename(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", filepath.Join(dir, "snapshots"))
	t.Setenv("ATE_SNAPSHOT_FS_RAW_DIRECT_WRITE", "1")

	oldRename := snapshotFSRename
	t.Cleanup(func() {
		snapshotFSRename = oldRename
	})
	snapshotFSRename = func(_, _ string) error {
		return syscall.ENOENT
	}

	srcPath := filepath.Join(dir, "source.img")
	want := bytes.Repeat([]byte("raw direct snapshot fs\n"), 4096)
	if err := os.WriteFile(srcPath, want, 0o600); err != nil {
		t.Fatal(err)
	}

	const rawURL = "gs://bucket/run/pages.img"
	writeResult, err := SendLocalFileToSnapshotFSRaw(context.Background(), rawURL, srcPath)
	if err != nil {
		t.Fatalf("SendLocalFileToSnapshotFSRaw direct: %v", err)
	}
	if writeResult.LogicalBytes != int64(len(want)) || writeResult.StoredBytes != int64(len(want)) {
		t.Fatalf("write result=%+v", writeResult)
	}
	storedPath := filepath.Join(dir, "snapshots", "bucket", "run", "pages.img")
	got, err := os.ReadFile(storedPath)
	if err != nil {
		t.Fatalf("direct raw snapshot fs object was not written: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("direct raw snapshot fs content differs")
	}
}

func TestRawSnapshotFSMkdirAllExistingDirectoryErrorIsTolerated(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "snapshots")
	targetDir := filepath.Join(root, "bucket", "run")
	if err := os.MkdirAll(targetDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", root)
	t.Setenv("ATE_SNAPSHOT_FS_RAW_DIRECT_WRITE", "1")

	oldMkdirAll := snapshotFSMkdirAll
	t.Cleanup(func() {
		snapshotFSMkdirAll = oldMkdirAll
	})
	snapshotFSMkdirAll = func(path string, perm os.FileMode) error {
		if path == targetDir {
			return syscall.EEXIST
		}
		return os.MkdirAll(path, perm)
	}

	srcPath := filepath.Join(dir, "source.img")
	want := bytes.Repeat([]byte("raw direct existing dir\n"), 4096)
	if err := os.WriteFile(srcPath, want, 0o600); err != nil {
		t.Fatal(err)
	}

	const rawURL = "gs://bucket/run/pages.img"
	if _, err := SendLocalFileToSnapshotFSRaw(context.Background(), rawURL, srcPath); err != nil {
		t.Fatalf("SendLocalFileToSnapshotFSRaw should tolerate existing-dir EEXIST: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(targetDir, "pages.img"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("direct raw snapshot fs content differs")
	}
}

func TestRawSnapshotFSMkdirAllIntermediateExistsErrorContinuesCreating(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "snapshots")
	if err := os.MkdirAll(filepath.Join(root, "bucket"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", root)
	t.Setenv("ATE_SNAPSHOT_FS_RAW_DIRECT_WRITE", "1")

	oldMkdirAll := snapshotFSMkdirAll
	t.Cleanup(func() {
		snapshotFSMkdirAll = oldMkdirAll
	})
	snapshotFSMkdirAll = func(path string, perm os.FileMode) error {
		if path == filepath.Join(root, "bucket", "run", "nested") {
			return syscall.EEXIST
		}
		return os.MkdirAll(path, perm)
	}

	srcPath := filepath.Join(dir, "source.img")
	want := bytes.Repeat([]byte("raw direct nested dir\n"), 4096)
	if err := os.WriteFile(srcPath, want, 0o600); err != nil {
		t.Fatal(err)
	}

	const rawURL = "gs://bucket/run/nested/pages.img"
	if _, err := SendLocalFileToSnapshotFSRaw(context.Background(), rawURL, srcPath); err != nil {
		t.Fatalf("SendLocalFileToSnapshotFSRaw should continue after intermediate EEXIST: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, "bucket", "run", "nested", "pages.img"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("direct raw snapshot fs content differs")
	}
}

func TestRawSnapshotFSMkdirAllToleratesRacyIntermediateExist(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "snapshots")
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", root)
	t.Setenv("ATE_SNAPSHOT_FS_RAW_DIRECT_WRITE", "1")

	oldMkdirAll := snapshotFSMkdirAll
	oldMkdir := snapshotFSMkdir
	oldStat := snapshotFSStat
	t.Cleanup(func() {
		snapshotFSMkdirAll = oldMkdirAll
		snapshotFSMkdir = oldMkdir
		snapshotFSStat = oldStat
	})

	bucketDir := filepath.Join(root, "bucket")
	snapshotFSMkdirAll = func(string, os.FileMode) error {
		return syscall.EEXIST
	}
	snapshotFSStat = func(path string) (os.FileInfo, error) {
		if path == bucketDir {
			return nil, os.ErrNotExist
		}
		return os.Stat(path)
	}
	snapshotFSMkdir = func(path string, perm os.FileMode) error {
		if path == bucketDir {
			if err := os.MkdirAll(path, perm); err != nil {
				return err
			}
			return syscall.EEXIST
		}
		return os.Mkdir(path, perm)
	}

	srcPath := filepath.Join(dir, "source.img")
	want := bytes.Repeat([]byte("raw direct racy mkdir\n"), 4096)
	if err := os.WriteFile(srcPath, want, 0o600); err != nil {
		t.Fatal(err)
	}

	const rawURL = "gs://bucket/run/nested/pages.img"
	if _, err := SendLocalFileToSnapshotFSRaw(context.Background(), rawURL, srcPath); err != nil {
		t.Fatalf("SendLocalFileToSnapshotFSRaw should tolerate racy intermediate EEXIST: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, "bucket", "run", "nested", "pages.img"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("direct raw snapshot fs content differs")
	}
}

func TestSnapshotFSRejectsTraversal(t *testing.T) {
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", t.TempDir())
	_, _, _, err := snapshotFSPath("gs://bucket/../escape")
	if err == nil {
		t.Fatal("expected traversal path to be rejected")
	}
}

func TestSnapshotFSReadRetriesUntilObjectAppears(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", filepath.Join(dir, "snapshots"))
	t.Setenv("ATE_SNAPSHOT_FS_FALLBACK", "")

	_, _, path, err := snapshotFSPath("gs://bucket/snap/sandbox-manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = os.WriteFile(path, []byte(`{"visible":true}`), 0o600)
	}()

	got, err := FetchFromGCS(context.Background(), nil, "gs://bucket/snap/sandbox-manifest.json")
	if err != nil {
		t.Fatalf("FetchFromGCS should retry until the snapshot is visible: %v", err)
	}
	if string(got) != `{"visible":true}` {
		t.Fatalf("FetchFromGCS = %q", got)
	}
}

func TestSnapshotFSRenameRetriesTransientMissingSource(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "tmp")
	dst := filepath.Join(dir, "published")
	if err := os.WriteFile(src, []byte("snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}

	oldRename := snapshotFSRename
	oldDelays := snapshotFSRenameRetryDelays
	t.Cleanup(func() {
		snapshotFSRename = oldRename
		snapshotFSRenameRetryDelays = oldDelays
	})
	snapshotFSRenameRetryDelays = []time.Duration{0, 0, 0}
	attempts := 0
	snapshotFSRename = func(oldpath, newpath string) error {
		attempts++
		if attempts < 3 {
			return syscall.ENOENT
		}
		return os.Rename(oldpath, newpath)
	}

	if err := renameSnapshotFSFile(src, dst); err != nil {
		t.Fatalf("renameSnapshotFSFile: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d, want 3", attempts)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "snapshot" {
		t.Fatalf("published content=%q", got)
	}
}

func TestSnapshotFSZstdDirectWriteBypassesRename(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", filepath.Join(dir, "snapshots"))
	t.Setenv("ATE_SNAPSHOT_FS_ZSTD_DIRECT_WRITE", "1")

	oldRename := snapshotFSRename
	t.Cleanup(func() {
		snapshotFSRename = oldRename
	})
	snapshotFSRename = func(_, _ string) error {
		return syscall.ENOENT
	}

	srcPath := filepath.Join(dir, "memory-ranges")
	want := bytes.Repeat([]byte("direct snapshot fs zstd\n"), 4096)
	if err := os.WriteFile(srcPath, want, 0o600); err != nil {
		t.Fatal(err)
	}

	const gsURL = "gs://bucket/snap/pages.img.zstd"
	if err := SendLocalFileToGCSWithZstd(context.Background(), nil, gsURL, srcPath); err != nil {
		t.Fatalf("direct snapshot fs upload: %v", err)
	}
	storedPath := filepath.Join(dir, "snapshots", "bucket", "snap", "pages.img.zstd")
	if _, err := os.Stat(storedPath); err != nil {
		t.Fatalf("direct snapshot fs object was not written: %v", err)
	}
	dstPath := filepath.Join(dir, "restored")
	if err := FetchLocalFileFromGCSWithZstd(context.Background(), nil, gsURL, dstPath); err != nil {
		t.Fatalf("direct snapshot fs download: %v", err)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("direct snapshot fs round-trip differs")
	}
}

func TestSnapshotFSZstdObjectWriteBypassesSnapshotFS(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", filepath.Join(dir, "snapshots"))
	t.Setenv("ATE_SNAPSHOT_FS_FALLBACK", "object")
	t.Setenv("ATE_SNAPSHOT_FS_ZSTD_OBJECT_WRITE", "1")

	oldRename := snapshotFSRename
	t.Cleanup(func() {
		snapshotFSRename = oldRename
	})
	snapshotFSRename = func(_, _ string) error {
		return syscall.ENOENT
	}

	srcPath := filepath.Join(dir, "pages.img")
	want := bytes.Repeat([]byte("object snapshot fs zstd\n"), 4096)
	if err := os.WriteFile(srcPath, want, 0o600); err != nil {
		t.Fatal(err)
	}

	store := newMemStore()
	const gsURL = "gs://bucket/snap/pages.img.zstd"
	if err := SendLocalFileToGCSWithZstd(context.Background(), store, gsURL, srcPath); err != nil {
		t.Fatalf("object snapshot upload: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "snapshots", "bucket", "snap", "pages.img.zstd")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("snapshot fs object exists err=%v, want not exist", err)
	}
	dstPath := filepath.Join(dir, "restored")
	if err := FetchLocalFileFromGCSWithZstd(context.Background(), store, gsURL, dstPath); err != nil {
		t.Fatalf("object snapshot download: %v", err)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("object snapshot round-trip differs")
	}
}

// TestSparseUploadDownloadRoundTrip drives the real upload+download entry points
// through an in-memory store: a genuinely sparse source file (multiple data extents
// separated by holes) must upload in the sparse-extent format (magic) and download
// byte-exact AND sparse on disk. This is the guest memory image, so correctness is
// non-negotiable.
func TestSparseUploadDownloadRoundTrip(t *testing.T) {
	const size = 8 << 20 // 8 MiB logical
	want := make([]byte, size)
	// Three populated extents at varied offsets/sizes (aligned + unaligned), the
	// rest holes — mirrors scattered resident pages in free RAM.
	fill := func(start, n int) {
		for i := start; i < start+n; i++ {
			want[i] = byte((i*7)%251 + 1) // never zero
		}
	}
	fill(0, 4096)         // first page
	fill(2<<20, 70000)    // interior, crosses 64KiB boundaries
	fill(size-9000, 5000) // near end, leaving a trailing hole

	// Write the source as a genuinely sparse file (holes between the extents).
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "memory-ranges")
	src, err := os.Create(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Truncate(size); err != nil {
		t.Fatal(err)
	}
	for _, e := range [][2]int{{0, 4096}, {2 << 20, 70000}, {size - 9000, 5000}} {
		if _, err := src.WriteAt(want[e[0]:e[0]+e[1]], int64(e[0])); err != nil {
			t.Fatal(err)
		}
	}
	src.Close()

	store := newMemStore()
	ctx := context.Background()
	const gsURL = "gs://bucket/snap/memory-ranges.zstd"
	if err := SendLocalFileToGCSWithZstd(ctx, store, gsURL, srcPath); err != nil {
		t.Fatalf("upload: %v", err)
	}
	// The stored object must use the sparse-extent format (magic header).
	stored := store.m["bucket/snap/memory-ranges.zstd"]
	if len(stored) < len(sparseMagic) || string(stored[:len(sparseMagic)]) != sparseMagic {
		t.Fatalf("stored object is not sparse-extent format (magic=%q)", stored[:min(len(stored), len(sparseMagic))])
	}
	// The compressed object must be far smaller than the logical size (holes skipped).
	if int64(len(stored)) >= size/2 {
		t.Errorf("stored %d bytes; expected far less than logical %d (holes not skipped)", len(stored), size)
	}

	dstPath := filepath.Join(dir, "restored")
	if err := FetchLocalFileFromGCSWithZstd(ctx, store, gsURL, dstPath); err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip mismatch: len(got)=%d len(want)=%d", len(got), len(want))
	}
	if fi, err := os.Stat(dstPath); err == nil {
		if blk := diskBlocks(fi); blk > 0 {
			t.Logf("restored sparse: apparent=%d actual=%d", size, blk*512)
			if blk*512 >= int64(size) {
				t.Logf("note: restored file not sparse on this fs — correctness still holds")
			}
		}
	}
}

// TestSparseVersionRejected confirms the reader refuses a sparse snapshot whose
// format version it doesn't understand (rather than misparsing it). This is the
// guest memory image, so a future incompatible layout must fail loudly.
func TestSparseVersionRejected(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "memory-ranges")
	f, err := os.Create(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(1 << 20); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("hello sparse"), 4096); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, _, _, _, err := writeSparseZstd(&buf, f); err != nil {
		t.Fatalf("writeSparseZstd: %v", err)
	}
	f.Close()

	blob := buf.Bytes()
	// The version is the little-endian uint32 immediately after the 8-byte magic;
	// corrupt it so it no longer matches sparseVersion.
	blob[len(sparseMagic)] ^= 0xFF

	out, err := os.Create(filepath.Join(dir, "out"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	// readSparseZstd is called with the reader positioned just after the magic.
	if _, _, err := readSparseZstd(out, bytes.NewReader(blob[len(sparseMagic):])); err == nil {
		t.Fatal("expected an unsupported-version error, got nil")
	}
}

// TestWriteSparseZstdSkipsHoles asserts the encoder feeds ONLY the populated
// extents to zstd (not the whole logical image). Gated on the fs actually reporting
// the source as sparse (ext4/Linux does; macOS/APFS in CI dev may not — skipped there).
func TestWriteSparseZstdSkipsHoles(t *testing.T) {
	const size = 8 << 20
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "memory-ranges")
	src, err := os.Create(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Truncate(size); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 70000)
	for i := range data {
		data[i] = byte(i%251 + 1)
	}
	if _, err := src.WriteAt(data, 2<<20); err != nil { // one ~70KB extent in a sea of holes
		t.Fatal(err)
	}
	if err := src.Sync(); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(srcPath); err != nil {
		t.Fatal(err)
	} else if blk := diskBlocks(fi); blk == 0 || blk*512 >= int64(size) {
		t.Skipf("fs did not make the source sparse (actual=%d, logical=%d) — can't assert hole-skipping here", blk*512, size)
	}
	if _, err := src.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	logical, dataBytes, _, _, err := writeSparseZstd(&buf, src)
	if err != nil {
		t.Fatalf("writeSparseZstd: %v", err)
	}
	if logical != size {
		t.Errorf("logical=%d, want %d", logical, size)
	}
	if dataBytes >= int64(size)/2 {
		t.Errorf("dataBytes=%d fed to zstd; expected ~70KB (holes not skipped)", dataBytes)
	}
	t.Logf("fed %d data bytes for a %d logical image (holes skipped)", dataBytes, size)
}

// TestPlainZstdBackwardCompatRoundTrip drives the NON-file upload path (plain zstd,
// no magic) and confirms the download auto-detects + restores it — i.e. snapshots
// written before the sparse-extent format still restore.
func TestPlainZstdBackwardCompatRoundTrip(t *testing.T) {
	want := bytes.Repeat([]byte("agent-substrate snapshot payload\n"), 4096)
	store := newMemStore()
	ctx := context.Background()
	const gsURL = "gs://bucket/snap/config.json.zstd"
	// SendBytesToGCS is uncompressed; use sendZstd with a non-file reader to
	// hit the plain-zstd branch (no magic).
	if _, err := sendZstd(ctx, store, gsURL, bytes.NewReader(want)); err != nil {
		t.Fatalf("upload: %v", err)
	}
	stored := store.m["bucket/snap/config.json.zstd"]
	if len(stored) >= len(sparseMagic) && string(stored[:len(sparseMagic)]) == sparseMagic {
		t.Fatal("non-file upload unexpectedly used the sparse-extent format")
	}
	dir := t.TempDir()
	dstPath := filepath.Join(dir, "config.json")
	if err := FetchLocalFileFromGCSWithZstd(ctx, store, gsURL, dstPath); err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("plain round-trip mismatch: len(got)=%d len(want)=%d", len(got), len(want))
	}
}

// diskBlocks returns the number of 512-byte blocks the file occupies on disk (for
// the sparseness check), or 0 if unavailable on this platform/fs.
func diskBlocks(fi os.FileInfo) int64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return int64(st.Blocks)
	}
	return 0
}

// TestCopyZstdSparse checks the sparse decompress is byte-exact (it's the guest
// memory image — corruption = a dead guest) and actually punches holes for the
// zero regions.
func TestCopyZstdSparse(t *testing.T) {
	// A mostly-zero image with non-zero data at the start, an interior block, the
	// tail-but-not-end, and aligned + unaligned sizes. Mirrors a guest memory-ranges:
	// scattered resident pages in a sea of zero (free) RAM.
	const size = 4 << 20 // 4 MiB
	want := make([]byte, size)
	for i := 0; i < 4096; i++ { // first page
		want[i] = byte(i%251 + 1)
	}
	for i := 1 << 20; i < (1<<20)+9000; i++ { // interior, crosses a 64KiB block boundary
		want[i] = byte(i%253 + 1)
	}
	for i := size - 5000; i < size-1000; i++ { // near the end, leaving a trailing zero hole
		want[i] = byte(i%249 + 1)
	}

	dir := t.TempDir()
	out := filepath.Join(dir, "memory-ranges")
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	size64, written, err := copyZstdSparse(f, bytes.NewReader(want))
	if err != nil {
		t.Fatalf("copyZstdSparse: %v", err)
	}
	if size64 != int64(len(want)) {
		t.Errorf("logical size = %d, want %d", size64, len(want))
	}
	if written >= int64(len(want)) {
		t.Errorf("written %d bytes; expected far less than %d for a mostly-zero image (not sparse)", written, len(want))
	}

	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip mismatch: len(got)=%d len(want)=%d", len(got), len(want))
	}

	// Best-effort sparseness check: the file should occupy fewer 512-byte blocks than
	// its apparent size (holes punched). fs-dependent, so only log if unavailable.
	if fi, err := os.Stat(out); err == nil {
		if st := diskBlocks(fi); st > 0 {
			actual := st * 512
			t.Logf("sparse: apparent=%d actual=%d written=%d", len(want), actual, written)
			if actual >= int64(len(want)) {
				t.Logf("note: file not sparse on this fs (actual=%d >= apparent=%d) — correctness still holds", actual, len(want))
			}
		}
	}
}

// TestWriteDecodeContentRoundTrip exercises the io-only compress/decompress halves
// (writeContent / decodeContent) directly — no object store — for both the sparse
// (file source) and plain (non-file reader) paths.
func TestWriteDecodeContentRoundTrip(t *testing.T) {
	dir := t.TempDir()

	t.Run("sparse file", func(t *testing.T) {
		const size = 8 << 20
		srcPath := filepath.Join(dir, "src")
		src, err := os.Create(srcPath)
		if err != nil {
			t.Fatal(err)
		}
		defer src.Close()
		if err := src.Truncate(size); err != nil {
			t.Fatal(err)
		}
		data := make([]byte, 70000)
		for i := range data {
			data[i] = byte(i%251 + 1)
		}
		if _, err := src.WriteAt(data, 2<<20); err != nil { // one extent in a sea of holes
			t.Fatal(err)
		}
		if err := src.Sync(); err != nil {
			t.Fatal(err)
		}
		if _, err := src.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		wres, err := writeContent(&buf, src)
		if err != nil {
			t.Fatalf("writeContent: %v", err)
		}
		if !wres.sparse {
			t.Error("writeContent: sparse=false for a file source")
		}
		if wres.logicalBytes != size {
			t.Errorf("writeContent logicalBytes=%d, want %d", wres.logicalBytes, size)
		}

		dstPath := filepath.Join(dir, "dst")
		dst, err := os.Create(dstPath)
		if err != nil {
			t.Fatal(err)
		}
		defer dst.Close()
		dres, err := decodeContent(dst, &buf)
		if err != nil {
			t.Fatalf("decodeContent: %v", err)
		}
		if !dres.sparse {
			t.Error("decodeContent: sparse=false for a sparse-extent stream")
		}
		if dres.logicalBytes != size {
			t.Errorf("decodeContent logicalBytes=%d, want %d", dres.logicalBytes, size)
		}

		want, err := os.ReadFile(srcPath)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(dstPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("sparse round-trip mismatch: len(got)=%d len(want)=%d", len(got), len(want))
		}
	})

	t.Run("plain reader", func(t *testing.T) {
		want := bytes.Repeat([]byte("substrate payload\n"), 1000)
		var buf bytes.Buffer
		wres, err := writeContent(&buf, bytes.NewReader(want))
		if err != nil {
			t.Fatalf("writeContent: %v", err)
		}
		if wres.sparse {
			t.Error("writeContent: sparse=true for a non-file reader")
		}
		var out bytes.Buffer
		if _, err := decodeContent(&out, &buf); err != nil {
			t.Fatalf("decodeContent: %v", err)
		}
		if !bytes.Equal(out.Bytes(), want) {
			t.Fatalf("plain round-trip mismatch: len(got)=%d len(want)=%d", out.Len(), len(want))
		}
	})
}

func TestSnapshotIOResult(t *testing.T) {
	want := bytes.Repeat([]byte("snapshot-stage-evidence\n"), 4096)
	var encoded bytes.Buffer
	encodeResult, err := writeContentWithResult(&encoded, bytes.NewReader(want))
	if err != nil {
		t.Fatalf("writeContentWithResult: %v", err)
	}
	if encodeResult.LogicalBytes != int64(len(want)) {
		t.Errorf("LogicalBytes=%d, want %d", encodeResult.LogicalBytes, len(want))
	}
	if encodeResult.PopulatedBytes != int64(len(want)) {
		t.Errorf("PopulatedBytes=%d, want %d", encodeResult.PopulatedBytes, len(want))
	}
	if encodeResult.StoredBytes != int64(encoded.Len()) || encodeResult.StoredBytes <= 0 {
		t.Errorf("StoredBytes=%d, encoded=%d", encodeResult.StoredBytes, encoded.Len())
	}
	if encodeResult.Duration <= 0 {
		t.Errorf("Duration=%v, want positive", encodeResult.Duration)
	}
	if encodeResult.CompressionDuration <= 0 {
		t.Errorf("CompressionDuration=%v, want positive", encodeResult.CompressionDuration)
	}
	if encodeResult.StorageWriteDuration <= 0 {
		t.Errorf("StorageWriteDuration=%v, want positive", encodeResult.StorageWriteDuration)
	}
	if encodeResult.ExtentCopyDuration != 0 {
		t.Errorf("ExtentCopyDuration=%v, want zero for non-file input", encodeResult.ExtentCopyDuration)
	}

	var decoded bytes.Buffer
	decodeResult, err := decodeContentWithResult(&decoded, &encoded)
	if err != nil {
		t.Fatalf("decodeContentWithResult: %v", err)
	}
	if decodeResult.LogicalBytes != int64(len(want)) {
		t.Errorf("decode LogicalBytes=%d, want %d", decodeResult.LogicalBytes, len(want))
	}
	if decodeResult.StoredBytes != encodeResult.StoredBytes {
		t.Errorf("decode StoredBytes=%d, want %d", decodeResult.StoredBytes, encodeResult.StoredBytes)
	}
	if decodeResult.WrittenBytes != int64(len(want)) {
		t.Errorf("decode WrittenBytes=%d, want %d", decodeResult.WrittenBytes, len(want))
	}
	if decodeResult.Duration <= 0 {
		t.Errorf("decode Duration=%v, want positive", decodeResult.Duration)
	}
	if !bytes.Equal(decoded.Bytes(), want) {
		t.Fatal("decoded payload differs")
	}
}

func TestSnapshotIOResultReportsSparseExtentCopy(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "memory-ranges")
	writeSparseSource(t, srcPath, 8<<20, []region{{off: 2 << 20, len: 1 << 20, fill: 0x55}})
	src, err := os.Open(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	var encoded bytes.Buffer
	encodeResult, err := writeContentWithResult(&encoded, src)
	if err != nil {
		t.Fatalf("writeContentWithResult: %v", err)
	}
	if !encodeResult.Sparse {
		t.Fatal("Sparse=false, want true")
	}
	if encodeResult.ExtentCopyDuration <= 0 {
		t.Errorf("ExtentCopyDuration=%v, want positive for sparse file input", encodeResult.ExtentCopyDuration)
	}
}

func TestSnapshotDecoderConcurrency(t *testing.T) {
	t.Setenv("ATE_SNAPSHOT_DECODER_CONCURRENCY", "4")
	if got := snapshotDecoderConcurrency(); got != 4 {
		t.Fatalf("snapshotDecoderConcurrency=%d, want 4", got)
	}
	t.Setenv("ATE_SNAPSHOT_DECODER_CONCURRENCY", "invalid")
	if got := snapshotDecoderConcurrency(); got != 1 {
		t.Fatalf("invalid decoder concurrency=%d, want safe default 1", got)
	}
}

// TestCopyZstdSparseClearsStaleData exercises the defensive dst.Truncate(0): a dst
// that already holds bytes — here larger than and different from the new content —
// must come out byte-exact, with the would-be holes reading back as zero (not the
// stale bytes) and the file shrunk to the new logical size.
func TestCopyZstdSparseClearsStaleData(t *testing.T) {
	const size = 2 << 20
	want := make([]byte, size)
	for i := 0; i < 4096; i++ { // first page
		want[i] = byte(i%251 + 1)
	}
	for i := 1 << 20; i < (1<<20)+4096; i++ { // an interior page, rest stays a hole
		want[i] = byte(i%253 + 1)
	}

	out := filepath.Join(t.TempDir(), "dst")
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// Pre-fill with stale non-zero bytes, larger than the new content, so the test
	// also covers shrinking to the exact logical size.
	stale := make([]byte, size+size/2)
	for i := range stale {
		stale[i] = 0xFF
	}
	if _, err := f.Write(stale); err != nil {
		t.Fatal(err)
	}

	if _, _, err := copyZstdSparse(f, bytes.NewReader(want)); err != nil {
		t.Fatalf("copyZstdSparse: %v", err)
	}

	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("stale data not cleared / wrong size: len(got)=%d len(want)=%d", len(got), len(want))
	}
}
