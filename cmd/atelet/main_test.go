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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ategcs"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestSnapshotStageSummary(t *testing.T) {
	started := time.Now()
	var summary snapshotStageSummary
	summary.add(ategcs.SnapshotIOResult{
		LogicalBytes:         100,
		PopulatedBytes:       95,
		StoredBytes:          40,
		WrittenBytes:         90,
		Duration:             7 * time.Millisecond,
		SparseScanDuration:   time.Millisecond,
		ExtentCopyDuration:   1500 * time.Microsecond,
		PrepareDuration:      6 * time.Millisecond,
		CompressionDuration:  2 * time.Millisecond,
		StorageWriteDuration: 3 * time.Millisecond,
		CloseDuration:        500 * time.Microsecond,
		SyncDuration:         1500 * time.Microsecond,
		PublishDuration:      4 * time.Millisecond,
	})
	summary.add(ategcs.SnapshotIOResult{
		LogicalBytes:         200,
		PopulatedBytes:       185,
		StoredBytes:          60,
		WrittenBytes:         180,
		Duration:             11 * time.Millisecond,
		SparseScanDuration:   2 * time.Millisecond,
		ExtentCopyDuration:   2500 * time.Microsecond,
		PrepareDuration:      7 * time.Millisecond,
		CompressionDuration:  3 * time.Millisecond,
		StorageWriteDuration: 4 * time.Millisecond,
		CloseDuration:        time.Millisecond,
		SyncDuration:         time.Millisecond,
		PublishDuration:      5 * time.Millisecond,
	})
	got := summary.finish(started)
	if got.FileCount != 2 || got.LogicalBytes != 300 || got.PopulatedBytes != 280 || got.StoredBytes != 100 || got.WrittenBytes != 270 {
		t.Fatalf("totals = files:%d logical:%d populated:%d stored:%d written:%d", got.FileCount, got.LogicalBytes, got.PopulatedBytes, got.StoredBytes, got.WrittenBytes)
	}
	if got.IOTime != 18*time.Millisecond {
		t.Errorf("IOTime=%v, want 18ms", got.IOTime)
	}
	if got.SparseScanDuration != 3*time.Millisecond {
		t.Errorf("SparseScanDuration=%v, want 3ms", got.SparseScanDuration)
	}
	if got.ExtentCopyDuration != 4*time.Millisecond {
		t.Errorf("ExtentCopyDuration=%v, want 4ms", got.ExtentCopyDuration)
	}
	if got.PrepareDuration != 13*time.Millisecond {
		t.Errorf("PrepareDuration=%v, want 13ms", got.PrepareDuration)
	}
	if got.CompressionDuration != 5*time.Millisecond {
		t.Errorf("CompressionDuration=%v, want 5ms", got.CompressionDuration)
	}
	if got.StorageWriteDuration != 7*time.Millisecond {
		t.Errorf("StorageWriteDuration=%v, want 7ms", got.StorageWriteDuration)
	}
	if got.CloseDuration != 1500*time.Microsecond {
		t.Errorf("CloseDuration=%v, want 1.5ms", got.CloseDuration)
	}
	if got.SyncDuration != 2500*time.Microsecond {
		t.Errorf("SyncDuration=%v, want 2.5ms", got.SyncDuration)
	}
	if got.PublishDuration != 9*time.Millisecond {
		t.Errorf("PublishDuration=%v, want 9ms", got.PublishDuration)
	}
	if got.WallTime <= 0 || got.WallTime >= got.IOTime {
		t.Errorf("WallTime=%v, want positive and independent of summed IOTime=%v", got.WallTime, got.IOTime)
	}
}

func TestDirectRestoreEligibility(t *testing.T) {
	root := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", root)
	t.Setenv("ATE_SNAPSHOT_DIRECT_RESTORE", "1")
	prefix := "gs://bucket/snapshot-1"
	dir := filepath.Join(root, "bucket", "snapshot-1")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pages.img"), []byte("pages"), 0o600); err != nil {
		t.Fatal(err)
	}
	rec := &sandboxAssetsRecord{SnapshotFormat: snapshotFormatRawSparseV1, SnapshotFiles: []string{"pages.img"}}
	got, ok, err := directRestorePath(prefix, rec)
	if err != nil || !ok || got != dir {
		t.Fatalf("directRestorePath=(%q,%v,%v), want (%q,true,nil)", got, ok, err, dir)
	}
	rec.SnapshotFormat = snapshotFormatSparseZstdV1
	if _, ok, err := directRestorePath(prefix, rec); err != nil || ok {
		t.Fatalf("compressed snapshot eligibility=(%v,%v), want false,nil", ok, err)
	}
}

func TestDirectRestoreAllowsMicroVM(t *testing.T) {
	root := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", root)
	t.Setenv("ATE_SNAPSHOT_DIRECT_RESTORE", "1")

	prefix := "gs://bucket/snapshot-1"
	dir := filepath.Join(root, "bucket", "snapshot-1")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"base-id", "config.json", "memory-ranges", "state.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	rec := &sandboxAssetsRecord{
		SandboxClass:   "microvm",
		SnapshotFormat: snapshotFormatRawSparseV1,
		SnapshotFiles:  []string{"base-id", "config.json", "memory-ranges", "state.json"},
	}

	got, ok, err := directRestorePath(prefix, rec)
	if err != nil || !ok || got != dir {
		t.Fatalf("directRestorePath=(%q,%v,%v), want (%q,true,nil)", got, ok, err, dir)
	}
}

func TestStageMicroVMDirectRestoreLinksLargeFiles(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "restore-state")
	t.Setenv("ATE_MICROVM_DIRECT_RESTORE_LINK_MIN_BYTES", "16")

	if err := os.WriteFile(filepath.Join(srcDir, "config.json"), []byte(`{"vsock":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "state.json"), []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "memory-ranges"), []byte("0123456789abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := stageMicroVMDirectRestore(dstDir, srcDir, []string{"config.json", "memory-ranges", "state.json"}); err != nil {
		t.Fatalf("stageMicroVMDirectRestore: %v", err)
	}

	if target, err := os.Readlink(filepath.Join(dstDir, "memory-ranges")); err != nil || target != filepath.Join(srcDir, "memory-ranges") {
		t.Fatalf("memory-ranges symlink=(%q,%v), want %q,nil", target, err, filepath.Join(srcDir, "memory-ranges"))
	}
	if info, err := os.Lstat(filepath.Join(dstDir, "config.json")); err != nil || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("config.json Lstat=(%v,%v), want copied regular file", info.Mode(), err)
	}
	if info, err := os.Lstat(filepath.Join(dstDir, "state.json")); err != nil || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("state.json Lstat=(%v,%v), want copied regular file", info.Mode(), err)
	}
}

func TestDirectCheckpointEligibility(t *testing.T) {
	root := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", root)
	t.Setenv("ATE_SNAPSHOT_DIRECT_CHECKPOINT", "1")
	prefix := "gs://bucket/snapshot-1"
	dir := filepath.Join(root, "bucket", "snapshot-1")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pages.img"), []byte("pages"), 0o600); err != nil {
		t.Fatal(err)
	}
	req := validCheckpointRequest()
	req.GetExternalConfig().SnapshotUriPrefix = prefix
	rec := &sandboxAssetsRecord{SnapshotFormat: snapshotFormatRawSparseV1, SnapshotFiles: []string{"pages.img"}}
	got, ok, err := directCheckpointPath(req, rec)
	if err != nil || !ok || got != dir {
		t.Fatalf("directCheckpointPath=(%q,%v,%v), want (%q,true,nil)", got, ok, err, dir)
	}

	rec.SnapshotFormat = snapshotFormatSparseZstdV1
	if _, ok, err := directCheckpointPath(req, rec); err != nil || ok {
		t.Fatalf("compressed snapshot eligibility=(%v,%v), want false,nil", ok, err)
	}
}

func TestDirectCheckpointAllowsMicroVMWhenFilesAlreadyOnSnapshotFS(t *testing.T) {
	root := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", root)
	t.Setenv("ATE_SNAPSHOT_DIRECT_CHECKPOINT", "1")

	req := validCheckpointRequest()
	req.GetExternalConfig().SnapshotUriPrefix = "gs://bucket/snapshot-1"
	dir := filepath.Join(root, "bucket", "snapshot-1")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"base-id", "config.json", "memory-ranges", "state.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	rec := &sandboxAssetsRecord{
		SandboxClass:   "microvm",
		SnapshotFormat: snapshotFormatRawSparseV1,
		SnapshotFiles:  []string{"base-id", "config.json", "memory-ranges", "state.json"},
	}

	if got, ok, err := directCheckpointPath(req, rec); err != nil || !ok || got != dir {
		t.Fatalf("directCheckpointPath=(%q,%v,%v), want (%q,true,nil)", got, ok, err, dir)
	}
}

func TestDirectCheckpointFallsBackWhenSnapshotFSMissingFiles(t *testing.T) {
	root := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", root)
	t.Setenv("ATE_SNAPSHOT_DIRECT_CHECKPOINT", "1")

	req := validCheckpointRequest()
	req.GetExternalConfig().SnapshotUriPrefix = "gs://bucket/snapshot-1"
	rec := &sandboxAssetsRecord{
		SandboxClass:   "microvm",
		SnapshotFormat: snapshotFormatRawSparseV1,
		SnapshotFiles:  []string{"base-id", "config.json", "memory-ranges", "state.json"},
	}

	got, ok, err := directCheckpointPath(req, rec)
	if err != nil || ok || got != "" {
		t.Fatalf("directCheckpointPath=(%q,%v,%v), want (\"\",false,nil)", got, ok, err)
	}
}

func TestCheckpointAlreadyOnSnapshotFS(t *testing.T) {
	root := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", root)
	prefix := "gs://bucket/snapshot-1"
	dir := filepath.Join(root, "bucket", "snapshot-1")
	ok, err := checkpointAlreadyOnSnapshotFS(prefix, dir)
	if err != nil || !ok {
		t.Fatalf("checkpointAlreadyOnSnapshotFS=(%v,%v), want true,nil", ok, err)
	}
	ok, err = checkpointAlreadyOnSnapshotFS(prefix, filepath.Join(root, "bucket", "other"))
	if err != nil || ok {
		t.Fatalf("checkpointAlreadyOnSnapshotFS mismatch=(%v,%v), want false,nil", ok, err)
	}
}

func TestUploadExternalCheckpointUsesRawChunksWhenConfigured(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", filepath.Join(dir, "snapshots"))
	t.Setenv("ATE_SNAPSHOT_RAW_CHUNK_BYTES", strconv.Itoa(2<<20))
	t.Setenv("ATE_SNAPSHOT_RAW_CHUNK_CONCURRENCY", "3")

	checkpointDir := filepath.Join(dir, "checkpoint")
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 5<<20)
	for i := range want {
		want[i] = byte((i * 19) % 251)
	}
	if err := os.WriteFile(filepath.Join(checkpointDir, "pages.img"), want, 0o600); err != nil {
		t.Fatal(err)
	}
	rec := &sandboxAssetsRecord{
		SandboxClass:   "gvisor",
		Assets:         map[string]assetEntry{"runsc": {URL: "gs://gvisor/runsc", SHA256: strings.Repeat("a", 64)}},
		SnapshotFiles:  []string{"pages.img"},
		SnapshotFormat: snapshotFormatRawSparseV1,
	}
	req := validCheckpointRequest()
	req.GetExternalConfig().SnapshotUriPrefix = "gs://bucket/snap"

	var h AteomHerder
	manifest, summary, err := h.uploadExternalCheckpoint(context.Background(), req, checkpointDir, rec)
	if err != nil {
		t.Fatalf("uploadExternalCheckpoint: %v", err)
	}
	if summary.LogicalBytes != int64(len(want)) || summary.WrittenBytes != int64(len(want)) {
		t.Fatalf("summary=%+v", summary)
	}
	var gotRec sandboxAssetsRecord
	if err := json.Unmarshal(manifest, &gotRec); err != nil {
		t.Fatal(err)
	}
	chunks := gotRec.SnapshotFileChunks["pages.img"]
	if len(chunks) != 3 {
		t.Fatalf("manifest chunks=%+v, want 3 chunks", chunks)
	}
	if diff := cmp.Diff([]string{"pages.img"}, gotRec.SnapshotFiles); diff != "" {
		t.Fatalf("snapshot files diff (-want +got):\n%s", diff)
	}

	restoreDir := filepath.Join(dir, "restore")
	if err := os.MkdirAll(restoreDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := h.downloadExternalCheckpoint(context.Background(), req.GetExternalConfig().GetSnapshotUriPrefix(), restoreDir, gotRec.SnapshotFiles, gotRec.snapshotFormat(), gotRec.SnapshotFileChunks); err != nil {
		t.Fatalf("downloadExternalCheckpoint: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(restoreDir, "pages.img"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("restored chunked checkpoint differs")
	}
}

func TestUploadExternalCheckpointRawSparseUsesObjectStoreWithoutSnapshotFS(t *testing.T) {
	checkpointDir := t.TempDir()
	want := []byte("raw checkpoint bytes")
	if err := os.WriteFile(filepath.Join(checkpointDir, "pages.img"), want, 0o600); err != nil {
		t.Fatal(err)
	}

	store := &recordingObjectStorage{objects: map[string][]byte{}}
	h := &AteomHerder{gcsClient: store}
	req := validCheckpointRequest()
	req.GetExternalConfig().SnapshotUriPrefix = "gs://bucket/snap"
	rec := &sandboxAssetsRecord{
		SandboxClass:   "microvm",
		SnapshotFiles:  []string{"pages.img"},
		SnapshotFormat: snapshotFormatRawSparseV1,
	}

	manifest, summary, err := h.uploadExternalCheckpoint(context.Background(), req, checkpointDir, rec)
	if err != nil {
		t.Fatalf("uploadExternalCheckpoint: %v", err)
	}
	if summary.LogicalBytes != int64(len(want)) || summary.StoredBytes != int64(len(want)) {
		t.Fatalf("summary=%+v", summary)
	}
	if got := store.objects["bucket/snap/pages.img"]; !bytes.Equal(got, want) {
		t.Fatalf("raw object bytes=%q, want %q", got, want)
	}
	if got := store.objects["bucket/snap/"+sandboxManifestName]; !bytes.Equal(got, manifest) {
		t.Fatalf("manifest object=%q, want returned manifest %q", got, manifest)
	}
}

func TestWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "actor-id")

	// One shared write over an existing value, as happens on every resume;
	// each subtest checks one postcondition.
	if err := os.WriteFile(target, []byte("golden-id"), 0o600); err != nil {
		t.Fatalf("seeding target: %v", err)
	}
	if err := writeFileAtomic(target, []byte("counter-1"), 0o644); err != nil {
		t.Fatalf("writeFileAtomic: %v", err)
	}

	t.Run("replaces content", func(t *testing.T) {
		got, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("reading target: %v", err)
		}
		if string(got) != "counter-1" {
			t.Errorf("content = %q, want %q", got, "counter-1")
		}
	})

	t.Run("sets permissions", func(t *testing.T) {
		info, err := os.Stat(target)
		if err != nil {
			t.Fatalf("stat target: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o644 {
			t.Errorf("perm = %o, want 644", perm)
		}
	})

	t.Run("leaves no temp files", func(t *testing.T) {
		// The directory is visible inside the actor.
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading dir: %v", err)
		}
		if len(entries) != 1 {
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}
			t.Errorf("leftover files in identity dir: %v", names)
		}
	})
}

// validRunRequest, validCheckpointRequest, and validRestoreRequest build
// requests whose every field passes validation; the per-request tests below
// break one field per case.
func validRunRequest() *ateletpb.RunRequest {
	return &ateletpb.RunRequest{
		Atespace:               "ate-demo",
		ActorName:              "counter-1",
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		TargetAteomUid:         "422938ba-8860-4983-a25d-d6bcb0a69d4e",
		Spec:                   &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}},
	}
}

func validCheckpointRequest() *ateletpb.CheckpointRequest {
	return &ateletpb.CheckpointRequest{
		Atespace:               "ate-demo",
		ActorName:              "counter-1",
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		TargetAteomUid:         "422938ba-8860-4983-a25d-d6bcb0a69d4e",
		Spec:                   &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}},
		Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.CheckpointRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
				SnapshotUriPrefix: "gs://bucket/actors/1/snapshots/2/",
			},
		},
		Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
	}
}

func validRestoreRequest() *ateletpb.RestoreRequest {
	return &ateletpb.RestoreRequest{
		Atespace:               "ate-demo",
		ActorName:              "counter-1",
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		TargetAteomUid:         "422938ba-8860-4983-a25d-d6bcb0a69d4e",
		Spec:                   &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}},
		Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.RestoreRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
				SnapshotUriPrefix: "gs://bucket/actors/1/snapshots/2/",
			},
		},
		Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
	}
}

func TestValidateRunRequest(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ateletpb.RunRequest)
		wantErr bool
	}{
		{"valid", func(*ateletpb.RunRequest) {}, false},
		{"invalid ateom uid", func(r *ateletpb.RunRequest) { r.TargetAteomUid = "../escape" }, true},
		{"invalid atespace", func(r *ateletpb.RunRequest) { r.Atespace = "../escape" }, true},
		{"invalid actor name", func(r *ateletpb.RunRequest) { r.ActorName = "../escape" }, true},
		{"invalid actor template namespace", func(r *ateletpb.RunRequest) { r.ActorTemplateNamespace = "Not_Valid" }, true},
		{"invalid actor template name", func(r *ateletpb.RunRequest) { r.ActorTemplateName = "Not_Valid" }, true},
		{"invalid container name", func(r *ateletpb.RunRequest) {
			r.Spec.Containers = []*ateletpb.Container{{Name: "../escape"}}
		}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validRunRequest()
			tc.mutate(req)
			if err := validateRunRequest(req); (err != nil) != tc.wantErr {
				t.Errorf("validateRunRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// Checkpoint and Restore must reject a bad snapshot URI prefix even when
// every common field is valid.
func TestValidateCheckpointRequest(t *testing.T) {
	makeReq := func(opts ...func(*ateletpb.CheckpointRequest)) *ateletpb.CheckpointRequest {
		r := validCheckpointRequest()
		for _, opt := range opts {
			opt(r)
		}
		return r
	}

	tests := []struct {
		name    string
		req     *ateletpb.CheckpointRequest
		wantErr bool
	}{
		{"valid", makeReq(), false},
		{"empty snapshot uri", makeReq(func(r *ateletpb.CheckpointRequest) { r.GetExternalConfig().SnapshotUriPrefix = "" }), true},
		{"bucketless snapshot uri", makeReq(func(r *ateletpb.CheckpointRequest) { r.GetExternalConfig().SnapshotUriPrefix = "relative/path" }), true},
		{"invalid ateom uid", makeReq(func(r *ateletpb.CheckpointRequest) { r.TargetAteomUid = "../escape" }), true},
		{"invalid atespace", makeReq(func(r *ateletpb.CheckpointRequest) { r.Atespace = "../escape" }), true},
		{"invalid actor name", makeReq(func(r *ateletpb.CheckpointRequest) { r.ActorName = "../escape" }), true},
		{"invalid actor template namespace", makeReq(func(r *ateletpb.CheckpointRequest) { r.ActorTemplateNamespace = "Not_Valid" }), true},
		{"invalid actor template name", makeReq(func(r *ateletpb.CheckpointRequest) { r.ActorTemplateName = "Not_Valid" }), true},
		{"invalid container name", makeReq(func(r *ateletpb.CheckpointRequest) {
			r.Spec.Containers = []*ateletpb.Container{{Name: "../escape"}}
		}), true},
		{"invalid local snapshot prefix", makeReq(func(r *ateletpb.CheckpointRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.CheckpointRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotPrefix: ""}}
		}), true},
		{"unspecified snapshot type", makeReq(func(r *ateletpb.CheckpointRequest) { r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_UNSPECIFIED }), true},
		{"unspecified snapshot scope", makeReq(func(r *ateletpb.CheckpointRequest) { r.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED }), true},
		{"invalid snapshot scope", makeReq(func(r *ateletpb.CheckpointRequest) { r.Scope = ateletpb.SnapshotScope(23) }), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateCheckpointRequest(tc.req); (err != nil) != tc.wantErr {
				t.Errorf("validateCheckpointRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateRestoreRequest(t *testing.T) {
	makeReq := func(opts ...func(*ateletpb.RestoreRequest)) *ateletpb.RestoreRequest {
		r := validRestoreRequest()
		for _, opt := range opts {
			opt(r)
		}
		return r
	}

	tests := []struct {
		name    string
		req     *ateletpb.RestoreRequest
		wantErr bool
	}{
		{"valid", makeReq(), false},
		{"empty snapshot uri", makeReq(func(r *ateletpb.RestoreRequest) { r.GetExternalConfig().SnapshotUriPrefix = "" }), true},
		{"bucketless snapshot uri", makeReq(func(r *ateletpb.RestoreRequest) { r.GetExternalConfig().SnapshotUriPrefix = "relative/path" }), true},
		{"invalid ateom uid", makeReq(func(r *ateletpb.RestoreRequest) { r.TargetAteomUid = "../escape" }), true},
		{"invalid atespace", makeReq(func(r *ateletpb.RestoreRequest) { r.Atespace = "../escape" }), true},
		{"invalid actor name", makeReq(func(r *ateletpb.RestoreRequest) { r.ActorName = "../escape" }), true},
		{"invalid actor template namespace", makeReq(func(r *ateletpb.RestoreRequest) { r.ActorTemplateNamespace = "Not_Valid" }), true},
		{"invalid actor template name", makeReq(func(r *ateletpb.RestoreRequest) { r.ActorTemplateName = "Not_Valid" }), true},
		{"invalid container name", makeReq(func(r *ateletpb.RestoreRequest) {
			r.Spec.Containers = []*ateletpb.Container{{Name: "../escape"}}
		}), true},
		{"invalid local snapshot prefix", makeReq(func(r *ateletpb.RestoreRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.RestoreRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotPrefix: ""}}
		}), true},
		{"unspecified snapshot type", makeReq(func(r *ateletpb.RestoreRequest) { r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_UNSPECIFIED }), true},
		{"unspecified snapshot scope", makeReq(func(r *ateletpb.RestoreRequest) { r.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED }), true},
		{"invalid snapshot scope", makeReq(func(r *ateletpb.RestoreRequest) { r.Scope = ateletpb.SnapshotScope(23) }), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateRestoreRequest(tc.req); (err != nil) != tc.wantErr {
				t.Errorf("validateRestoreRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestFetchAssetRejectsBadHash confirms fetchAsset validates the asset hash
// before the cache-hit os.Stat/early-return, not merely "at some point". To
// prove the ordering, it plants a real file at the exact path an invalid hash
// resolves to: a correctly-ordered fetchAsset validates first and returns an
// error, while a regression that stats first would find this file and return it
// with a nil error, failing the test. StaticFilesDir is redirected to a temp
// dir so the planted path is writable and isolated.
func TestFetchAssetRejectsBadHash(t *testing.T) {
	orig := ateompath.StaticFilesDir
	ateompath.StaticFilesDir = t.TempDir()
	t.Cleanup(func() { ateompath.StaticFilesDir = orig })

	// Invalid (8 chars, not 64) but separator-free, so it resolves to a normal
	// filename inside the temp StaticFilesDir.
	const badHash = "deadbeef"
	if err := os.WriteFile(ateompath.RunSCBinaryPath(badHash), []byte("planted"), 0o755); err != nil {
		t.Fatalf("planting cache file: %v", err)
	}

	s := &AteomHerder{}
	_, err := s.fetchAsset(context.Background(), assetEntry{SHA256: badHash})
	if err == nil {
		t.Fatal("fetchAsset returned a cache hit for an invalid hash; validation must run before the os.Stat early return")
	}
	// The error must come from the validation step, proving it ran before the
	// cache-hit stat could return the planted file.
	if !strings.Contains(err.Error(), "while validating asset hash") {
		t.Errorf("error did not come from hash validation: %v", err)
	}
}

// fakeObjectStorage serves fixed bytes for GetObject so fetchAsset can be tested.
type fakeObjectStorage struct {
	data []byte
	err  error
}

func (f fakeObjectStorage) GetObject(_ context.Context, _, _ string) (io.ReadCloser, error) {
	if f.err != nil {
		return nil, f.err
	}
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

func (fakeObjectStorage) PutObject(_ context.Context, _, _ string, _ io.Reader) error { return nil }

type recordingObjectStorage struct {
	objects map[string][]byte
}

func (s *recordingObjectStorage) GetObject(_ context.Context, bucket, object string) (io.ReadCloser, error) {
	b, ok := s.objects[bucket+"/"+object]
	if !ok {
		return nil, fmt.Errorf("object %q/%q not found", bucket, object)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (s *recordingObjectStorage) PutObject(_ context.Context, bucket, object string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.objects[bucket+"/"+object] = b
	return nil
}

// TestFetchAssetStreaming covers the streamed download: good asset cached,
// over-cap rejected, hash mismatch rejected (failures leave no cache file).
func TestFetchAssetStreaming(t *testing.T) {
	origDir, origCap := ateompath.StaticFilesDir, maxAssetBytes
	t.Cleanup(func() { ateompath.StaticFilesDir, maxAssetBytes = origDir, origCap })

	content := []byte("micro-vm kernel bytes")
	goodHash := fmt.Sprintf("%x", sha256.Sum256(content))
	const url = "gs://test-bucket/asset"

	t.Run("good asset is cached", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		path, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: goodHash})
		if err != nil {
			t.Fatalf("fetchAsset: %v", err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading cached asset: %v", err)
		}
		if !bytes.Equal(got, content) {
			t.Errorf("cached bytes = %q, want %q", got, content)
		}
	})

	t.Run("over-cap asset rejected, cache not written", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		maxAssetBytes = 4 // content is longer than this
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		_, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: goodHash})
		if err == nil {
			t.Fatal("fetchAsset accepted an over-cap asset")
		}
		if !errors.Is(err, ateerrors.ReasonInvalidSandboxAsset) {
			t.Errorf("over-cap error not tagged terminal: %v", err)
		}
		if _, err := os.Stat(ateompath.RunSCBinaryPath(goodHash)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("over-cap download left a file at the cache path (stat err = %v)", err)
		}
	})

	t.Run("hash mismatch rejected, cache not written", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		maxAssetBytes = origCap
		wrongHash := strings.Repeat("a", 64) // valid 64-hex format, wrong value
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		_, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: wrongHash})
		if err == nil {
			t.Fatal("fetchAsset accepted a hash mismatch")
		}
		if !errors.Is(err, ateerrors.ReasonInvalidSandboxAsset) {
			t.Errorf("hash-mismatch error not tagged terminal: %v", err)
		}
		if _, err := os.Stat(ateompath.RunSCBinaryPath(wrongHash)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("mismatched download left a file at the cache path (stat err = %v)", err)
		}
	})

	t.Run("missing object is terminal", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		maxAssetBytes = origCap
		// The ategcs clients tag a missing object with ReasonFailedGetExternalObject.
		notFound := fmt.Errorf("%w: no such object", ateerrors.ReasonFailedGetExternalObject)
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{err: notFound}}
		_, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: goodHash})
		if !errors.Is(err, ateerrors.ReasonFailedGetExternalObject) {
			t.Errorf("missing-object error not tagged terminal: %v", err)
		}
		if errors.Is(err, ateerrors.ReasonInvalidSandboxAsset) {
			t.Errorf("missing-object error wrongly tagged ReasonInvalidSandboxAsset: %v", err)
		}
		// The extracted (outermost) Reason drives CrashIfReason's ErrorInfo;
		// it must be the client tag, not a fetchAsset blanket wrap.
		if r, ok := errors.AsType[ateerrors.Reason](err); !ok || r != ateerrors.ReasonFailedGetExternalObject {
			t.Errorf("extracted reason = %v (ok=%v), want ReasonFailedGetExternalObject", r, ok)
		}
	})

	t.Run("malformed url is terminal", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		maxAssetBytes = origCap
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		// Invalid percent-escape: url.Parse rejects it inside ategcs.Open, which
		// tags the failure with ReasonInvalidObjectURL.
		_, err := s.fetchAsset(context.Background(), assetEntry{URL: "gs://bucket/%zz", SHA256: goodHash})
		if !errors.Is(err, ateerrors.ReasonInvalidObjectURL) {
			t.Errorf("malformed-url error not tagged terminal: %v", err)
		}
	})

	t.Run("network error stays untagged (retriable)", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		maxAssetBytes = origCap
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{err: errors.New("connection refused")}}
		_, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: goodHash})
		if err == nil {
			t.Fatal("fetchAsset accepted a failing open")
		}
		// A transient open failure must carry no Reason at all: any tag here
		// is claimed by CrashIfReason in Checkpoint/Restore and would mark a
		// recoverable actor CRASHED instead of letting the control plane retry.
		if r, ok := errors.AsType[ateerrors.Reason](err); ok {
			t.Errorf("network error wrongly tagged with reason %v: %v", r, err)
		}
		if !strings.Contains(err.Error(), "while fetching") {
			t.Errorf("open failure lost its context wrap: %v", err)
		}
	})
}

// TestRPCBoundariesReject confirms each of the three RPCs validates path inputs
// before touching its (here nil) dependencies. A traversal value must be
// rejected as InvalidArgument rather than panicking or surfacing as
// Internal. Guards against a future removal or reordering of the validation
// call at any boundary.
func TestRPCBoundariesReject(t *testing.T) {
	s := &AteomHerder{}
	ctx := context.Background()
	badUID := "../escape" // valid actor ref, invalid ateom UID
	const okAtespace, okID = "ate-demo", "counter-1"
	okSpec := &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}}

	wantInvalidArgument := func(t *testing.T, rpc string, err error) {
		t.Helper()
		if err == nil {
			t.Errorf("%s accepted an invalid target ateom UID", rpc)
			return
		}
		if code := status.Code(err); code != codes.InvalidArgument {
			t.Errorf("%s returned code %v, want InvalidArgument", rpc, code)
		}
	}

	t.Run("Run", func(t *testing.T) {
		_, err := s.Run(ctx, &ateletpb.RunRequest{
			Atespace: okAtespace, ActorName: okID,
			TargetAteomUid: badUID, Spec: okSpec,
		})
		wantInvalidArgument(t, "Run", err)
	})
	t.Run("Checkpoint", func(t *testing.T) {
		_, err := s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
			Atespace: okAtespace, ActorName: okID,
			TargetAteomUid: badUID, Spec: okSpec,
		})
		wantInvalidArgument(t, "Checkpoint", err)
	})
	t.Run("Restore", func(t *testing.T) {
		_, err := s.Restore(ctx, &ateletpb.RestoreRequest{
			Atespace: okAtespace, ActorName: okID,
			TargetAteomUid: badUID, Spec: okSpec,
		})
		wantInvalidArgument(t, "Restore", err)
	})
	t.Run("ReceiveLiveMigration", func(t *testing.T) {
		_, err := s.ReceiveLiveMigration(ctx, &ateletpb.ReceiveLiveMigrationRequest{
			Atespace: okAtespace, ActorName: okID,
			TargetAteomUid: badUID, Spec: okSpec,
			ReceiverUrl: "tcp:0.0.0.0:19000",
		})
		wantInvalidArgument(t, "ReceiveLiveMigration", err)
	})
	t.Run("SendLiveMigration", func(t *testing.T) {
		_, err := s.SendLiveMigration(ctx, &ateletpb.SendLiveMigrationRequest{
			ActorName: okID, TargetAteomUid: badUID,
			DestinationUrl: "tcp:10.0.0.2:19000",
		})
		wantInvalidArgument(t, "SendLiveMigration", err)
	})
}

func TestBuildAteomWorkloadSpecForwardsReadyz(t *testing.T) {
	in := &ateletpb.WorkloadSpec{
		PauseImage: "pause",
		Containers: []*ateletpb.Container{
			{
				Name:  "with-probe",
				Image: "main",
				Readyz: &ateletpb.Readyz{
					HttpGet: &ateletpb.HTTPGetAction{Path: "/health", Port: 8080},
				},
			},
			{
				Name: "without-probe",
			},
		},
	}
	want := &ateompb.WorkloadSpec{
		Containers: []*ateompb.Container{
			{
				Name: "with-probe",
				Readyz: &ateompb.Readyz{
					HttpGet: &ateompb.HTTPGetAction{Path: "/health", Port: 8080},
				},
			},
			{Name: "without-probe"},
		},
	}
	got := buildAteomWorkloadSpec(in)
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("buildAteomWorkloadSpec mismatch (-want +got):\n%s", diff)
	}
}

func TestIsTerminalFileErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"not exist", os.ErrNotExist, true},
		{"permission", os.ErrPermission, true},
		{"is a directory", syscall.EISDIR, true},
		{"not a directory", syscall.ENOTDIR, true},
		{"name too long", syscall.ENAMETOOLONG, true},
		{"symlink loop", syscall.ELOOP, true},
		{"read-only filesystem", syscall.EROFS, true},
		{"wrapped not exist", fmt.Errorf("while reading: %w", os.ErrNotExist), true},
		{"too many open files", syscall.EMFILE, false},
		{"stale nfs handle", syscall.ESTALE, false},
		{"try again", syscall.EAGAIN, false},
		{"io error", syscall.EIO, false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTerminalFileSystemErr(tt.err); got != tt.want {
				t.Errorf("isTerminalFileSystemErr(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
