//go:build linux

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
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

func TestDirectSnapshotCheckpointDirDisabled(t *testing.T) {
	t.Setenv("ATE_SNAPSHOT_DIRECT_CHECKPOINT", "")

	got, ok, err := directSnapshotCheckpointDir("gs://bucket/path/to/snapshot")
	if err != nil || ok || got != "" {
		t.Fatalf("directSnapshotCheckpointDir=(%q,%v,%v), want (\"\",false,nil)", got, ok, err)
	}
}

func TestDirectSnapshotCheckpointDirDefaultRoot(t *testing.T) {
	t.Setenv("ATE_SNAPSHOT_DIRECT_CHECKPOINT", "1")

	got, ok, err := directSnapshotCheckpointDir("gs://bucket/path/to/snapshot")
	if err != nil {
		t.Fatalf("directSnapshotCheckpointDir: %v", err)
	}
	if !ok {
		t.Fatal("directSnapshotCheckpointDir ok=false, want true")
	}
	want := filepath.Join(ateompath.BasePath, "snapshotfs", "bucket", "path/to/snapshot")
	if got != want {
		t.Fatalf("directSnapshotCheckpointDir=%q, want %q", got, want)
	}
}

func TestDirectSnapshotCheckpointDirExplicitRoot(t *testing.T) {
	root := t.TempDir()
	t.Setenv("ATE_SNAPSHOT_DIRECT_CHECKPOINT", "1")
	t.Setenv("ATE_SNAPSHOT_FS_ROOT", root)

	got, ok, err := directSnapshotCheckpointDir("gs://bucket/path/to/snapshot")
	if err != nil {
		t.Fatalf("directSnapshotCheckpointDir: %v", err)
	}
	if !ok {
		t.Fatal("directSnapshotCheckpointDir ok=false, want true")
	}
	want := filepath.Join(root, "bucket", "path/to/snapshot")
	if got != want {
		t.Fatalf("directSnapshotCheckpointDir=%q, want %q", got, want)
	}
}

func TestDirectSnapshotCheckpointDirRejectsTraversal(t *testing.T) {
	t.Setenv("ATE_SNAPSHOT_DIRECT_CHECKPOINT", "1")

	if got, ok, err := directSnapshotCheckpointDir("gs://bucket/../escape"); err == nil || ok || got != "" {
		t.Fatalf("directSnapshotCheckpointDir traversal=(%q,%v,%v), want error", got, ok, err)
	}
}
