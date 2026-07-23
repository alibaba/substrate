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
	"time"
)

func TestRuntimeStageSummary(t *testing.T) {
	s := runtimeStageSummary{
		NetworkSetup:  3 * time.Millisecond,
		PauseRestore:  7 * time.Millisecond,
		AppRestore:    11 * time.Millisecond,
		ReadinessWait: 5 * time.Millisecond,
	}
	if got, want := s.MeasuredTime(), 26*time.Millisecond; got != want {
		t.Fatalf("MeasuredTime()=%v, want %v", got, want)
	}
}

func TestDirectSnapshotImagePath(t *testing.T) {
	root := t.TempDir()
	t.Setenv("ATE_RUNSC_SNAPSHOT_FS_ROOT", root)

	got, ok, err := directSnapshotImagePath("gs://bucket/path/to/snapshot")
	if err != nil || !ok {
		t.Fatalf("directSnapshotImagePath returned (%q,%v,%v), want ok", got, ok, err)
	}
	if want := filepath.Join(root, "bucket", "path/to/snapshot"); got != want {
		t.Fatalf("directSnapshotImagePath=%q, want %q", got, want)
	}

	if _, ok, err := directSnapshotImagePath("gs://bucket/../escape"); err == nil || ok {
		t.Fatalf("directSnapshotImagePath should reject unsafe path, got ok=%v err=%v", ok, err)
	}
}

func TestRunscRestoreBackgroundEnabled(t *testing.T) {
	t.Setenv("ATE_RUNSC_RESTORE_BACKGROUND", "")
	if !runscRestoreBackgroundEnabled() {
		t.Fatal("runscRestoreBackgroundEnabled()=false, want true by default")
	}

	t.Setenv("ATE_RUNSC_RESTORE_BACKGROUND", "false")
	if runscRestoreBackgroundEnabled() {
		t.Fatal("runscRestoreBackgroundEnabled()=true, want false")
	}

	t.Setenv("ATE_RUNSC_RESTORE_BACKGROUND", "not-bool")
	if !runscRestoreBackgroundEnabled() {
		t.Fatal("runscRestoreBackgroundEnabled()=false for invalid value, want true fallback")
	}
}
