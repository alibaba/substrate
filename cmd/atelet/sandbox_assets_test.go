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

import "testing"

func TestLegacySnapshotFormat(t *testing.T) {
	rec, err := unmarshalSandboxRecord([]byte(`{"sandboxClass":"gvisor","assets":{"runsc":{"url":"gs://gvisor/runsc","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},"snapshotFiles":["pages.img"]}`))
	if err != nil {
		t.Fatalf("unmarshalSandboxRecord: %v", err)
	}
	if got := rec.snapshotFormat(); got != snapshotFormatSparseZstdV1 {
		t.Fatalf("legacy format=%q, want %q", got, snapshotFormatSparseZstdV1)
	}
}

func TestSnapshotFormatFromEnvironment(t *testing.T) {
	t.Setenv("ATE_SNAPSHOT_FORMAT", snapshotFormatRawSparseV1)
	if got := configuredSnapshotFormat(); got != snapshotFormatRawSparseV1 {
		t.Fatalf("configuredSnapshotFormat=%q", got)
	}
	t.Setenv("ATE_SNAPSHOT_FORMAT", "unknown")
	if got := configuredSnapshotFormat(); got != snapshotFormatSparseZstdV1 {
		t.Fatalf("unknown configured format=%q, want safe default", got)
	}
}
