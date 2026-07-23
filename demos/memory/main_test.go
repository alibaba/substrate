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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMemoryProfiles(t *testing.T) {
	const size = 64 << 10
	a := make([]byte, size)
	b := make([]byte, size)
	touchMemory(a, "dense-random", 42)
	touchMemory(b, "dense-random", 42)
	if !bytes.Equal(a, b) {
		t.Fatal("dense-random is not deterministic for a fixed seed")
	}
	nonzero := 0
	for _, v := range a {
		if v != 0 {
			nonzero++
		}
	}
	if nonzero < size*99/100 {
		t.Fatalf("dense-random populated only %d/%d bytes", nonzero, size)
	}

	sparse := make([]byte, size)
	touchMemory(sparse, "sparse-randomish", 42)
	sparseNonzero := 0
	for _, v := range sparse {
		if v != 0 {
			sparseNonzero++
		}
	}
	if sparseNonzero > 2*(size/4096) {
		t.Fatalf("sparse-randomish populated %d bytes", sparseNonzero)
	}
}

func TestMigrationStateAndQuiesceHandlers(t *testing.T) {
	ready.Store(true)
	targetBytes = 4096
	resident = make([]byte, targetBytes)
	checksum = "checksum-for-test"
	t.Setenv("INSTANCE_ID", "instance-a")
	t.Setenv("POD_NAME", "pod-a")
	t.Setenv("NODE_NAME", "node-a")

	state := newAppState("sparse-randomish", 42)
	server := httptest.NewServer(newMux(state))
	defer server.Close()

	payload := postJSON(t, server.URL+"/increment")
	if got := int64(payload["counter"].(float64)); got != 1 {
		t.Fatalf("counter after increment=%d, want 1", got)
	}
	if payload["checksum"] != "checksum-for-test" {
		t.Fatalf("checksum=%v, want checksum-for-test", payload["checksum"])
	}
	if payload["instance_id"] != "instance-a" || payload["pod_name"] != "pod-a" || payload["node_name"] != "node-a" {
		t.Fatalf("unexpected instance metadata: %+v", payload)
	}

	_ = postJSON(t, server.URL+"/substrate/quiesce")
	resp, err := http.Post(server.URL+"/increment", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST /increment while quiesced: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("POST /increment while quiesced status=%d, want 503", resp.StatusCode)
	}

	payload = postJSON(t, server.URL+"/substrate/resume")
	if payload["quiesced"].(bool) {
		t.Fatalf("quiesced=true after resume")
	}
	payload = getJSON(t, server.URL+"/substrate/migration-state")
	if got := int64(payload["counter"].(float64)); got != 1 {
		t.Fatalf("counter after failed increment=%d, want 1", got)
	}
}

func postJSON(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s status=%d, want 200", url, resp.StatusCode)
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode %s response: %v", url, err)
	}
	return payload
}

func getJSON(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status=%d, want 200", url, resp.StatusCode)
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode %s response: %v", url, err)
	}
	return payload
}
