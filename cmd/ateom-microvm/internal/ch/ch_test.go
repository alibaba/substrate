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

package ch

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSnapshotURL(t *testing.T) {
	if got, want := SnapshotURL("/var/lib/snap"), "file:///var/lib/snap"; got != want {
		t.Errorf("SnapshotURL = %q, want %q", got, want)
	}
}

// fakeCH is a stand-in cloud-hypervisor REST server on a unix socket. It records
// the requests it receives so tests can assert on method/path/body.
type fakeCH struct {
	mu       sync.Mutex
	requests []recordedReq
	srv      *http.Server
}

type recordedReq struct {
	method string
	path   string
	body   string
}

func startFakeCH(t *testing.T) (*Client, *fakeCH) {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "ch.sock")
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}

	f := &fakeCH{}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.requests = append(f.requests, recordedReq{method: r.Method, path: r.URL.Path, body: string(body)})
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	f.srv = &http.Server{Handler: mux}
	go f.srv.Serve(lis)
	t.Cleanup(func() { _ = f.srv.Close() })

	return NewClient(sockPath), f
}

func (f *fakeCH) recorded() []recordedReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]recordedReq, len(f.requests))
	copy(out, f.requests)
	return out
}

func TestClientLifecycleCalls(t *testing.T) {
	client, fake := startFakeCH(t)
	ctx := context.Background()

	if err := client.WaitReady(ctx, time.Second); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if err := client.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	snapDir := filepath.Join(t.TempDir(), "snap")
	if err := client.Snapshot(ctx, snapDir); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := client.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	reqs := fake.recorded()
	// First request is the WaitReady ping; assert the meaningful ones after it.
	var got []recordedReq
	for _, r := range reqs {
		if r.path == "/api/v1/vmm.ping" {
			continue
		}
		got = append(got, r)
	}
	want := []recordedReq{
		// Pause checks vm.info first (idempotency); the fake's empty reply is an
		// unparseable state, so Pause falls through to the actual vm.pause PUT.
		{method: http.MethodGet, path: "/api/v1/vm.info", body: ""},
		{method: http.MethodPut, path: "/api/v1/vm.pause", body: ""},
		{method: http.MethodPut, path: "/api/v1/vm.snapshot"},
		{method: http.MethodPut, path: "/api/v1/vm.resume", body: ""},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d non-ping requests %+v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i].method != want[i].method || got[i].path != want[i].path {
			t.Errorf("request %d = %s %s, want %s %s", i, got[i].method, got[i].path, want[i].method, want[i].path)
		}
	}

	// The snapshot body must carry the file:// destination URL.
	var snap snapshotConfig
	if err := json.Unmarshal([]byte(got[2].body), &snap); err != nil {
		t.Fatalf("snapshot body not JSON: %v (%q)", err, got[2].body)
	}
	if snap.DestinationURL != SnapshotURL(snapDir) {
		t.Errorf("snapshot destination_url = %q, want %q", snap.DestinationURL, SnapshotURL(snapDir))
	}
}

func TestClientMigrationCalls(t *testing.T) {
	client, fake := startFakeCH(t)
	ctx := context.Background()

	if err := client.ReceiveMigration(ctx, ReceiveMigrationOptions{
		ReceiverURL: "tcp:0.0.0.0:19000",
		TLSDir:      "/tls/dst",
		MemoryMode:  "Precopy",
	}); err != nil {
		t.Fatalf("ReceiveMigration: %v", err)
	}
	if err := client.SendMigration(ctx, SendMigrationOptions{
		DestinationURL:  "tcp:10.0.0.2:19000",
		DowntimeMillis:  200,
		TimeoutSeconds:  300,
		TimeoutStrategy: "Cancel",
		Connections:     8,
		TLSDir:          "/tls/src",
		MemoryMode:      "Precopy",
	}); err != nil {
		t.Fatalf("SendMigration: %v", err)
	}

	reqs := fake.recorded()
	want := []recordedReq{
		{method: http.MethodPut, path: "/api/v1/vm.receive-migration"},
		{method: http.MethodPut, path: "/api/v1/vm.send-migration"},
	}
	if len(reqs) != len(want) {
		t.Fatalf("got %d requests %+v, want %d", len(reqs), reqs, len(want))
	}
	for i := range want {
		if reqs[i].method != want[i].method || reqs[i].path != want[i].path {
			t.Fatalf("request %d = %s %s, want %s %s", i, reqs[i].method, reqs[i].path, want[i].method, want[i].path)
		}
	}

	var recv receiveMigrationConfig
	if err := json.Unmarshal([]byte(reqs[0].body), &recv); err != nil {
		t.Fatalf("receive migration body not JSON: %v (%q)", err, reqs[0].body)
	}
	if recv.ReceiverURL != "tcp:0.0.0.0:19000" || recv.TLSDir != "/tls/dst" || recv.MemoryMode != "Precopy" {
		t.Fatalf("receive migration body = %+v, want receiver/tls/memory mode", recv)
	}

	var send sendMigrationConfig
	if err := json.Unmarshal([]byte(reqs[1].body), &send); err != nil {
		t.Fatalf("send migration body not JSON: %v (%q)", err, reqs[1].body)
	}
	if send.DestinationURL != "tcp:10.0.0.2:19000" ||
		send.DowntimeMillis != 200 ||
		send.TimeoutSeconds != 300 ||
		send.TimeoutStrategy != "Cancel" ||
		send.Connections != 8 ||
		send.TLSDir != "/tls/src" ||
		send.MemoryMode != "Precopy" {
		t.Fatalf("send migration body = %+v, want configured migration options", send)
	}
}

func TestWaitReadyTimesOut(t *testing.T) {
	// Socket that never exists -> WaitReady should time out, not hang.
	client := NewClient(filepath.Join(t.TempDir(), "nonexistent.sock"))
	err := client.WaitReady(context.Background(), 50*time.Millisecond)
	if err == nil {
		t.Fatal("WaitReady returned nil for a dead socket, want timeout error")
	}
}
