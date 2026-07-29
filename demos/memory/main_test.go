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
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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
	t.Setenv("BOOT_ID", "boot-a")
	t.Setenv("POD_NAME", "pod-a")
	t.Setenv("NODE_NAME", "node-a")

	state := newAppState("sparse-randomish", 42)
	server := httptest.NewServer(newMux(state))
	defer server.Close()

	payload := postJSON(t, server.URL+"/increment")
	if got := int64(payload["counter"].(float64)); got != 1 {
		t.Fatalf("counter after increment=%d, want 1", got)
	}
	if got := int64(payload["state_version"].(float64)); got != 1 {
		t.Fatalf("state_version after increment=%d, want 1", got)
	}
	if payload["last_mutation_id"] != "increment-1" {
		t.Fatalf("last_mutation_id=%v, want increment-1", payload["last_mutation_id"])
	}
	if payload["checksum"] != "checksum-for-test" {
		t.Fatalf("checksum=%v, want checksum-for-test", payload["checksum"])
	}
	if payload["instance_id"] != "instance-a" || payload["boot_id"] != "boot-a" || payload["pod_name"] != "pod-a" || payload["node_name"] != "node-a" {
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
	if got := int64(payload["state_version"].(float64)); got != 3 {
		t.Fatalf("state_version after quiesce/resume=%d, want 3", got)
	}
	payload = getJSON(t, server.URL+"/substrate/migration-state")
	if got := int64(payload["counter"].(float64)); got != 1 {
		t.Fatalf("counter after failed increment=%d, want 1", got)
	}
	if payload["last_mutation_id"] != "increment-1" {
		t.Fatalf("last_mutation_id after failed increment=%v, want increment-1", payload["last_mutation_id"])
	}
}

func TestIncrementIdempotencyKeyAppliesOnce(t *testing.T) {
	ready.Store(true)
	targetBytes = 4096
	resident = make([]byte, targetBytes)
	checksum = "checksum-for-test"

	state := newAppState("sparse-randomish", 42)
	server := httptest.NewServer(newMux(state))
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/increment", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("NewRequest /increment: %v", err)
	}
	req.Header.Set("Idempotency-Key", "mutation-a")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("POST /increment with idempotency key: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first POST status=%d, want 200", resp.StatusCode)
	}
	var first map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&first); err != nil {
		t.Fatalf("decode first POST response: %v", err)
	}
	resp.Body.Close()
	if got := int64(first["counter"].(float64)); got != 1 {
		t.Fatalf("first POST response counter=%d, want 1", got)
	}
	if got := int64(first["state_version"].(float64)); got != 1 {
		t.Fatalf("first POST response state_version=%d, want 1", got)
	}

	req, err = http.NewRequest(http.MethodPost, server.URL+"/increment", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("NewRequest duplicate /increment: %v", err)
	}
	req.Header.Set("Idempotency-Key", "mutation-a")
	resp, err = server.Client().Do(req)
	if err != nil {
		t.Fatalf("duplicate POST /increment with idempotency key: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("duplicate POST status=%d, want 200", resp.StatusCode)
	}
	var duplicate map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&duplicate); err != nil {
		t.Fatalf("decode duplicate POST response: %v", err)
	}
	resp.Body.Close()
	if got := int64(duplicate["counter"].(float64)); got != 1 {
		t.Fatalf("duplicate POST response counter=%d, want 1", got)
	}
	if got := int64(duplicate["state_version"].(float64)); got != 1 {
		t.Fatalf("duplicate POST response state_version=%d, want 1", got)
	}

	req, err = http.NewRequest(http.MethodPost, server.URL+"/increment", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("NewRequest second key /increment: %v", err)
	}
	req.Header.Set("Idempotency-Key", "mutation-b")
	resp, err = server.Client().Do(req)
	if err != nil {
		t.Fatalf("POST /increment with second idempotency key: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("second key POST status=%d, want 200", resp.StatusCode)
	}
	var second map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&second); err != nil {
		t.Fatalf("decode second key POST response: %v", err)
	}
	resp.Body.Close()
	if got := int64(second["counter"].(float64)); got != 2 {
		t.Fatalf("second key POST response counter=%d, want 2", got)
	}
	if got := int64(second["state_version"].(float64)); got != 2 {
		t.Fatalf("second key POST response state_version=%d, want 2", got)
	}

	payload := getJSON(t, server.URL+"/substrate/migration-state")
	if got := int64(payload["counter"].(float64)); got != 2 {
		t.Fatalf("counter after duplicate idempotency key and second key=%d, want 2", got)
	}
	if got := int64(payload["state_version"].(float64)); got != 2 {
		t.Fatalf("state_version after duplicate idempotency key and second key=%d, want 2", got)
	}
}

func TestStreamHandlerEmitsSSEStateFrames(t *testing.T) {
	ready.Store(true)
	targetBytes = 4096
	resident = make([]byte, targetBytes)
	checksum = "checksum-for-test"
	t.Setenv("INSTANCE_ID", "instance-a")
	t.Setenv("BOOT_ID", "boot-a")
	t.Setenv("POD_NAME", "pod-a")
	t.Setenv("NODE_NAME", "node-a")

	state := newAppState("sparse-randomish", 42)
	server := httptest.NewServer(newMux(state))
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/stream", nil)
	if err != nil {
		t.Fatalf("NewRequest /stream: %v", err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("GET /stream: %v", err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
		t.Fatalf("Content-Type=%q, want text/event-stream", got)
	}

	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read event line: %v", err)
	}
	if !strings.HasPrefix(line, "event: state") {
		t.Fatalf("event line=%q, want state event", line)
	}
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read data line: %v", err)
		}
		if strings.HasPrefix(line, "data: ") {
			break
		}
	}
	var frame map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(line), "data: ")), &frame); err != nil {
		t.Fatalf("decode SSE frame: %v", err)
	}
	if got := int64(frame["sequence"].(float64)); got != 1 {
		t.Fatalf("sequence=%d, want 1", got)
	}
	if frame["checksum"] != "checksum-for-test" || frame["boot_id"] != "boot-a" || frame["instance_id"] != "instance-a" {
		t.Fatalf("unexpected SSE state frame: %+v", frame)
	}
}

func TestStreamHandlerContinuesSequenceAcrossConnections(t *testing.T) {
	ready.Store(true)
	targetBytes = 4096
	resident = make([]byte, targetBytes)
	checksum = "checksum-for-test"

	state := newAppState("sparse-randomish", 42)
	server := httptest.NewServer(newMux(state))
	defer server.Close()

	first := readFirstSSESequence(t, server.URL+"/stream")
	second := readFirstSSESequence(t, server.URL+"/stream")

	if first != 1 {
		t.Fatalf("first stream sequence=%d, want 1", first)
	}
	if second != 2 {
		t.Fatalf("second stream sequence=%d, want 2; reconnect must not restart at 1", second)
	}
}

func TestStreamHandlerReplaysFramesAfterLastEventID(t *testing.T) {
	ready.Store(true)
	targetBytes = 4096
	resident = make([]byte, targetBytes)
	checksum = "checksum-for-test"

	state := newAppState("sparse-randomish", 42)
	var sink bytes.Buffer
	if !state.writeStreamFrame(&sink, noopFlusher{}) {
		t.Fatal("write stream frame 1 failed")
	}
	if !state.writeStreamFrame(&sink, noopFlusher{}) {
		t.Fatal("write stream frame 2 failed")
	}
	if !state.writeStreamFrame(&sink, noopFlusher{}) {
		t.Fatal("write stream frame 3 failed")
	}
	server := httptest.NewServer(newMux(state))
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/stream", nil)
	if err != nil {
		t.Fatalf("NewRequest /stream: %v", err)
	}
	req.Header.Set("Last-Event-ID", "1")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("GET /stream: %v", err)
	}
	defer resp.Body.Close()

	got := readSSESequences(t, resp.Body, 3)
	want := []int64{2, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replayed stream sequences=%v, want %v", got, want)
	}
}

func TestStreamFrameSequenceDoesNotAdvanceWhenWriteFails(t *testing.T) {
	ready.Store(true)
	targetBytes = 4096
	resident = make([]byte, targetBytes)
	checksum = "checksum-for-test"

	state := newAppState("sparse-randomish", 42)
	if state.writeStreamFrame(failingWriter{}, noopFlusher{}) {
		t.Fatal("writeStreamFrame with failing writer returned true, want false")
	}

	var buf bytes.Buffer
	if !state.writeStreamFrame(&buf, noopFlusher{}) {
		t.Fatal("writeStreamFrame with buffer returned false, want true")
	}
	var frame map[string]any
	for _, line := range strings.Split(buf.String(), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &frame); err != nil {
			t.Fatalf("decode SSE frame: %v", err)
		}
	}
	if frame == nil {
		t.Fatalf("no SSE data frame written: %q", buf.String())
	}
	if got := int64(frame["sequence"].(float64)); got != 1 {
		t.Fatalf("sequence after failed write=%d, want 1", got)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

type noopFlusher struct{}

func (noopFlusher) Flush() {}

func TestSlowHandlerReturnsStateProofAfterDelay(t *testing.T) {
	ready.Store(true)
	targetBytes = 4096
	resident = make([]byte, targetBytes)
	checksum = "checksum-for-test"
	t.Setenv("BOOT_ID", "boot-a")

	state := newAppState("sparse-randomish", 42)
	server := httptest.NewServer(newMux(state))
	defer server.Close()

	start := time.Now()
	payload := getJSON(t, server.URL+"/slow?duration_ms=25")
	if time.Since(start) < 20*time.Millisecond {
		t.Fatalf("/slow returned too quickly")
	}
	if payload["scenario"] != "slow_get" {
		t.Fatalf("scenario=%v, want slow_get", payload["scenario"])
	}
	if payload["checksum"] != "checksum-for-test" || payload["boot_id"] != "boot-a" {
		t.Fatalf("missing state proof in /slow response: %+v", payload)
	}
}

func readFirstSSESequence(t *testing.T, url string) int64 {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest /stream: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /stream: %v", err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE line: %v", err)
		}
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var frame map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(line), "data: ")), &frame); err != nil {
			t.Fatalf("decode SSE frame: %v", err)
		}
		return int64(frame["sequence"].(float64))
	}
}

func readSSESequences(t *testing.T, r io.Reader, count int) []int64 {
	t.Helper()
	reader := bufio.NewReader(r)
	var got []int64
	for len(got) < count {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE line: %v", err)
		}
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var frame map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(line), "data: ")), &frame); err != nil {
			t.Fatalf("decode SSE frame: %v", err)
		}
		got = append(got, int64(frame["sequence"].(float64)))
	}
	return got
}

func TestDownloadHandlerStreamsRequestedBytes(t *testing.T) {
	ready.Store(true)
	targetBytes = 4096
	resident = make([]byte, targetBytes)
	checksum = "checksum-for-test"

	state := newAppState("sparse-randomish", 42)
	server := httptest.NewServer(newMux(state))
	defer server.Close()

	resp, err := http.Get(server.URL + "/download?bytes=1024&chunk_bytes=128")
	if err != nil {
		t.Fatalf("GET /download: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/download status=%d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Substrate-Download-Bytes"); got != "1024" {
		t.Fatalf("X-Substrate-Download-Bytes=%q, want 1024", got)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /download body: %v", err)
	}
	if len(body) != 1024 {
		t.Fatalf("/download bytes=%d, want 1024", len(body))
	}
}

func TestUploadHandlerReadsBodyAndIsIdempotent(t *testing.T) {
	ready.Store(true)
	targetBytes = 4096
	resident = make([]byte, targetBytes)
	checksum = "checksum-for-test"

	state := newAppState("sparse-randomish", 42)
	server := httptest.NewServer(newMux(state))
	defer server.Close()

	postUpload := func(key, body string) map[string]any {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, server.URL+"/upload", strings.NewReader(body))
		if err != nil {
			t.Fatalf("NewRequest /upload: %v", err)
		}
		req.Header.Set("Idempotency-Key", key)
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatalf("POST /upload: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("/upload status=%d, want 200", resp.StatusCode)
		}
		var payload map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			t.Fatalf("decode /upload response: %v", err)
		}
		return payload
	}

	first := postUpload("upload-a", "abcdef")
	duplicate := postUpload("upload-a", "abcdef")
	second := postUpload("upload-b", "ghij")
	if got := int64(first["counter"].(float64)); got != 1 {
		t.Fatalf("first counter=%d, want 1", got)
	}
	if got := int64(duplicate["counter"].(float64)); got != 1 {
		t.Fatalf("duplicate counter=%d, want 1", got)
	}
	if got := int64(second["counter"].(float64)); got != 2 {
		t.Fatalf("second counter=%d, want 2", got)
	}
	if got := int64(first["upload_bytes"].(float64)); got != 6 {
		t.Fatalf("upload_bytes=%d, want 6", got)
	}
}

func TestDirtyMemoryControlMutatesResidentState(t *testing.T) {
	ready.Store(true)
	targetBytes = 4 << 20
	resident = make([]byte, targetBytes)
	checksum = "checksum-for-test"

	state := newAppState("zeroed", 42)
	server := httptest.NewServer(newMux(state))
	defer server.Close()
	defer state.stopDirtyWorker()

	payload := postJSON(t, server.URL+"/dirty/start?rate_mib_per_s=64")
	if !payload["dirty_active"].(bool) {
		t.Fatalf("dirty_active=false after /dirty/start: %+v", payload)
	}
	before := int64(payload["dirty_bytes_total"].(float64))
	time.Sleep(120 * time.Millisecond)
	after := getJSON(t, server.URL+"/substrate/migration-state")
	afterBytes := int64(after["dirty_bytes_total"].(float64))
	if afterBytes <= before {
		t.Fatalf("dirty_bytes_total did not increase: before=%d after=%d", before, afterBytes)
	}

	payload = postJSON(t, server.URL+"/dirty/stop")
	if payload["dirty_active"].(bool) {
		t.Fatalf("dirty_active=true after /dirty/stop: %+v", payload)
	}
}

func TestTerminalWebSocketTicksAndEchoesInput(t *testing.T) {
	ready.Store(true)
	targetBytes = 4096
	resident = make([]byte, targetBytes)
	checksum = "checksum-for-test"
	t.Setenv("BOOT_ID", "boot-a")
	t.Setenv("INSTANCE_ID", "instance-a")

	state := newAppState("sparse-randomish", 42)
	server := httptest.NewServer(newMux(state))
	defer server.Close()

	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	u.Scheme = "ws"
	u.Path = "/terminal/ws"
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("dial /terminal/ws: %v", err)
	}
	defer conn.Close()

	var first map[string]any
	if err := conn.ReadJSON(&first); err != nil {
		t.Fatalf("read first terminal frame: %v", err)
	}
	if first["type"] != "tick" {
		t.Fatalf("first frame type=%v, want tick: %+v", first["type"], first)
	}
	if first["boot_id"] != "boot-a" || first["instance_id"] != "instance-a" {
		t.Fatalf("missing terminal state proof: %+v", first)
	}

	if err := conn.WriteMessage(websocket.TextMessage, []byte("echo client-seq=1")); err != nil {
		t.Fatalf("write terminal input: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read terminal frame after input: %v", err)
		}
		if frame["type"] == "echo" && frame["input"] == "echo client-seq=1" {
			if frame["boot_id"] != "boot-a" {
				t.Fatalf("echo frame missing boot proof: %+v", frame)
			}
			for _, field := range []string{"server_read_unix_nano", "server_enqueue_unix_nano", "server_write_start_unix_nano", "server_write_end_unix_nano"} {
				if _, ok := frame[field].(float64); !ok {
					t.Fatalf("echo frame missing %s timing proof: %+v", field, frame)
				}
			}
			return
		}
	}
	t.Fatal("terminal websocket did not echo input")
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
