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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var (
	ready       atomic.Bool
	resident    []byte
	targetBytes int64
	checksum    string
)

type appState struct {
	pattern      string
	seed         uint64
	instanceID   string
	bootID       string
	podName      string
	nodeName     string
	startedUnix  int64
	counter      atomic.Int64
	stateVer     atomic.Int64
	lastMut      atomic.Int64
	streamSeq    atomic.Int64
	streamMu     sync.Mutex
	streamReplay []streamReplayFrame
	quiesced     atomic.Bool
	mutMu        sync.Mutex
	appliedKeys  map[string]mutationCommit
	dirty        dirtyState
}

type streamReplayFrame struct {
	sequence int64
	payload  []byte
}

const streamReplayLimit = 1024

type dirtyState struct {
	active        atomic.Bool
	rateMiBPerSec atomic.Int64
	totalBytes    atomic.Int64
	generation    atomic.Int64
	stopMu        sync.Mutex
	stop          chan struct{}
}

type mutationCommit struct {
	counter      int64
	stateVersion int64
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	targetMiB := envInt("TARGET_MEMORY_MIB", 64)
	pattern := os.Getenv("MEMORY_PATTERN")
	if pattern == "" {
		pattern = "randomish"
	}
	seed := envUint64("MEMORY_SEED", 0x9e3779b97f4a7c15)
	targetBytes = int64(targetMiB) * 1024 * 1024

	start := time.Now()
	resident = make([]byte, targetBytes)
	touchMemory(resident, pattern, seed)
	sum := sha256.Sum256(resident)
	checksum = hex.EncodeToString(sum[:])
	runtime.KeepAlive(resident)
	ready.Store(true)

	slog.Info("memory workload ready",
		slog.Int("target_memory_mib", targetMiB),
		slog.Int64("target_memory_bytes", targetBytes),
		slog.String("memory_pattern", pattern),
		slog.Uint64("memory_seed", seed),
		slog.String("checksum", checksum),
		slog.Duration("init_duration", time.Since(start)))

	state := newAppState(pattern, seed)
	mux := newMux(state)

	if err := http.ListenAndServe(":80", mux); err != nil {
		slog.Error("server failed", slog.Any("err", err))
		os.Exit(1)
	}
}

func newAppState(pattern string, seed uint64) *appState {
	hostname, _ := os.Hostname()
	podName := firstNonEmpty(os.Getenv("POD_NAME"), hostname)
	return &appState{
		pattern:     pattern,
		seed:        seed,
		instanceID:  firstNonEmpty(os.Getenv("INSTANCE_ID"), hostname),
		bootID:      firstNonEmpty(os.Getenv("BOOT_ID"), uuid.NewString()),
		podName:     podName,
		nodeName:    os.Getenv("NODE_NAME"),
		startedUnix: time.Now().UnixNano(),
		appliedKeys: make(map[string]mutationCommit),
	}
}

func newMux(state *appState) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/increment", state.handleIncrement)
	mux.HandleFunc("/slow", state.handleSlow)
	mux.HandleFunc("/download", state.handleDownload)
	mux.HandleFunc("/upload", state.handleUpload)
	mux.HandleFunc("/dirty/start", state.handleDirtyStart)
	mux.HandleFunc("/dirty/stop", state.handleDirtyStop)
	mux.HandleFunc("/terminal/ws", state.handleTerminalWebSocket)
	mux.HandleFunc("/stream", state.handleStream)
	mux.HandleFunc("/substrate/quiesce", state.handleQuiesce)
	mux.HandleFunc("/substrate/resume", state.handleResume)
	mux.HandleFunc("/substrate/migration-state", state.handleMigrationState)
	mux.HandleFunc("/", state.handleRoot)
	return mux
}

func (s *appState) handleDirtyStart(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rate := queryInt64(req, "rate_mib_per_s", 128, 4096)
	s.startDirtyWorker(rate)
	writeJSON(w, s.snapshot())
}

func (s *appState) handleDirtyStop(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.stopDirtyWorker()
	writeJSON(w, s.snapshot())
}

func (s *appState) startDirtyWorker(rateMiBPerSec int64) {
	s.stopDirtyWorker()
	stop := make(chan struct{})
	s.dirty.stopMu.Lock()
	s.dirty.stop = stop
	s.dirty.rateMiBPerSec.Store(rateMiBPerSec)
	s.dirty.active.Store(true)
	generation := s.dirty.generation.Add(1)
	s.dirty.stopMu.Unlock()

	go func() {
		const chunk = 1 << 20
		if len(resident) == 0 {
			return
		}
		offset := 0
		interval := 10 * time.Millisecond
		bytesPerTick := int(rateMiBPerSec) * chunk / int(time.Second/interval)
		if bytesPerTick < 4096 {
			bytesPerTick = 4096
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				n := bytesPerTick
				if n > len(resident) {
					n = len(resident)
				}
				for i := 0; i < n; i += 4096 {
					idx := (offset + i) % len(resident)
					resident[idx] = byte(generation + s.dirty.totalBytes.Load()/4096)
				}
				offset = (offset + n) % len(resident)
				s.dirty.totalBytes.Add(int64(n))
				s.stateVer.Add(1)
			}
		}
	}()
}

func (s *appState) stopDirtyWorker() {
	s.dirty.stopMu.Lock()
	stop := s.dirty.stop
	s.dirty.stop = nil
	s.dirty.active.Store(false)
	s.dirty.rateMiBPerSec.Store(0)
	s.dirty.stopMu.Unlock()
	if stop != nil {
		close(stop)
	}
}

var terminalUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

func (s *appState) handleTerminalWebSocket(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	conn, err := terminalUpgrader.Upgrade(w, req, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	done := make(chan struct{})
	var once sync.Once
	closeDone := func() { once.Do(func() { close(done) }) }

	var writeMu sync.Mutex
	writeFrame := func(kind string, enrich func(map[string]any)) bool {
		enqueueAt := time.Now()
		writeMu.Lock()
		defer writeMu.Unlock()
		frame := s.snapshot()
		frame["type"] = kind
		frame["terminal_pid"] = os.Getpid()
		frame["server_enqueue_unix_nano"] = enqueueAt.UnixNano()
		if enrich != nil {
			enrich(frame)
		}
		writeStart := time.Now()
		frame["server_unix_nano"] = writeStart.UnixNano()
		frame["server_write_start_unix_nano"] = writeStart.UnixNano()
		_ = conn.SetWriteDeadline(writeStart.Add(2 * time.Second))
		frame["server_write_end_unix_nano"] = time.Now().UnixNano()
		return conn.WriteJSON(frame) == nil
	}

	go func() {
		defer closeDone()
		for {
			messageType, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			readAt := time.Now()
			if !writeFrame("echo", func(payload map[string]any) {
				payload["message_type"] = messageType
				payload["input"] = string(data)
				payload["server_read_unix_nano"] = readAt.UnixNano()
			}) {
				return
			}
		}
	}()

	ticker := time.NewTicker(time.Duration(envInt("TERMINAL_TICK_INTERVAL_MS", 100)) * time.Millisecond)
	defer ticker.Stop()
	sequence := int64(0)
	for {
		sequence++
		if !writeFrame("tick", func(payload map[string]any) {
			payload["terminal_sequence"] = sequence
		}) {
			return
		}
		select {
		case <-done:
			return
		case <-req.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *appState) handleRoot(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.snapshot())
}

func (s *appState) handleMigrationState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.snapshot())
}

func (s *appState) handleStream(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	lastEventID := parseLastEventID(req.Header.Get("Last-Event-ID"))
	writeFrame := func() bool {
		return s.writeStreamFrame(w, flusher)
	}
	if !s.writeStreamFrameSince(w, flusher, lastEventID) {
		return
	}
	interval := time.Duration(envInt("STREAM_INTERVAL_MS", 100)) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-req.Context().Done():
			return
		case <-ticker.C:
			if !writeFrame() {
				return
			}
		}
	}
}

func (s *appState) writeStreamFrame(w io.Writer, flusher http.Flusher) bool {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	return s.writeStreamFrameLocked(w, flusher)
}

func (s *appState) writeStreamFrameSince(w io.Writer, flusher http.Flusher, lastEventID int64) bool {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	if lastEventID > 0 {
		for _, frame := range s.streamReplay {
			if frame.sequence <= lastEventID {
				continue
			}
			if err := writeSSEStateEvent(w, frame.sequence, frame.payload); err != nil {
				return false
			}
			flusher.Flush()
		}
	}
	return s.writeStreamFrameLocked(w, flusher)
}

func (s *appState) writeStreamFrameLocked(w io.Writer, flusher http.Flusher) bool {
	sequence := s.streamSeq.Load() + 1
	frame := s.snapshot()
	frame["sequence"] = sequence
	frame["server_unix_nano"] = time.Now().UnixNano()
	payload, err := json.Marshal(frame)
	if err != nil {
		return false
	}
	if err := writeSSEStateEvent(w, sequence, payload); err != nil {
		return false
	}
	flusher.Flush()
	s.streamSeq.Store(sequence)
	s.appendStreamReplayFrame(sequence, payload)
	return true
}

func writeSSEStateEvent(w io.Writer, sequence int64, payload []byte) error {
	_, err := fmt.Fprintf(w, "event: state\nid: %d\ndata: %s\n\n", sequence, payload)
	return err
}

func (s *appState) appendStreamReplayFrame(sequence int64, payload []byte) {
	copied := append([]byte(nil), payload...)
	s.streamReplay = append(s.streamReplay, streamReplayFrame{sequence: sequence, payload: copied})
	if len(s.streamReplay) > streamReplayLimit {
		copy(s.streamReplay, s.streamReplay[len(s.streamReplay)-streamReplayLimit:])
		s.streamReplay = s.streamReplay[:streamReplayLimit]
	}
}

func parseLastEventID(raw string) int64 {
	seq, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || seq < 0 {
		return 0
	}
	return seq
}

func (s *appState) handleIncrement(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.quiesced.Load() {
		http.Error(w, "quiesced", http.StatusServiceUnavailable)
		return
	}
	key := req.Header.Get("Idempotency-Key")
	if key != "" {
		s.mutMu.Lock()
		if commit, ok := s.appliedKeys[key]; ok {
			s.mutMu.Unlock()
			writeJSON(w, s.snapshotAt(commit.counter, commit.stateVersion, commit.counter))
			return
		}
		next := s.counter.Add(1)
		version := s.stateVer.Add(1)
		s.lastMut.Store(next)
		s.appliedKeys[key] = mutationCommit{counter: next, stateVersion: version}
		s.mutMu.Unlock()
		writeJSON(w, s.snapshotAt(next, version, next))
		return
	}
	next := s.counter.Add(1)
	version := s.stateVer.Add(1)
	s.lastMut.Store(next)
	writeJSON(w, s.snapshotAt(next, version, next))
}

func (s *appState) handleSlow(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	start := time.Now()
	delay := queryDuration(req, "duration_ms", 5*time.Second, 30*time.Second)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-req.Context().Done():
		return
	case <-timer.C:
	}
	payload := s.snapshot()
	payload["scenario"] = "slow_get"
	payload["requested_duration_ms"] = delay.Milliseconds()
	payload["server_duration_ms"] = time.Since(start).Milliseconds()
	writeJSON(w, payload)
}

func (s *appState) handleDownload(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	bytesToWrite := queryInt64(req, "bytes", 8<<20, 128<<20)
	chunkSize := queryInt64(req, "chunk_bytes", 64<<10, 1<<20)
	delay := queryDuration(req, "chunk_delay_ms", 0, time.Second)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Substrate-Download-Bytes", strconv.FormatInt(bytesToWrite, 10))
	w.Header().Set("X-Substrate-Download-Checksum", checksum)
	s.addStateHeaders(w.Header(), s.snapshot())
	flusher, _ := w.(http.Flusher)

	buf := make([]byte, chunkSize)
	var sent int64
	for sent < bytesToWrite {
		for i := range buf {
			buf[i] = byte((sent + int64(i)) & 0xff)
		}
		n := min(int64(len(buf)), bytesToWrite-sent)
		if _, err := w.Write(buf[:n]); err != nil {
			return
		}
		sent += n
		if flusher != nil {
			flusher.Flush()
		}
		if delay > 0 && sent < bytesToWrite {
			timer := time.NewTimer(delay)
			select {
			case <-req.Context().Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func (s *appState) handleUpload(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.quiesced.Load() {
		http.Error(w, "quiesced", http.StatusServiceUnavailable)
		return
	}
	limit := queryInt64(req, "max_bytes", 128<<20, 512<<20)
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(req.Body, limit+1))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if n > limit {
		http.Error(w, "upload too large", http.StatusRequestEntityTooLarge)
		return
	}
	key := req.Header.Get("Idempotency-Key")
	if key != "" {
		s.mutMu.Lock()
		if commit, ok := s.appliedKeys[key]; ok {
			s.mutMu.Unlock()
			payload := s.snapshotAt(commit.counter, commit.stateVersion, commit.counter)
			payload["scenario"] = "upload"
			payload["upload_bytes"] = n
			payload["upload_sha256"] = hex.EncodeToString(h.Sum(nil))
			writeJSON(w, payload)
			return
		}
		next := s.counter.Add(1)
		version := s.stateVer.Add(1)
		s.lastMut.Store(next)
		s.appliedKeys[key] = mutationCommit{counter: next, stateVersion: version}
		s.mutMu.Unlock()
		payload := s.snapshotAt(next, version, next)
		payload["scenario"] = "upload"
		payload["upload_bytes"] = n
		payload["upload_sha256"] = hex.EncodeToString(h.Sum(nil))
		writeJSON(w, payload)
		return
	}
	next := s.counter.Add(1)
	version := s.stateVer.Add(1)
	s.lastMut.Store(next)
	payload := s.snapshotAt(next, version, next)
	payload["scenario"] = "upload"
	payload["upload_bytes"] = n
	payload["upload_sha256"] = hex.EncodeToString(h.Sum(nil))
	writeJSON(w, payload)
}

func (s *appState) handleQuiesce(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.quiesced.Swap(true) {
		s.stateVer.Add(1)
	}
	writeJSON(w, s.snapshot())
}

func (s *appState) handleResume(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.quiesced.Swap(false) {
		s.stateVer.Add(1)
	}
	writeJSON(w, s.snapshot())
}

func (s *appState) snapshot() map[string]any {
	return s.snapshotAt(s.counter.Load(), s.stateVer.Load(), s.lastMut.Load())
}

func (s *appState) snapshotAt(counter, stateVersion, lastMut int64) map[string]any {
	runtime.KeepAlive(resident)
	lastMutation := ""
	if lastMut > 0 {
		lastMutation = fmt.Sprintf("increment-%d", lastMut)
	}
	return map[string]any{
		"ready":                ready.Load(),
		"target_memory_bytes":  targetBytes,
		"resident_len":         len(resident),
		"checksum":             checksum,
		"memory_pattern":       s.pattern,
		"memory_seed":          s.seed,
		"counter":              counter,
		"state_version":        stateVersion,
		"last_mutation_id":     lastMutation,
		"quiesced":             s.quiesced.Load(),
		"dirty_active":         s.dirty.active.Load(),
		"dirty_rate_mib_per_s": s.dirty.rateMiBPerSec.Load(),
		"dirty_bytes_total":    s.dirty.totalBytes.Load(),
		"dirty_generation":     s.dirty.generation.Load(),
		"instance_id":          s.instanceID,
		"boot_id":              s.bootID,
		"pod_name":             s.podName,
		"node_name":            s.nodeName,
		"started_unix_nano":    s.startedUnix,
		"unix_nano":            time.Now().UnixNano(),
	}
}

func (s *appState) addStateHeaders(header http.Header, snapshot map[string]any) {
	header.Set("X-Substrate-State-Checksum", checksum)
	header.Set("X-Substrate-State-Counter", fmt.Sprint(snapshot["counter"]))
	header.Set("X-Substrate-State-Version", fmt.Sprint(snapshot["state_version"]))
	header.Set("X-Substrate-State-Last-Mutation-ID", fmt.Sprint(snapshot["last_mutation_id"]))
	header.Set("X-Substrate-State-Boot-ID", s.bootID)
	header.Set("X-Substrate-State-Instance-ID", s.instanceID)
	header.Set("X-Substrate-State-Pod-Name", s.podName)
	header.Set("X-Substrate-State-Node-Name", s.nodeName)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func envInt(name string, def int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		panic(fmt.Sprintf("%s must be a positive integer, got %q", name, raw))
	}
	return v
}

func envUint64(name string, def uint64) uint64 {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseUint(raw, 0, 64)
	if err != nil || v == 0 {
		panic(fmt.Sprintf("%s must be a positive integer, got %q", name, raw))
	}
	return v
}

func queryDuration(req *http.Request, name string, def, maxValue time.Duration) time.Duration {
	raw := req.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return def
	}
	d := time.Duration(v) * time.Millisecond
	if d > maxValue {
		return maxValue
	}
	return d
}

func queryInt64(req *http.Request, name string, def, maxValue int64) int64 {
	raw := req.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return def
	}
	if v > maxValue {
		return maxValue
	}
	return v
}

func touchMemory(buf []byte, pattern string, seed uint64) {
	const page = 4096
	x := seed
	if pattern == "dense-random" {
		for i := 0; i < len(buf); i += 8 {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			if i+8 <= len(buf) {
				binary.LittleEndian.PutUint64(buf[i:i+8], x)
			} else {
				for j := i; j < len(buf); j++ {
					buf[j] = byte(x >> (8 * (j - i)))
				}
			}
		}
		return
	}
	for i := 0; i < len(buf); i += page {
		switch pattern {
		case "zeroed":
			buf[i] = 0
		case "randomish", "sparse-randomish":
			x ^= uint64(i) + 0x9e3779b97f4a7c15 + (x << 6) + (x >> 2)
			buf[i] = byte(x)
			if i+page-1 < len(buf) {
				buf[i+page-1] = byte(x >> 8)
			}
		default:
			panic(fmt.Sprintf("unknown MEMORY_PATTERN %q", pattern))
		}
	}
}
