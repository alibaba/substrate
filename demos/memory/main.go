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
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	ready       atomic.Bool
	resident    []byte
	targetBytes int64
	checksum    string
)

type appState struct {
	pattern     string
	seed        uint64
	instanceID  string
	podName     string
	nodeName    string
	startedUnix int64
	counter     atomic.Int64
	quiesced    atomic.Bool
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
		podName:     podName,
		nodeName:    os.Getenv("NODE_NAME"),
		startedUnix: time.Now().UnixNano(),
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
	mux.HandleFunc("/substrate/quiesce", state.handleQuiesce)
	mux.HandleFunc("/substrate/resume", state.handleResume)
	mux.HandleFunc("/substrate/migration-state", state.handleMigrationState)
	mux.HandleFunc("/", state.handleRoot)
	return mux
}

func (s *appState) handleRoot(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.snapshot())
}

func (s *appState) handleMigrationState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.snapshot())
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
	s.counter.Add(1)
	writeJSON(w, s.snapshot())
}

func (s *appState) handleQuiesce(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.quiesced.Store(true)
	writeJSON(w, s.snapshot())
}

func (s *appState) handleResume(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.quiesced.Store(false)
	writeJSON(w, s.snapshot())
}

func (s *appState) snapshot() map[string]any {
	runtime.KeepAlive(resident)
	return map[string]any{
		"ready":               ready.Load(),
		"target_memory_bytes": targetBytes,
		"resident_len":        len(resident),
		"checksum":            checksum,
		"memory_pattern":      s.pattern,
		"memory_seed":         s.seed,
		"counter":             s.counter.Load(),
		"quiesced":            s.quiesced.Load(),
		"instance_id":         s.instanceID,
		"pod_name":            s.podName,
		"node_name":           s.nodeName,
		"started_unix_nano":   s.startedUnix,
		"unix_nano":           time.Now().UnixNano(),
	}
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
