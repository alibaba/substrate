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
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestBuildVMConfigDisablesCloudHypervisorCoreScheduling(t *testing.T) {
	cfg := buildVMConfig("sandbox", "/kernel", "/image", "", "/serial.log", 256, 1)

	got, err := json.Marshal(cfg.Cpus)
	if err != nil {
		t.Fatalf("marshal cpus config: %v", err)
	}
	if !strings.Contains(string(got), `"core_scheduling":"Off"`) {
		t.Fatalf("cpus config = %s, want core_scheduling Off", got)
	}
}

func TestWithTapNetConfigAddsMigrationCompatibleNetDevice(t *testing.T) {
	cfg := buildVMConfig("sandbox", "/kernel", "/image", "", "/serial.log", 256, 1)

	withTapNetConfig(&cfg, "tap0_kata")

	if len(cfg.Net) != 1 {
		t.Fatalf("len(cfg.Net) = %d, want 1", len(cfg.Net))
	}
	net := cfg.Net[0]
	if net.Tap != "tap0_kata" {
		t.Fatalf("net tap = %q, want tap0_kata", net.Tap)
	}
	if net.MAC != actorGuestMAC {
		t.Fatalf("net mac = %q, want %q", net.MAC, actorGuestMAC)
	}
	if net.NumQueues != 2 {
		t.Fatalf("net num_queues = %d, want 2", net.NumQueues)
	}
}

func TestLiveMigrationTapNetEnabled(t *testing.T) {
	t.Setenv("ATE_CH_TAP_NET", "")
	if liveMigrationTapNetEnabled() {
		t.Fatal("liveMigrationTapNetEnabled() = true, want false")
	}

	t.Setenv("ATE_CH_TAP_NET", "1")
	if !liveMigrationTapNetEnabled() {
		t.Fatal("liveMigrationTapNetEnabled() = false, want true")
	}
}

func TestCloseTapFilesClosesAllOpenFiles(t *testing.T) {
	f1, err := os.CreateTemp(t.TempDir(), "tap-1-*")
	if err != nil {
		t.Fatalf("create temp file 1: %v", err)
	}
	f2, err := os.CreateTemp(t.TempDir(), "tap-2-*")
	if err != nil {
		t.Fatalf("create temp file 2: %v", err)
	}

	closeTapFiles([]*os.File{f1, nil, f2})

	if _, err := f1.Write([]byte("x")); err == nil {
		t.Fatal("first file is still writable after closeTapFiles")
	}
	if _, err := f2.Write([]byte("x")); err == nil {
		t.Fatal("second file is still writable after closeTapFiles")
	}
}
