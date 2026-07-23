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
