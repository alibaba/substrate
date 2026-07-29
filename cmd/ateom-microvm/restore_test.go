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

func TestMicroVMRestoreMemoryModeDefault(t *testing.T) {
	t.Setenv("ATE_MICROVM_RESTORE_MEMORY_MODE", "")

	if got := microVMRestoreMemoryMode(); got != "OnDemand" {
		t.Fatalf("microVMRestoreMemoryMode=%q, want OnDemand", got)
	}
}

func TestMicroVMRestoreMemoryModeCopyOverride(t *testing.T) {
	t.Setenv("ATE_MICROVM_RESTORE_MEMORY_MODE", "Copy")

	if got := microVMRestoreMemoryMode(); got != "Copy" {
		t.Fatalf("microVMRestoreMemoryMode=%q, want Copy", got)
	}
}

func TestMicroVMRestoreMemoryModeRejectsUnknown(t *testing.T) {
	t.Setenv("ATE_MICROVM_RESTORE_MEMORY_MODE", "bogus")

	if got := microVMRestoreMemoryMode(); got != "OnDemand" {
		t.Fatalf("microVMRestoreMemoryMode=%q, want OnDemand", got)
	}
}

func TestMicroVMRestoreLaunchModeDefault(t *testing.T) {
	t.Setenv("ATE_MICROVM_RESTORE_LAUNCH_MODE", "")

	if got := microVMRestoreLaunchMode(); got != "REST" {
		t.Fatalf("microVMRestoreLaunchMode=%q, want REST", got)
	}
}

func TestMicroVMRestoreLaunchModeCLIOverride(t *testing.T) {
	t.Setenv("ATE_MICROVM_RESTORE_LAUNCH_MODE", "CLI")

	if got := microVMRestoreLaunchMode(); got != "CLI" {
		t.Fatalf("microVMRestoreLaunchMode=%q, want CLI", got)
	}
}

func TestMicroVMRestoreLaunchModeRejectsUnknown(t *testing.T) {
	t.Setenv("ATE_MICROVM_RESTORE_LAUNCH_MODE", "bogus")

	if got := microVMRestoreLaunchMode(); got != "REST" {
		t.Fatalf("microVMRestoreLaunchMode=%q, want REST", got)
	}
}
