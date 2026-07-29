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
	"os"
	"strconv"
	"strings"
	"time"
)

func liveMigrationTCPDialTimeout() time.Duration {
	return durationEnvMS("ATE_LIVE_MIGRATION_TCP_DIAL_TIMEOUT_MS", 120*time.Second)
}

func liveMigrationUnixDialTimeout() time.Duration {
	return durationEnvMS("ATE_LIVE_MIGRATION_UNIX_DIAL_TIMEOUT_MS", 120*time.Second)
}

func microVMRestoreMemoryMode() string {
	switch strings.TrimSpace(os.Getenv("ATE_MICROVM_RESTORE_MEMORY_MODE")) {
	case "Copy":
		return "Copy"
	case "OnDemand":
		return "OnDemand"
	default:
		return "OnDemand"
	}
}

func microVMRestoreLaunchMode() string {
	switch strings.TrimSpace(os.Getenv("ATE_MICROVM_RESTORE_LAUNCH_MODE")) {
	case "CLI":
		return "CLI"
	default:
		return "REST"
	}
}

func durationEnvMS(name string, def time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		return def
	}
	return time.Duration(ms) * time.Millisecond
}
