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
	"encoding/json"
	"strings"
	"testing"
)

func TestCpusConfigMarshalsCoreScheduling(t *testing.T) {
	got, err := json.Marshal(CpusConfig{
		BootVcpus:      1,
		MaxVcpus:       1,
		CoreScheduling: "Off",
	})
	if err != nil {
		t.Fatalf("marshal cpus config: %v", err)
	}
	if !strings.Contains(string(got), `"core_scheduling":"Off"`) {
		t.Fatalf("cpus config = %s, want core_scheduling Off", got)
	}
}

func TestVmConfigMarshalsTapNetConfig(t *testing.T) {
	got, err := json.Marshal(VmConfig{
		Cpus: CpusConfig{BootVcpus: 1, MaxVcpus: 1},
		Memory: MemoryConfig{
			Size:   256 << 20,
			Shared: true,
		},
		Payload: PayloadConfig{Kernel: "/kernel", Cmdline: "console=ttyS0"},
		Net: []NetConfig{{
			Tap:       "tap0_kata",
			MAC:       "02:a8:1e:00:00:02",
			NumQueues: 2,
			QueueSize: 1024,
		}},
	})
	if err != nil {
		t.Fatalf("marshal vm config: %v", err)
	}
	for _, want := range []string{
		`"net":[{`,
		`"tap":"tap0_kata"`,
		`"mac":"02:a8:1e:00:00:02"`,
		`"num_queues":2`,
		`"queue_size":1024`,
	} {
		if !strings.Contains(string(got), want) {
			t.Fatalf("vm config = %s, want substring %s", got, want)
		}
	}
}
