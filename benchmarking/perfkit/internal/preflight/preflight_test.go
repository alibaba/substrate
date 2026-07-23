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

package preflight

import "testing"

func TestEvaluateRequiresRealNodesAndSubstrate(t *testing.T) {
	result := Evaluate(Observation{
		ServerVersion: "v1.36.1",
		Nodes: []NodeObservation{
			{Name: "virtual-kubelet-cn-hongkong-c", Ready: true, Virtual: true},
		},
		Namespaces:       []string{"default", "kube-system"},
		RequireSubstrate: true,
	})

	if result.Status != StatusFail {
		t.Fatalf("Status=%s, want %s", result.Status, StatusFail)
	}
	if check := result.CheckByName("real_nodes_ready"); check == nil || check.Status != StatusFail {
		t.Fatalf("real_nodes_ready check = %+v, want fail", check)
	}
	if check := result.CheckByName("substrate_namespace"); check == nil || check.Status != StatusFail {
		t.Fatalf("substrate_namespace check = %+v, want fail", check)
	}
}

func TestEvaluatePassesWhenCorePodsReady(t *testing.T) {
	result := Evaluate(Observation{
		ServerVersion: "v1.36.1",
		Nodes: []NodeObservation{
			{Name: "cn-hongkong.10.18.140.247", Ready: true},
			{Name: "cn-hongkong.10.18.140.248", Ready: true},
		},
		Namespaces: []string{"default", "kube-system", "ate-system"},
		Pods: []PodObservation{
			{Namespace: "ate-system", Name: "ate-api-server", Ready: true},
			{Namespace: "ate-system", Name: "atelet-abc", Ready: true},
			{Namespace: "ate-system", Name: "valkey-cluster-0", Ready: true},
		},
		RequireSubstrate: true,
	})

	if result.Status != StatusPass {
		t.Fatalf("Status=%s, want %s; checks=%+v", result.Status, StatusPass, result.Checks)
	}
}
