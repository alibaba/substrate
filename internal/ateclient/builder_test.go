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

package ateclient

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIsJWTAuthModeArg(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "equals jwt", args: []string{"--auth-mode=jwt"}, want: true},
		{name: "split jwt", args: []string{"--auth-mode", "jwt"}, want: true},
		{name: "equals mtls", args: []string{"--auth-mode=mtls"}, want: false},
		{name: "split mtls", args: []string{"--auth-mode", "mtls"}, want: false},
		{name: "missing value", args: []string{"--auth-mode"}, want: false},
		{name: "unrelated", args: []string{"--foo=bar"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isJWTAuthModeArg(tt.args); got != tt.want {
				t.Fatalf("isJWTAuthModeArg(%v) = %v, want %v", tt.args, got, tt.want)
			}
		})
	}
}

func TestClientKeepaliveParams(t *testing.T) {
	t.Setenv("ATE_CLIENT_KEEPALIVE_TIME", "")
	got := clientKeepaliveParams()
	if got.Time != 10*time.Minute {
		t.Fatalf("default keepalive time = %s, want 10m", got.Time)
	}
	if got.Timeout != 20*time.Second {
		t.Fatalf("keepalive timeout = %s, want 20s", got.Timeout)
	}
	if got.PermitWithoutStream {
		t.Fatal("PermitWithoutStream = true, want false")
	}
}

func TestClientKeepaliveParamsFromEnv(t *testing.T) {
	t.Setenv("ATE_CLIENT_KEEPALIVE_TIME", "30m")
	got := clientKeepaliveParams()
	if got.Time != 30*time.Minute {
		t.Fatalf("env keepalive time = %s, want 30m", got.Time)
	}
}

func TestClientKeepaliveParamsIgnoresInvalidEnv(t *testing.T) {
	t.Setenv("ATE_CLIENT_KEEPALIVE_TIME", "not-a-duration")
	got := clientKeepaliveParams()
	if got.Time != 10*time.Minute {
		t.Fatalf("invalid env keepalive time = %s, want default 10m", got.Time)
	}
}

func TestSelectPortForwardPodPrefersReadyRunningPod(t *testing.T) {
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "old-error"},
			Status:     corev1.PodStatus{Phase: corev1.PodFailed},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "new-ready"},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		},
	}

	if got := selectPortForwardPod(pods); got.Name != "new-ready" {
		t.Fatalf("selectPortForwardPod() = %q, want new-ready", got.Name)
	}
}

func TestSelectPortForwardPodFallsBackToFirstPod(t *testing.T) {
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "first"},
			Status:     corev1.PodStatus{Phase: corev1.PodPending},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "second"},
			Status:     corev1.PodStatus{Phase: corev1.PodFailed},
		},
	}

	if got := selectPortForwardPod(pods); got.Name != "first" {
		t.Fatalf("selectPortForwardPod() = %q, want first", got.Name)
	}
}
