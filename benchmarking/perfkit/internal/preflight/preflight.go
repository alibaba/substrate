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

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
)

const (
	StatusPass = "pass"
	StatusFail = "fail"
	StatusWarn = "warn"
)

type Observation struct {
	ServerVersion    string            `json:"server_version"`
	Nodes            []NodeObservation `json:"nodes"`
	Namespaces       []string          `json:"namespaces"`
	Pods             []PodObservation  `json:"pods"`
	RequireSubstrate bool              `json:"require_substrate"`
}

type NodeObservation struct {
	Name    string `json:"name"`
	Ready   bool   `json:"ready"`
	Virtual bool   `json:"virtual"`
}

type PodObservation struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Ready     bool   `json:"ready"`
	Phase     string `json:"phase,omitempty"`
}

type Check struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type Result struct {
	GeneratedAt string      `json:"generated_at"`
	Status      string      `json:"status"`
	Observation Observation `json:"observation"`
	Checks      []Check     `json:"checks"`
}

func Collect(ctx context.Context, kubeconfig string, requireSubstrate bool) (Observation, error) {
	cfg, err := ateclient.LoadConfig(kubeconfig, "")
	if err != nil {
		return Observation{}, fmt.Errorf("load kubeconfig: %w", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return Observation{}, fmt.Errorf("create kubernetes client: %w", err)
	}
	disc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return Observation{}, fmt.Errorf("create discovery client: %w", err)
	}
	version, err := disc.ServerVersion()
	if err != nil {
		return Observation{}, fmt.Errorf("server version: %w", err)
	}
	obs := Observation{ServerVersion: version.GitVersion, RequireSubstrate: requireSubstrate}

	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return Observation{}, fmt.Errorf("list nodes: %w", err)
	}
	for _, node := range nodes.Items {
		obs.Nodes = append(obs.Nodes, NodeObservation{
			Name:    node.Name,
			Ready:   nodeReady(node),
			Virtual: strings.HasPrefix(node.Name, "virtual-kubelet") || node.Labels["type"] == "virtual-kubelet",
		})
	}

	namespaces, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return Observation{}, fmt.Errorf("list namespaces: %w", err)
	}
	for _, ns := range namespaces.Items {
		obs.Namespaces = append(obs.Namespaces, ns.Name)
	}

	pods, err := client.CoreV1().Pods("ate-system").List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, pod := range pods.Items {
			obs.Pods = append(obs.Pods, PodObservation{
				Namespace: pod.Namespace,
				Name:      pod.Name,
				Ready:     podReady(pod),
				Phase:     string(pod.Status.Phase),
			})
		}
	}
	return obs, nil
}

func CollectWithKubectl(ctx context.Context, kubeconfig string, requireSubstrate bool) (Observation, error) {
	var version struct {
		ServerVersion struct {
			GitVersion string `json:"gitVersion"`
		} `json:"serverVersion"`
	}
	if err := kubectlJSON(ctx, kubeconfig, &version, "version", "-o", "json"); err != nil {
		return Observation{}, fmt.Errorf("kubectl version: %w", err)
	}
	obs := Observation{ServerVersion: version.ServerVersion.GitVersion, RequireSubstrate: requireSubstrate}

	var nodes struct {
		Items []struct {
			Metadata struct {
				Name   string            `json:"name"`
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
			Status struct {
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := kubectlJSON(ctx, kubeconfig, &nodes, "get", "nodes", "-o", "json"); err != nil {
		return Observation{}, fmt.Errorf("kubectl get nodes: %w", err)
	}
	for _, node := range nodes.Items {
		ready := false
		for _, cond := range node.Status.Conditions {
			if cond.Type == "Ready" {
				ready = cond.Status == "True"
				break
			}
		}
		obs.Nodes = append(obs.Nodes, NodeObservation{
			Name:    node.Metadata.Name,
			Ready:   ready,
			Virtual: strings.HasPrefix(node.Metadata.Name, "virtual-kubelet") || node.Metadata.Labels["type"] == "virtual-kubelet",
		})
	}

	var namespaces struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := kubectlJSON(ctx, kubeconfig, &namespaces, "get", "ns", "-o", "json"); err != nil {
		return Observation{}, fmt.Errorf("kubectl get ns: %w", err)
	}
	for _, ns := range namespaces.Items {
		obs.Namespaces = append(obs.Namespaces, ns.Metadata.Name)
	}

	var pods struct {
		Items []struct {
			Metadata struct {
				Namespace string `json:"namespace"`
				Name      string `json:"name"`
			} `json:"metadata"`
			Status struct {
				Phase      string `json:"phase"`
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := kubectlJSON(ctx, kubeconfig, &pods, "get", "pods", "-n", "ate-system", "-o", "json"); err == nil {
		for _, pod := range pods.Items {
			ready := false
			for _, cond := range pod.Status.Conditions {
				if cond.Type == "Ready" {
					ready = cond.Status == "True"
					break
				}
			}
			obs.Pods = append(obs.Pods, PodObservation{
				Namespace: pod.Metadata.Namespace,
				Name:      pod.Metadata.Name,
				Ready:     ready,
				Phase:     pod.Status.Phase,
			})
		}
	}
	return obs, nil
}

func Evaluate(obs Observation) Result {
	result := Result{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Status:      StatusPass,
		Observation: obs,
		Checks:      []Check{},
	}
	add := func(name, status, msg string) {
		result.Checks = append(result.Checks, Check{Name: name, Status: status, Message: msg})
		if status == StatusFail {
			result.Status = StatusFail
		} else if status == StatusWarn && result.Status == StatusPass {
			result.Status = StatusWarn
		}
	}
	if obs.ServerVersion == "" {
		add("kubernetes_api", StatusFail, "Kubernetes API version is unavailable")
	} else {
		add("kubernetes_api", StatusPass, "Kubernetes API reachable: "+obs.ServerVersion)
	}

	readyRealNodes := 0
	for _, node := range obs.Nodes {
		if node.Ready && !node.Virtual {
			readyRealNodes++
		}
	}
	if readyRealNodes == 0 {
		add("real_nodes_ready", StatusFail, "no Ready real ECS nodes observed")
	} else {
		add("real_nodes_ready", StatusPass, fmt.Sprintf("%d Ready real ECS nodes observed", readyRealNodes))
	}

	hasAteSystem := contains(obs.Namespaces, "ate-system")
	if obs.RequireSubstrate && !hasAteSystem {
		add("substrate_namespace", StatusFail, "ate-system namespace is missing")
	} else if hasAteSystem {
		add("substrate_namespace", StatusPass, "ate-system namespace exists")
	} else {
		add("substrate_namespace", StatusWarn, "ate-system namespace not required and not present")
	}

	if obs.RequireSubstrate && hasAteSystem {
		if hasReadyPrefix(obs.Pods, "ate-api-server") && hasReadyPrefix(obs.Pods, "atelet") && hasReadyPrefix(obs.Pods, "valkey-cluster") {
			add("substrate_core_pods", StatusPass, "ate-api-server, atelet, and valkey pods are Ready")
		} else {
			add("substrate_core_pods", StatusFail, "one or more core Substrate pods are missing or not Ready")
		}
	}
	return result
}

func (r Result) CheckByName(name string) *Check {
	for i := range r.Checks {
		if r.Checks[i].Name == name {
			return &r.Checks[i]
		}
	}
	return nil
}

func nodeReady(node corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func podReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func hasReadyPrefix(pods []PodObservation, prefix string) bool {
	for _, pod := range pods {
		if strings.HasPrefix(pod.Name, prefix) && pod.Ready {
			return true
		}
	}
	return false
}

func kubectlJSON(ctx context.Context, kubeconfig string, out any, args ...string) error {
	fullArgs := append([]string{"--kubeconfig", kubeconfig}, args...)
	cmd := exec.CommandContext(ctx, "kubectl", fullArgs...)
	data, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(ee.Stderr)))
		}
		return err
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode kubectl JSON: %w", err)
	}
	return nil
}
