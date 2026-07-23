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
	"io"
	"testing"
)

func TestSanitizePodName(t *testing.T) {
	got := sanitizePodName("cn-hangzhou.192.168.11.141")
	if got != "cn-hangzhou-192-168-11-141" {
		t.Fatalf("sanitizePodName()=%q, want %q", got, "cn-hangzhou-192-168-11-141")
	}
}

func TestCreateOutputStdout(t *testing.T) {
	out, err := createOutput("-")
	if err != nil {
		t.Fatalf("createOutput(-) returned error: %v", err)
	}
	defer out.Close()
	if _, ok := out.(nopWriteCloser); !ok {
		t.Fatalf("createOutput(-) returned %T, want nopWriteCloser", out)
	}
	if _, err := io.WriteString(out, ""); err != nil {
		t.Fatalf("write to stdout output: %v", err)
	}
}

func TestParseConfigMapOutput(t *testing.T) {
	target, ok := parseConfigMapOutput("k8s-configmaps://ate-system/perfkit-run")
	if !ok {
		t.Fatalf("parseConfigMapOutput() ok=false, want true")
	}
	if target.namespace != "ate-system" || target.prefix != "perfkit-run" {
		t.Fatalf("parseConfigMapOutput()=%+v", target)
	}
}

func TestParseLabelMap(t *testing.T) {
	got, err := parseLabelMap("disk=raid, zone = a")
	if err != nil {
		t.Fatalf("parseLabelMap returned error: %v", err)
	}
	if got["disk"] != "raid" || got["zone"] != "a" {
		t.Fatalf("parseLabelMap()=%v", got)
	}
}

func TestParseLabelMapRejectsInvalidLabel(t *testing.T) {
	if _, err := parseLabelMap("disk"); err == nil {
		t.Fatal("parseLabelMap returned nil error, want invalid label error")
	}
}
