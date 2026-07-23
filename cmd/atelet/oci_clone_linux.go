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

//go:build linux

package main

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func cloneRegularFile(src, dst string, mode os.FileMode) (string, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_RDWR, mode)
	if err != nil {
		return "", err
	}
	method := "reflink"
	if err := unix.IoctlFileClone(int(out.Fd()), int(in.Fd())); err != nil {
		method = "copy"
		if _, seekErr := in.Seek(0, 0); seekErr != nil {
			_ = out.Close()
			return "", seekErr
		}
		if _, copyErr := io.Copy(out, in); copyErr != nil {
			_ = out.Close()
			return "", copyErr
		}
	}
	if err := out.Chmod(mode); err != nil {
		_ = out.Close()
		return "", err
	}
	if err := out.Close(); err != nil {
		return "", err
	}
	return method, nil
}
