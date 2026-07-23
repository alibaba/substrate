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
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

type runsc struct {
	path      string
	atespace  string
	actorName string
}

func (r *runsc) cmdCreate(ctx context.Context, out io.Writer, containerName string, additionalArgs []string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to run runsc create", slog.String("container", containerName))

	args := []string{
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		// "-debug-log", ateompath.RunscDebugLogDir(r.atespace, r.actorName, containerName) + "/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-root", ateompath.RunSCStateDir(r.atespace, r.actorName),
		"create",
		"-bundle", ateompath.OCIBundlePath(r.atespace, r.actorName, containerName),
		"-pid-file", ateompath.PIDFilePath(r.atespace, r.actorName, containerName),
	}

	args = append(args, additionalArgs...)
	args = append(args, containerName) // Name of the container
	cmd := exec.CommandContext(
		ctx,
		r.path,
		args...,
	)
	cmd.Stdout = out
	cmd.Stderr = out

	return runRunscCommand(ctx, "runsc create", containerName, cmd)
}

func (r *runsc) cmdStart(ctx context.Context, out io.Writer, containerName string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to run runsc start", slog.String("container", containerName))

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		// "-debug-log", ateompath.RunscDebugLogDir(r.atespace, r.actorName, containerName)+"/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-allow-connected-on-save",
		"-root", ateompath.RunSCStateDir(r.atespace, r.actorName),
		"start",
		containerName, // Name of the container
	)
	cmd.Stdout = out
	cmd.Stderr = out

	return runRunscCommand(ctx, "runsc start", containerName, cmd)
}

func (r *runsc) cmdCheckpoint(ctx context.Context, containerName, checkpointPath string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to run runsc checkpoint", slog.String("container", containerName))

	args := []string{
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		// "-debug-log", ateompath.RunscDebugLogDir(r.atespace, r.actorName, containerName)+"/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-root", ateompath.RunSCStateDir(r.atespace, r.actorName),
		"checkpoint",
		"-image-path", checkpointPath,
	}
	if runscCheckpointDirectEnabled() {
		args = append(args, "-direct")
	}
	if runscCheckpointExcludeCommittedZeroPagesEnabled() {
		args = append(args, "-exclude-committed-zero-pages")
	}
	args = append(args, containerName) // Name of the container

	cmd := exec.CommandContext(ctx, r.path, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return runRunscCommand(ctx, "runsc checkpoint", containerName, cmd)
}

func runscCheckpointDirectEnabled() bool {
	return parseBoolEnv("ATE_RUNSC_CHECKPOINT_DIRECT", false)
}

func runscCheckpointExcludeCommittedZeroPagesEnabled() bool {
	return parseBoolEnv("ATE_RUNSC_CHECKPOINT_EXCLUDE_ZERO_PAGES", false)
}

func (r *runsc) cmdFsCheckpoint(ctx context.Context, containerName, checkpointPath string, durableDirMounts []string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to run runsc fscheckpoint", slog.String("container", containerName))

	args := []string{
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		// "-debug-log", ateompath.RunscDebugLogDir(r.atespace, r.actorName, containerName)+"/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-root", ateompath.RunSCStateDir(r.atespace, r.actorName),
		"fscheckpoint",
		"-image-path", checkpointPath,
	}
	for _, ddv := range durableDirMounts {
		args = append(args, "-path", ddv)
	}

	// name of the container must be the last parameter.
	args = append(args, containerName)

	cmd := exec.CommandContext(
		ctx,
		r.path,
		args...,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return runRunscCommand(ctx, "runsc fscheckpoint", containerName, cmd)
}

// We take a checkpoint only of the root container of the sandbox, but we need
// to call restore on each container, using the same checkpoint.
func (r *runsc) cmdRestore(ctx context.Context, out io.Writer, containerName, checkpointPath string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to run runsc restore", slog.String("container", containerName))

	args := []string{
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		// "-debug-log", ateompath.RunscDebugLogDir(r.atespace, r.actorName, containerName)+"/",
		// "-debug-to-user-log",
		// "-log-packets",
		// "-strace",
		"-root", ateompath.RunSCStateDir(r.atespace, r.actorName),
		"restore",
		"-bundle", ateompath.OCIBundlePath(r.atespace, r.actorName, containerName),
		"-image-path", checkpointPath,
		"-pid-file", ateompath.PIDFilePath(r.atespace, r.actorName, containerName),
	}
	if runscRestoreBackgroundEnabled() {
		args = append(args, "-background", "-detach")
	}
	args = append(args, containerName)

	cmd := exec.CommandContext(ctx, r.path, args...)
	cmd.Stdout = out
	cmd.Stderr = out
	return runRunscCommand(ctx, "runsc restore", containerName, cmd)
}

func runscRestoreBackgroundEnabled() bool {
	return parseBoolEnv("ATE_RUNSC_RESTORE_BACKGROUND", true)
}

func parseBoolEnv(name string, defaultValue bool) bool {
	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		slog.Warn("Invalid boolean environment variable, using default",
			slog.String("name", name),
			slog.String("value", raw),
			slog.Bool("default", defaultValue),
			slog.Any("err", err))
		return defaultValue
	}
	return enabled
}

func (r *runsc) cmdDelete(ctx context.Context, containerName string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	// token := rand.Text()
	// logFile := "/tmp/runsc.delete." + token + ".log"

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		// "-debug",
		"-root", ateompath.RunSCStateDir(r.atespace, r.actorName),
		"delete",
		"-force",
		containerName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return runRunscCommand(ctx, "runsc delete", containerName, cmd)
}

func (r *runsc) cmdState(ctx context.Context, containerName string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir(r.atespace, r.actorName),
		"state",
		containerName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return runRunscCommand(ctx, "runsc state", containerName, cmd)
}

func runRunscCommand(ctx context.Context, operation, containerName string, cmd *exec.Cmd) error {
	start := time.Now()
	err := cmd.Run()
	attrs := []any{
		slog.String("operation", operation),
		slog.String("container", containerName),
		slog.Duration("wall", time.Since(start)),
	}
	if ps := cmd.ProcessState; ps != nil {
		attrs = append(attrs,
			slog.Duration("user_cpu", ps.UserTime()),
			slog.Duration("system_cpu", ps.SystemTime()),
			slog.Bool("success", ps.Success()))
	}
	if err != nil {
		attrs = append(attrs, slog.Any("err", err))
		slog.WarnContext(ctx, "runsc command finished with error", attrs...)
		return fmt.Errorf("while running `%s`: %w", operation, err)
	}
	slog.InfoContext(ctx, "runsc command finished", attrs...)
	return nil
}
