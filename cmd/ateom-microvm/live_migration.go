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
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/readyz"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *AteomService) ReceiveLiveMigration(ctx context.Context, req *ateompb.ReceiveLiveMigrationRequest) (resp *ateompb.ReceiveLiveMigrationResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !liveMigrationTapNetEnabled() {
		return nil, status.Error(codes.FailedPrecondition, "microVM live migration requires ATE_CH_TAP_NET=1")
	}
	if req.GetReceiverUrl() == "" {
		return nil, status.Error(codes.InvalidArgument, "receiver_url is required")
	}

	atespace := req.GetAtespace()
	name := req.GetActorName()
	templateNS := req.GetActorTemplateNamespace()
	templateName := req.GetActorTemplateName()
	tStart := time.Now()
	s.actorLogger.EmitLifecycleLog("Actor live migration receiving", atespace, name, templateNS, templateName)

	containers := req.GetSpec().GetContainers()
	if len(containers) == 0 {
		return nil, status.Error(codes.InvalidArgument, "actor spec has no containers")
	}
	if len(containers) > maxActorContainers {
		return nil, status.Errorf(codes.Unimplemented, "ateom-microvm supports at most %d containers, got %d", maxActorContainers, len(containers))
	}

	rr := s.resolveRuntime(req.GetRuntimeAssetPaths())
	kata.CleanupSandboxState(ctx, name)
	if err := os.MkdirAll(kata.VMDir(name), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}
	ctrs, err := s.buildActorContainers(atespace, name, containers)
	if err != nil {
		return nil, err
	}
	vfsdCmd, err := s.stageOverlayLowers(ctx, rr, name, ctrs)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil && vfsdCmd.Process != nil {
			_ = vfsdCmd.Process.Kill()
			_, _ = vfsdCmd.Process.Wait()
		}
	}()

	if err := s.setupActorNetwork(ctx); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			if cleanupErr := s.cleanupActorNetwork(ctx); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to clean up actor network after live migration receive failure", slog.Any("err", cleanupErr))
			}
		}
	}()

	tapFiles, err := s.setupRestoreTap(ctx, "tap0_kata", 1)
	if err != nil {
		return nil, fmt.Errorf("while building tap: %w", err)
	}
	defer func() {
		for _, f := range tapFiles {
			_ = f.Close()
		}
	}()

	apiSocket := filepath.Join(kata.VMDir(name), "clh-api-live-recv.sock")
	var chCmd *exec.Cmd
	var client *ch.Client
	launch := func(context.Context) error {
		var launchErr error
		chCmd, client, launchErr = ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
			Binary:    rr.chBinary,
			APISocket: apiSocket,
			Stdout:    slogWriter{ctx},
			Stderr:    slogWriter{ctx},
		})
		return launchErr
	}
	if err := netNSDo(ctx, s.interiorNetNS, launch); err != nil {
		return nil, fmt.Errorf("while launching destination VMM: %w", err)
	}
	defer func() {
		if retErr != nil && chCmd.Process != nil {
			_ = chCmd.Process.Kill()
			_, _ = chCmd.Process.Wait()
		}
	}()

	receiver, err := newMigrationReceiver(req.GetReceiverUrl(), kata.VMDir(name))
	if err != nil {
		return nil, err
	}
	stopProxy, err := receiver.startProxy(ctx)
	if err != nil {
		return nil, err
	}
	defer stopProxy()

	if err := client.ReceiveMigration(ctx, ch.ReceiveMigrationOptions{
		ReceiverURL: receiver.chURL,
		TLSDir:      req.GetTlsDir(),
		MemoryMode:  firstNonEmpty(req.GetMemoryMode(), "Precopy"),
	}); err != nil {
		return nil, fmt.Errorf("while receiving live migration: %w", err)
	}

	if err := readyz.WaitAll(ctx, containers, actorVethIP); err != nil {
		return nil, fmt.Errorf("while waiting for migrated container readyz: %w", err)
	}

	ra := &runningActor{chCmd: chCmd, vfsdCmd: vfsdCmd, apiSocket: apiSocket, baseID: name}
	vsockPath := kata.VsockSocketPath(name)
	logAC, dialErr := dialAgentRetry(ctx, vsockPath, 15*time.Second)
	if dialErr != nil {
		slog.WarnContext(ctx, "post-live-migration agent dial failed; actor log forwarding disabled",
			slog.String("id", name), slog.Any("err", dialErr))
	} else {
		ra.logAgent = logAC
		for _, c := range containers {
			s.startActorLogForwarding(logAC, atespace, name, templateNS, templateName, overlayWorkloadID(c.GetName()), c.GetName())
		}
	}

	s.running[name] = ra
	s.actorLogger.EmitLifecycleLog("Actor live migration received", atespace, name, templateNS, templateName)
	slog.InfoContext(ctx, "Actor live migration received", slog.String("id", name), slog.Duration("total", time.Since(tStart)))
	return &ateompb.ReceiveLiveMigrationResponse{}, nil
}

func (s *AteomService) SendLiveMigration(ctx context.Context, req *ateompb.SendLiveMigrationRequest) (*ateompb.SendLiveMigrationResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !liveMigrationTapNetEnabled() {
		return nil, status.Error(codes.FailedPrecondition, "microVM live migration requires ATE_CH_TAP_NET=1")
	}
	if req.GetActorName() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor_name is required")
	}
	if req.GetDestinationUrl() == "" {
		return nil, status.Error(codes.InvalidArgument, "destination_url is required")
	}

	ra := s.running[req.GetActorName()]
	if ra == nil || ra.apiSocket == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "actor %q is not running on this ateom", req.GetActorName())
	}
	client := ch.NewClient(ra.apiSocket)
	if err := client.WaitReady(ctx, 10*time.Second); err != nil {
		return nil, fmt.Errorf("while waiting for CH api-socket: %w", err)
	}

	tStart := time.Now()
	if err := client.SendMigration(ctx, ch.SendMigrationOptions{
		DestinationURL:  req.GetDestinationUrl(),
		DowntimeMillis:  req.GetDowntimeMs(),
		TimeoutSeconds:  req.GetTimeoutS(),
		TimeoutStrategy: firstNonEmpty(req.GetTimeoutStrategy(), "Cancel"),
		Connections:     req.GetConnections(),
		TLSDir:          req.GetTlsDir(),
		MemoryMode:      firstNonEmpty(req.GetMemoryMode(), "Precopy"),
	}); err != nil {
		return nil, fmt.Errorf("while sending live migration: %w", err)
	}

	s.teardownActor(ctx, req.GetActorName(), ra, nil)
	delete(s.running, req.GetActorName())
	if err := s.cleanupActorNetwork(ctx); err != nil {
		slog.WarnContext(ctx, "Failed to clean up actor network after live migration send", slog.Any("err", err))
	}
	slog.InfoContext(ctx, "Actor live migration sent", slog.String("id", req.GetActorName()), slog.Duration("total", time.Since(tStart)))
	return &ateompb.SendLiveMigrationResponse{}, nil
}
