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
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type migrationReceiver struct {
	chURL         string
	listenNetwork string
	listenAddress string
	unixPath      string
}

func newMigrationReceiver(receiverURL, vmDir string) (migrationReceiver, error) {
	switch {
	case strings.HasPrefix(receiverURL, "unix:"):
		return migrationReceiver{chURL: receiverURL}, nil
	case strings.HasPrefix(receiverURL, "tcp:"):
		addr := strings.TrimPrefix(receiverURL, "tcp:")
		if addr == "" {
			return migrationReceiver{}, status.Error(codes.InvalidArgument, "tcp receiver_url missing address")
		}
		unixPath := filepath.Join(vmDir, "live-migration.sock")
		return migrationReceiver{
			chURL:         "unix:" + unixPath,
			listenNetwork: "tcp",
			listenAddress: addr,
			unixPath:      unixPath,
		}, nil
	default:
		return migrationReceiver{}, status.Errorf(codes.InvalidArgument, "unsupported receiver_url %q", receiverURL)
	}
}

func (r migrationReceiver) startProxy(ctx context.Context) (func(), error) {
	if r.listenNetwork == "" {
		return func() {}, nil
	}
	lis, err := net.Listen(r.listenNetwork, r.listenAddress)
	if err != nil {
		return nil, fmt.Errorf("while listening for live migration proxy on %s/%s: %w", r.listenNetwork, r.listenAddress, err)
	}
	proxyCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := lis.Accept()
			if err != nil {
				if proxyCtx.Err() != nil {
					return
				}
				slog.WarnContext(proxyCtx, "live migration proxy accept failed", slog.Any("err", err))
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				proxyMigrationConn(proxyCtx, conn, r.unixPath)
			}()
		}
	}()
	return func() {
		cancel()
		_ = lis.Close()
		wg.Wait()
	}, nil
}

func proxyMigrationConn(ctx context.Context, src net.Conn, unixPath string) {
	defer src.Close()
	dst, err := dialUnixRetry(ctx, unixPath, liveMigrationUnixDialTimeout())
	if err != nil {
		slog.WarnContext(ctx, "live migration proxy failed to connect to CH unix receiver", slog.String("unix", unixPath), slog.Any("err", err))
		return
	}
	defer dst.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(dst, src)
		_ = closeWrite(dst)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(src, dst)
		_ = closeWrite(src)
	}()
	wg.Wait()
}

func dialUnixRetry(ctx context.Context, path string, timeout time.Duration) (net.Conn, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.Dial("unix", path)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("unix receiver %q not ready after %s: %w", path, timeout, lastErr)
}

func closeWrite(conn net.Conn) error {
	type closeWriter interface {
		CloseWrite() error
	}
	if cw, ok := conn.(closeWriter); ok {
		return cw.CloseWrite()
	}
	return conn.Close()
}
