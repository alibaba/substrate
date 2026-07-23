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
	"strings"
	"sync"
	"time"
)

type migrationSender struct {
	chURL              string
	listenNetwork      string
	listenAddress      string
	destinationAddress string
}

func newMigrationSender(destinationURL string) (migrationSender, error) {
	switch {
	case strings.HasPrefix(destinationURL, "unix:"):
		return migrationSender{chURL: destinationURL}, nil
	case strings.HasPrefix(destinationURL, "tcp:"):
		addr := strings.TrimPrefix(destinationURL, "tcp:")
		if addr == "" {
			return migrationSender{}, fmt.Errorf("tcp destination_url missing address")
		}
		return migrationSender{
			listenNetwork:      "tcp",
			listenAddress:      "127.0.0.1:0",
			destinationAddress: addr,
		}, nil
	default:
		return migrationSender{}, fmt.Errorf("unsupported destination_url %q", destinationURL)
	}
}

func (s migrationSender) startProxy(ctx context.Context) (string, func(), error) {
	return s.startProxyWithListen(ctx, net.Listen)
}

func (s migrationSender) startProxyWithListen(ctx context.Context, listen func(network, address string) (net.Listener, error)) (string, func(), error) {
	if s.listenAddress == "" {
		return s.chURL, func() {}, nil
	}
	lis, err := listen(s.listenNetwork, s.listenAddress)
	if err != nil {
		return "", nil, fmt.Errorf("while listening for live migration sender proxy on %s: %w", s.listenAddress, err)
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
				slog.WarnContext(proxyCtx, "live migration sender proxy accept failed", slog.Any("err", err))
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				proxyMigrationTCPConn(proxyCtx, conn, s.destinationAddress)
			}()
		}
	}()
	stop := func() {
		cancel()
		_ = lis.Close()
		wg.Wait()
	}
	return "tcp:" + lis.Addr().String(), stop, nil
}

func proxyMigrationTCPConn(ctx context.Context, src net.Conn, destinationAddress string) {
	defer src.Close()
	dst, err := dialTCPRetry(ctx, destinationAddress, 10*time.Second)
	if err != nil {
		slog.WarnContext(ctx, "live migration sender proxy failed to connect to destination",
			slog.String("destination", destinationAddress), slog.Any("err", err))
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

func dialTCPRetry(ctx context.Context, address string, timeout time.Duration) (net.Conn, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	var d net.Dialer
	for time.Now().Before(deadline) {
		conn, err := d.DialContext(ctx, "tcp", address)
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
	return nil, fmt.Errorf("tcp destination %q not ready after %s: %w", address, timeout, lastErr)
}
