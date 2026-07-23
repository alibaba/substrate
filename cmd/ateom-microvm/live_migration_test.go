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
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMigrationReceiverForTCPUsesUnixSocketBehindProxy(t *testing.T) {
	vmDir := t.TempDir()

	receiver, err := newMigrationReceiver("tcp:0.0.0.0:19000", vmDir)
	if err != nil {
		t.Fatalf("newMigrationReceiver: %v", err)
	}
	if receiver.chURL != "unix:"+filepath.Join(vmDir, "live-migration.sock") {
		t.Fatalf("chURL=%q, want unix socket under vm dir", receiver.chURL)
	}
	if receiver.listenNetwork != "tcp" || receiver.listenAddress != "0.0.0.0:19000" {
		t.Fatalf("listen=(%q,%q), want tcp 0.0.0.0:19000", receiver.listenNetwork, receiver.listenAddress)
	}
}

func TestMigrationReceiverForUnixDoesNotNeedProxy(t *testing.T) {
	receiver, err := newMigrationReceiver("unix:/tmp/migrate.sock", t.TempDir())
	if err != nil {
		t.Fatalf("newMigrationReceiver: %v", err)
	}
	if receiver.chURL != "unix:/tmp/migrate.sock" {
		t.Fatalf("chURL=%q, want original unix URL", receiver.chURL)
	}
	if receiver.listenNetwork != "" || receiver.listenAddress != "" {
		t.Fatalf("listen=(%q,%q), want no proxy listener", receiver.listenNetwork, receiver.listenAddress)
	}
}

func TestMigrationReceiverProxyForwardsTCPToUnix(t *testing.T) {
	vmDir, err := os.MkdirTemp("/tmp", "lm-")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(vmDir)
	unixPath := filepath.Join(vmDir, "live-migration.sock")
	unixLis, err := net.Listen("unix", unixPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer unixLis.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := unixLis.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, len("ping"))
		if _, err := conn.Read(buf); err != nil {
			t.Errorf("unix read: %v", err)
			return
		}
		if string(buf) != "ping" {
			t.Errorf("unix read %q, want ping", string(buf))
			return
		}
		if _, err := conn.Write([]byte("pong")); err != nil {
			t.Errorf("unix write: %v", err)
		}
	}()

	portLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve tcp port: %v", err)
	}
	tcpAddr := portLis.Addr().String()
	if err := portLis.Close(); err != nil {
		t.Fatalf("close reserved tcp port: %v", err)
	}

	receiver, err := newMigrationReceiver("tcp:"+tcpAddr, vmDir)
	if err != nil {
		t.Fatalf("newMigrationReceiver: %v", err)
	}
	stop, err := receiver.startProxy(context.Background())
	if err != nil {
		t.Fatalf("startProxy: %v", err)
	}
	defer stop()

	conn, err := net.DialTimeout("tcp", tcpAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("tcp write: %v", err)
	}
	buf := make([]byte, len("pong"))
	if _, err := conn.Read(buf); err != nil {
		t.Fatalf("tcp read: %v", err)
	}
	if string(buf) != "pong" {
		t.Fatalf("tcp read %q, want pong", string(buf))
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("unix server did not finish")
	}
}

func TestMigrationSenderForTCPUsesLoopbackProxy(t *testing.T) {
	sender, err := newMigrationSender("tcp:10.0.0.2:19000")
	if err != nil {
		t.Fatalf("newMigrationSender: %v", err)
	}
	if sender.destinationAddress != "10.0.0.2:19000" {
		t.Fatalf("destinationAddress=%q, want 10.0.0.2:19000", sender.destinationAddress)
	}
	if sender.listenNetwork != "tcp" || sender.listenAddress != "127.0.0.1:0" {
		t.Fatalf("listen=(%q,%q), want tcp 127.0.0.1:0", sender.listenNetwork, sender.listenAddress)
	}
	if sender.chURL != "" {
		t.Fatalf("chURL=%q before start, want empty", sender.chURL)
	}
}

func TestMigrationSenderForUnixDoesNotNeedProxy(t *testing.T) {
	sender, err := newMigrationSender("unix:/tmp/migrate.sock")
	if err != nil {
		t.Fatalf("newMigrationSender: %v", err)
	}
	if sender.chURL != "unix:/tmp/migrate.sock" {
		t.Fatalf("chURL=%q, want original unix URL", sender.chURL)
	}
	if sender.listenAddress != "" || sender.destinationAddress != "" {
		t.Fatalf("listenAddress=%q destinationAddress=%q, want no proxy", sender.listenAddress, sender.destinationAddress)
	}
}

func TestMigrationSenderProxyForwardsTCPToTCP(t *testing.T) {
	dstLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen destination tcp: %v", err)
	}
	defer dstLis.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := dstLis.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, len("ping"))
		if _, err := conn.Read(buf); err != nil {
			t.Errorf("destination read: %v", err)
			return
		}
		if string(buf) != "ping" {
			t.Errorf("destination read %q, want ping", string(buf))
			return
		}
		if _, err := conn.Write([]byte("pong")); err != nil {
			t.Errorf("destination write: %v", err)
		}
	}()

	sender, err := newMigrationSender("tcp:" + dstLis.Addr().String())
	if err != nil {
		t.Fatalf("newMigrationSender: %v", err)
	}
	chURL, stop, err := sender.startProxy(context.Background())
	if err != nil {
		t.Fatalf("startProxy: %v", err)
	}
	defer stop()

	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(chURL, "tcp:"), 2*time.Second)
	if err != nil {
		t.Fatalf("dial source proxy: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("proxy write: %v", err)
	}
	buf := make([]byte, len("pong"))
	if _, err := conn.Read(buf); err != nil {
		t.Fatalf("proxy read: %v", err)
	}
	if string(buf) != "pong" {
		t.Fatalf("proxy read %q, want pong", string(buf))
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("destination server did not finish")
	}
}

func TestMigrationSenderProxyRetriesDelayedDestination(t *testing.T) {
	portLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve destination tcp port: %v", err)
	}
	dstAddr := portLis.Addr().String()
	if err := portLis.Close(); err != nil {
		t.Fatalf("close reserved destination tcp port: %v", err)
	}

	sender, err := newMigrationSender("tcp:" + dstAddr)
	if err != nil {
		t.Fatalf("newMigrationSender: %v", err)
	}
	chURL, stop, err := sender.startProxy(context.Background())
	if err != nil {
		t.Fatalf("startProxy: %v", err)
	}
	defer stop()

	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(chURL, "tcp:"), 2*time.Second)
	if err != nil {
		t.Fatalf("dial source proxy: %v", err)
	}
	defer conn.Close()

	done := make(chan struct{})
	time.AfterFunc(150*time.Millisecond, func() {
		dstLis, err := net.Listen("tcp", dstAddr)
		if err != nil {
			t.Errorf("listen delayed destination tcp: %v", err)
			close(done)
			return
		}
		defer dstLis.Close()
		defer close(done)

		dstConn, err := dstLis.Accept()
		if err != nil {
			t.Errorf("destination accept: %v", err)
			return
		}
		defer dstConn.Close()
		buf := make([]byte, len("ping"))
		if _, err := dstConn.Read(buf); err != nil {
			t.Errorf("destination read: %v", err)
			return
		}
		if string(buf) != "ping" {
			t.Errorf("destination read %q, want ping", string(buf))
			return
		}
		if _, err := dstConn.Write([]byte("pong")); err != nil {
			t.Errorf("destination write: %v", err)
		}
	})

	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("proxy write: %v", err)
	}
	buf := make([]byte, len("pong"))
	if _, err := conn.Read(buf); err != nil {
		t.Fatalf("proxy read: %v", err)
	}
	if string(buf) != "pong" {
		t.Fatalf("proxy read %q, want pong", string(buf))
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("destination server did not finish")
	}
}
