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

package router

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
)

func TestDirectProxyRetriesIdempotentRequestAfterRouteSwitch(t *testing.T) {
	var routeCalls atomic.Int32
	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			call := routeCalls.Add(1)
			ip := "10.0.0.1"
			gen := int64(1)
			if call > 1 {
				ip = "10.0.0.2"
				gen = 2
			}
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: ip},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: gen,
					},
				},
			}, nil
		},
	}
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Host == "10.0.0.1:80" {
				return nil, fmt.Errorf("source closed")
			}
			if req.URL.Host != "10.0.0.2:80" {
				t.Fatalf("unexpected upstream host %q", req.URL.Host)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("target")),
				Request:    req,
			}, nil
		}),
	}

	req := httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	rec := httptest.NewRecorder()

	srv.handleDirectProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q, want 200", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "target" {
		t.Fatalf("body = %q, want target", rec.Body.String())
	}
	if got := routeCalls.Load(); got < 2 {
		t.Fatalf("route calls = %d, want at least 2", got)
	}
}

func TestDirectProxyAddsRouteTraceHeaders(t *testing.T) {
	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active: &ateapipb.RouteTarget{
							AteomPodNamespace: "ate-system",
							AteomPodName:      "worker-b",
							AteomPodIp:        "10.0.0.2",
							WorkerPoolName:    "pool-a",
							NodeName:          "node-b",
						},
						Phase:      ateapipb.ActorRoute_PHASE_SWITCHED,
						Generation: 7,
					},
				},
			}, nil
		},
	}
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("target")),
				Request:    req,
			}, nil
		}),
	}

	req := httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	rec := httptest.NewRecorder()

	srv.handleDirectProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q, want 200", rec.Code, rec.Body.String())
	}
	for name, want := range map[string]string{
		"X-Substrate-Route-Generation":    "7",
		"X-Substrate-Route-Phase":         "PHASE_SWITCHED",
		"X-Substrate-Route-Target-Worker": "ate-system/worker-b",
		"X-Substrate-Route-Target-Node":   "node-b",
		"X-Substrate-Route-Target-IP":     "10.0.0.2",
	} {
		if got := rec.Header().Get(name); got != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
}

func TestDirectProxyStreamsEventStreamBeforeUpstreamEnds(t *testing.T) {
	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.2"},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: 7,
					},
				},
			}, nil
		},
	}
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			pr, pw := io.Pipe()
			go func() {
				_, _ = pw.Write([]byte("data: first\n\n"))
				<-req.Context().Done()
				_ = pw.Close()
			}()
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       pr,
				Request:    req,
			}, nil
		}),
	}
	server := httptest.NewServer(http.HandlerFunc(srv.handleDirectProxy))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/stream", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("GET /stream: %v", err)
	}
	defer resp.Body.Close()

	lineCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		line, readErr := bufio.NewReader(resp.Body).ReadString('\n')
		if readErr != nil {
			errCh <- readErr
			return
		}
		lineCh <- line
	}()
	select {
	case line := <-lineCh:
		if line != "data: first\n" {
			t.Fatalf("first line = %q, want SSE data line", line)
		}
	case err := <-errCh:
		t.Fatalf("read first SSE line: %v", err)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("timed out waiting for first SSE line; proxy likely buffered the upstream body")
	}
}

func TestDirectProxyTunnelsWebSocketUpgrade(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			t.Fatalf("upgrade upstream websocket: %v", err)
		}
		defer conn.Close()
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err := conn.WriteMessage(messageType, append([]byte("echo:"), payload...)); err != nil {
				return
			}
		}
	}))
	defer upstream.Close()
	targetAddr := strings.TrimPrefix(upstream.URL, "http://")

	srv := &RouterServer{}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		target := routeTarget{IP: "127.0.0.1", Port: "80", Generation: 3, Phase: ateapipb.ActorRoute_PHASE_ACTIVE}
		if err := srv.proxyDirectRawUpgrade(w, req, targetAddr, target); err != nil {
			t.Errorf("proxyDirectUpgrade: %v", err)
		}
	}))
	defer proxy.Close()

	wsURL := "ws" + strings.TrimPrefix(proxy.URL, "http") + "/terminal/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Host": []string{"my-actor.team-a.actors.resources.substrate.ate.dev"}})
	if err != nil {
		t.Fatalf("dial proxy websocket: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("client-seq=1")); err != nil {
		t.Fatalf("write websocket message: %v", err)
	}
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket echo: %v", err)
	}
	if string(payload) != "echo:client-seq=1" {
		t.Fatalf("websocket payload=%q, want echo", payload)
	}
}

func TestDirectProxyTerminalWebSocketReplaysPendingInputAfterReconnect(t *testing.T) {
	t.Setenv("ATENET_DIRECT_PROXY_WEBSOCKET_IDLE_TIMEOUT_MS", "100")
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			t.Fatalf("upgrade source websocket: %v", err)
		}
		defer conn.Close()
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if string(payload) == "client-seq=1" {
				if err := conn.WriteMessage(messageType, []byte(`{"type":"echo","input":"client-seq=1"}`)); err != nil {
					return
				}
				continue
			}
			if string(payload) == "client-seq=2" {
				select {
				case <-req.Context().Done():
				case <-time.After(time.Second):
				}
				return
			}
		}
	}))
	defer source.Close()

	targetSawReplay := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			t.Fatalf("upgrade target websocket: %v", err)
		}
		defer conn.Close()
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if string(payload) == "client-seq=2" {
				select {
				case targetSawReplay <- struct{}{}:
				default:
				}
			}
			if err := conn.WriteMessage(messageType, []byte(`{"type":"echo","input":"`+string(payload)+`"}`)); err != nil {
				return
			}
		}
	}))
	defer target.Close()

	srv := &RouterServer{}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		downstream, err := directProxyWebSocketUpgrader.Upgrade(w, req, nil)
		if err != nil {
			t.Errorf("upgrade downstream websocket: %v", err)
			return
		}
		defer downstream.Close()

		ctx := req.Context()
		clientMessages := make(chan directProxyWSMessage, 8)
		clientErr := make(chan error, 1)
		go func() {
			for {
				messageType, payload, err := downstream.ReadMessage()
				if err != nil {
					clientErr <- err
					return
				}
				clientMessages <- directProxyWSMessage{messageType: messageType, payload: append([]byte(nil), payload...)}
			}
		}()

		state := &directProxyTerminalState{pending: map[int]directProxyWSMessage{}}
		sourceConn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(source.URL, "http")+"/terminal/ws", nil)
		if err != nil {
			t.Errorf("dial source: %v", err)
			return
		}
		reconnect, err := srv.proxyDirectTerminalWebSocketOnce(ctx, downstream, sourceConn, clientMessages, clientErr, state)
		_ = sourceConn.Close()
		if err != nil {
			t.Errorf("proxy source websocket: %v", err)
			return
		}
		if !reconnect {
			t.Errorf("proxy source reconnect=false, want true")
			return
		}
		targetConn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(target.URL, "http")+"/terminal/ws", nil)
		if err != nil {
			t.Errorf("dial target: %v", err)
			return
		}
		_, err = srv.proxyDirectTerminalWebSocketOnce(ctx, downstream, targetConn, clientMessages, clientErr, state)
		_ = targetConn.Close()
		if err != nil {
			t.Errorf("proxy target websocket: %v", err)
		}
	}))
	defer proxy.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(proxy.URL, "http")+"/terminal/ws", nil)
	if err != nil {
		t.Fatalf("dial proxy websocket: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("client-seq=1")); err != nil {
		t.Fatalf("write seq1: %v", err)
	}
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read seq1 echo: %v", err)
	}
	if !strings.Contains(string(payload), "client-seq=1") {
		t.Fatalf("seq1 echo payload=%q", payload)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("client-seq=2")); err != nil {
		t.Fatalf("write seq2: %v", err)
	}
	_, payload, err = conn.ReadMessage()
	if err != nil {
		t.Fatalf("read replayed seq2 echo: %v", err)
	}
	if !strings.Contains(string(payload), "client-seq=2") {
		t.Fatalf("seq2 echo payload=%q", payload)
	}
	select {
	case <-targetSawReplay:
	case <-time.After(time.Second):
		t.Fatal("target did not receive replayed client-seq=2")
	}
}

func TestDrainQueuedTerminalClientMessagesPreservesPendingInputBeforeReconnect(t *testing.T) {
	clientMessages := make(chan directProxyWSMessage, 2)
	clientMessages <- directProxyWSMessage{messageType: websocket.TextMessage, payload: []byte("client-seq=54 client_unix_nano=1784995855900000000")}
	clientMessages <- directProxyWSMessage{messageType: websocket.TextMessage, payload: []byte("client-seq=55 client_unix_nano=1784995856000000000")}

	state := &directProxyTerminalState{pending: map[int]directProxyWSMessage{}}
	drainQueuedTerminalClientMessages(clientMessages, state)

	if _, ok := state.pending[54]; !ok {
		t.Fatalf("pending missing client-seq=54: %+v", state.pending)
	}
	if _, ok := state.pending[55]; !ok {
		t.Fatalf("pending missing client-seq=55: %+v", state.pending)
	}
}

func TestTerminalTickSequenceParsesTickFramesOnly(t *testing.T) {
	seq, ok := terminalTickSequence([]byte(`{"type":"tick","terminal_sequence":52}`))
	if !ok || seq != 52 {
		t.Fatalf("terminalTickSequence tick = %d/%v, want 52/true", seq, ok)
	}
	if _, ok := terminalTickSequence([]byte(`{"type":"echo","terminal_sequence":52,"input":"client-seq=1"}`)); ok {
		t.Fatal("terminalTickSequence accepted echo frame")
	}
}

func TestTerminalClientSequenceAllowsTimingSuffix(t *testing.T) {
	seq, ok := terminalClientSequence([]byte("client-seq=42 client_unix_nano=1785000000123456789"))

	if !ok || seq != 42 {
		t.Fatalf("terminalClientSequence()=%d/%v, want 42/true", seq, ok)
	}
}

func TestDirectProxyReconnectsSSEAndDropsDuplicateSequence(t *testing.T) {
	var routeCalls atomic.Int32
	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			call := routeCalls.Add(1)
			ip := "10.0.0.1"
			gen := int64(1)
			if call > 1 {
				ip = "10.0.0.2"
				gen = 2
			}
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: ip},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: gen,
					},
				},
			}, nil
		},
	}
	var upstreamCalls atomic.Int32
	var upstreamLastEventIDsMu sync.Mutex
	var upstreamLastEventIDs []string
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			call := upstreamCalls.Add(1)
			upstreamLastEventIDsMu.Lock()
			upstreamLastEventIDs = append(upstreamLastEventIDs, req.Header.Get("Last-Event-ID"))
			upstreamLastEventIDsMu.Unlock()
			body := "event: state\ndata: {\"sequence\":1}\n\nevent: state\ndata: {\"sequence\":2}\n\n"
			if call > 1 {
				body = "event: state\ndata: {\"sequence\":2}\n\nevent: state\ndata: {\"sequence\":3}\n\n"
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    req,
			}, nil
		}),
	}
	server := httptest.NewServer(http.HandlerFunc(srv.handleDirectProxy))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/stream", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("GET /stream: %v", err)
	}
	defer resp.Body.Close()

	var got []string
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			got = append(got, line)
			if len(got) == 3 {
				break
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan SSE response: %v", err)
	}
	want := []string{
		`data: {"sequence":1}`,
		`data: {"sequence":2}`,
		`data: {"sequence":3}`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("SSE data lines = %q, want %q", got, want)
	}
	if got := upstreamCalls.Load(); got < 2 {
		t.Fatalf("upstream calls = %d, want reconnect", got)
	}
	upstreamLastEventIDsMu.Lock()
	defer upstreamLastEventIDsMu.Unlock()
	if len(upstreamLastEventIDs) < 2 {
		t.Fatalf("upstream Last-Event-ID samples=%v, want at least two upstream calls", upstreamLastEventIDs)
	}
	if got := upstreamLastEventIDs[1]; got != "2" {
		t.Fatalf("second upstream Last-Event-ID=%q, want 2", got)
	}
}

func TestDirectProxyReconnectsSSEWhenUpstreamGoesIdle(t *testing.T) {
	t.Setenv("ATENET_DIRECT_PROXY_SSE_IDLE_TIMEOUT_MS", "30")
	t.Setenv("ATENET_DIRECT_PROXY_RETRY_DELAY_MS", "1")

	var routeCalls atomic.Int32
	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			call := routeCalls.Add(1)
			ip := "10.0.0.1"
			gen := int64(1)
			if call > 1 {
				ip = "10.0.0.2"
				gen = 2
			}
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: ip},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: gen,
					},
				},
			}, nil
		},
	}
	var upstreamCalls atomic.Int32
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			call := upstreamCalls.Add(1)
			if call == 1 {
				pr, pw := io.Pipe()
				go func() {
					_, _ = pw.Write([]byte("event: state\ndata: {\"sequence\":1}\n\n"))
					<-req.Context().Done()
					_ = pw.Close()
				}()
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
					Body:       pr,
					Request:    req,
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader("event: state\ndata: {\"sequence\":1}\n\nevent: state\ndata: {\"sequence\":2}\n\n")),
				Request:    req,
			}, nil
		}),
	}
	server := httptest.NewServer(http.HandlerFunc(srv.handleDirectProxy))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/stream", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("GET /stream: %v", err)
	}
	defer resp.Body.Close()

	var got []string
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			got = append(got, line)
			if len(got) == 2 {
				break
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan SSE response: %v", err)
	}
	want := []string{
		`data: {"sequence":1}`,
		`data: {"sequence":2}`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("SSE data lines = %q, want %q", got, want)
	}
	if got := upstreamCalls.Load(); got < 2 {
		t.Fatalf("upstream calls = %d, want idle reconnect", got)
	}
}

func TestDirectProxyRetryTimingCanBeTunedByEnvironment(t *testing.T) {
	t.Setenv("ATENET_DIRECT_PROXY_RETRY_BUDGET_MS", "30000")
	t.Setenv("ATENET_DIRECT_PROXY_MIGRATION_ATTEMPT_TIMEOUT_MS", "80")
	t.Setenv("ATENET_DIRECT_PROXY_RETRY_DELAY_MS", "20")
	t.Setenv("ATENET_DIRECT_PROXY_SSE_IDLE_TIMEOUT_MS", "700")

	if got := directProxyRetryBudgetValue(); got != 30*time.Second {
		t.Fatalf("retry budget = %s, want 30s", got)
	}
	if got := directProxyMigrationAttemptTimeoutValue(); got != 80*time.Millisecond {
		t.Fatalf("attempt timeout = %s, want 80ms", got)
	}
	if got := directProxyRetryDelayValue(); got != 20*time.Millisecond {
		t.Fatalf("retry delay = %s, want 20ms", got)
	}
	if got := directProxySSEIdleTimeoutValue(); got != 700*time.Millisecond {
		t.Fatalf("SSE idle timeout = %s, want 700ms", got)
	}
}

func TestDirectProxyRetryTimingFallsBackOnInvalidEnvironment(t *testing.T) {
	t.Setenv("ATENET_DIRECT_PROXY_RETRY_BUDGET_MS", "-1")
	t.Setenv("ATENET_DIRECT_PROXY_MIGRATION_ATTEMPT_TIMEOUT_MS", "0")
	t.Setenv("ATENET_DIRECT_PROXY_RETRY_DELAY_MS", "nope")
	t.Setenv("ATENET_DIRECT_PROXY_SSE_IDLE_TIMEOUT_MS", "-2")

	if got := directProxyRetryBudgetValue(); got != directProxyRetryBudget {
		t.Fatalf("retry budget = %s, want default %s", got, directProxyRetryBudget)
	}
	if got := directProxyMigrationAttemptTimeoutValue(); got != directProxyMigrationAttemptTimeout {
		t.Fatalf("attempt timeout = %s, want default %s", got, directProxyMigrationAttemptTimeout)
	}
	if got := directProxyRetryDelayValue(); got != directProxyRetryDelay {
		t.Fatalf("retry delay = %s, want default %s", got, directProxyRetryDelay)
	}
	if got := directProxySSEIdleTimeoutValue(); got != directProxySSEIdleTimeout {
		t.Fatalf("SSE idle timeout = %s, want default %s", got, directProxySSEIdleTimeout)
	}
}

func TestDirectProxyDoesNotBoundActiveEventStreamAttempt(t *testing.T) {
	t.Setenv("ATENET_DIRECT_PROXY_BOUND_ACTIVE_RETRYABLE_ATTEMPT", "1")

	req := httptest.NewRequest(http.MethodGet, "http://actor.space.actors.resources.substrate.ate.dev/stream", nil)
	req.Header.Set("Accept", "text/event-stream")

	if shouldBoundDirectProxyAttempt(req, ateapipb.ActorRoute_PHASE_ACTIVE, false) {
		t.Fatal("ACTIVE SSE requests must not use the short retryable attempt timeout")
	}
}

func TestDirectProxyLimitsConcurrentUpstreamAttemptsPerActor(t *testing.T) {
	t.Setenv("ATENET_DIRECT_PROXY_MAX_UPSTREAM_PER_ACTOR", "2")
	t.Setenv("ATENET_DIRECT_PROXY_BOUND_ACTIVE_RETRYABLE_ATTEMPT", "1")
	t.Setenv("ATENET_DIRECT_PROXY_MIGRATION_ATTEMPT_TIMEOUT_MS", "500")

	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.1"},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: 1,
					},
				},
			}, nil
		},
	}
	var current atomic.Int32
	var maxSeen atomic.Int32
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv:         &ExtProcServer{routeResolver: resolver},
		inflight:           newInFlightTracker(),
		directProxyLimiter: newDirectProxyUpstreamLimiter(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			now := current.Add(1)
			for {
				max := maxSeen.Load()
				if now <= max || maxSeen.CompareAndSwap(max, now) {
					break
				}
			}
			defer current.Add(-1)
			time.Sleep(80 * time.Millisecond)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("target")),
				Request:    req,
			}, nil
		}),
	}
	server := httptest.NewServer(http.HandlerFunc(srv.handleDirectProxy))
	defer server.Close()

	var wg sync.WaitGroup
	errCh := make(chan string, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/substrate/migration-state", nil)
			if err != nil {
				errCh <- err.Error()
				return
			}
			req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
			resp, err := server.Client().Do(req)
			if err != nil {
				errCh <- err.Error()
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK || string(body) != "target" {
				errCh <- fmt.Sprintf("status=%d body=%q", resp.StatusCode, string(body))
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if got := maxSeen.Load(); got > 2 {
		t.Fatalf("max concurrent upstream attempts = %d, want <= 2", got)
	}
}

func TestDirectProxyStreamUsesSeparateLimiterFromRetryableRequests(t *testing.T) {
	t.Setenv("ATENET_DIRECT_PROXY_MAX_UPSTREAM_PER_ACTOR", "1")
	t.Setenv("ATENET_DIRECT_PROXY_MAX_STREAMS_PER_ACTOR", "1")

	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.1"},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: 1,
					},
				},
			}, nil
		},
	}
	blockGET := make(chan struct{})
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv:         &ExtProcServer{routeResolver: resolver},
		inflight:           newInFlightTracker(),
		directProxyLimiter: newDirectProxyUpstreamLimiter(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/stream" {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
					Body:       io.NopCloser(strings.NewReader("data: first\n\n")),
					Request:    req,
				}, nil
			}
			<-blockGET
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("target")),
				Request:    req,
			}, nil
		}),
	}
	server := httptest.NewServer(http.HandlerFunc(srv.handleDirectProxy))
	defer server.Close()

	getDone := make(chan error, 1)
	go func() {
		req, err := http.NewRequest(http.MethodGet, server.URL+"/substrate/migration-state", nil)
		if err != nil {
			getDone <- err
			return
		}
		req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
		resp, err := server.Client().Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
		getDone <- err
	}()
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/stream", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	req.Header.Set("Accept", "text/event-stream")
	start := time.Now()
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("GET /stream: %v", err)
	}
	defer resp.Body.Close()
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if line != "data: first\n" {
		t.Fatalf("stream line = %q, want first event", line)
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("stream waited behind normal request for %s", elapsed)
	}

	close(blockGET)
	if err := <-getDone; err != nil {
		t.Fatalf("GET /substrate/migration-state: %v", err)
	}
}

func TestDirectProxyDoesNotRetryNonIdempotentRequest(t *testing.T) {
	var routeCalls atomic.Int32
	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			routeCalls.Add(1)
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.1"},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: 1,
					},
				},
			}, nil
		},
	}
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("source closed")
		}),
	}

	req := httptest.NewRequest(http.MethodPost, "http://my-actor.team-a.actors.resources.substrate.ate.dev/increment", strings.NewReader("{}"))
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	rec := httptest.NewRecorder()

	srv.handleDirectProxy(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if got := routeCalls.Load(); got != 1 {
		t.Fatalf("route calls = %d, want 1", got)
	}
}

func TestDirectProxyRetriesIdempotentPost(t *testing.T) {
	var routeCalls atomic.Int32
	var attempts atomic.Int32
	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			routeCalls.Add(1)
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.1"},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: 1,
					},
				},
			}, nil
		},
	}
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if attempts.Add(1) == 1 {
				return nil, fmt.Errorf("source closed")
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"counter":1}`)),
			}, nil
		}),
	}

	req := httptest.NewRequest(http.MethodPost, "http://my-actor.team-a.actors.resources.substrate.ate.dev/increment", strings.NewReader("{}"))
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	req.Header.Set("Idempotency-Key", "mutation-a")
	rec := httptest.NewRecorder()

	srv.handleDirectProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q, want 200", rec.Code, rec.Body.String())
	}
	if got := routeCalls.Load(); got < 2 {
		t.Fatalf("route calls = %d, want retry", got)
	}
}

func TestDirectProxyReplaysIdempotentPostBodyOnRetry(t *testing.T) {
	t.Setenv("ATENET_DIRECT_PROXY_RETRY_DELAY_MS", "1")
	t.Setenv("ATENET_DIRECT_PROXY_RETRY_BUDGET_MS", "500")

	var routeCalls atomic.Int32
	var attempts atomic.Int32
	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			routeCalls.Add(1)
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.1"},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: 1,
					},
				},
			}, nil
		},
	}
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			_ = req.Body.Close()
			if string(body) != `{"payload":"same-body"}` {
				t.Fatalf("attempt %d body = %q, want replayed POST body", attempts.Load()+1, string(body))
			}
			if attempts.Add(1) == 1 {
				return nil, fmt.Errorf("source closed after reading body")
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"stored":true}`)),
			}, nil
		}),
	}

	req := httptest.NewRequest(http.MethodPost, "http://my-actor.team-a.actors.resources.substrate.ate.dev/upload", bytes.NewBufferString(`{"payload":"same-body"}`))
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	req.Header.Set("Idempotency-Key", "upload-a")
	rec := httptest.NewRecorder()

	srv.handleDirectProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q, want 200", rec.Code, rec.Body.String())
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if got := routeCalls.Load(); got < 2 {
		t.Fatalf("route calls = %d, want retry", got)
	}
}

func TestDirectProxyServesStaleCachedGETAfterUpstreamFailure(t *testing.T) {
	t.Setenv("ATENET_DIRECT_PROXY_STALE_CACHE", "1")

	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.1"},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: 1,
					},
				},
			}, nil
		},
	}
	var upstreamCalls atomic.Int32
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if upstreamCalls.Add(1) == 1 {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("cached")),
					Request:    req,
				}, nil
			}
			return nil, fmt.Errorf("source paused")
		}),
	}

	req := httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	first := httptest.NewRecorder()
	srv.handleDirectProxy(first, req)
	if first.Code != http.StatusOK || first.Body.String() != "cached" {
		t.Fatalf("first response = %d %q, want 200 cached", first.Code, first.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	second := httptest.NewRecorder()
	srv.handleDirectProxy(second, req)

	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d body=%q, want stale 200", second.Code, second.Body.String())
	}
	if second.Body.String() != "cached" {
		t.Fatalf("second body = %q, want cached", second.Body.String())
	}
	if second.Header().Get("X-Substrate-Stale") != "true" {
		t.Fatalf("X-Substrate-Stale = %q, want true", second.Header().Get("X-Substrate-Stale"))
	}
}

func TestDirectProxyServesStaleCachedGETBeforeUpstreamDuringDrain(t *testing.T) {
	t.Setenv("ATENET_DIRECT_PROXY_STALE_CACHE", "1")

	var routeCalls atomic.Int32
	var upstreamCalls atomic.Int32
	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			phase := ateapipb.ActorRoute_PHASE_ACTIVE
			if routeCalls.Add(1) > 1 {
				phase = ateapipb.ActorRoute_PHASE_DRAINING
			}
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.1"},
						Phase:      phase,
						Generation: 1,
					},
				},
			}, nil
		},
	}
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if upstreamCalls.Add(1) > 1 {
				t.Fatal("upstream should not be called while serving cached GET during drain")
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("cached")),
				Request:    req,
			}, nil
		}),
	}

	req := httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	first := httptest.NewRecorder()
	srv.handleDirectProxy(first, req)
	if first.Code != http.StatusOK || first.Body.String() != "cached" {
		t.Fatalf("first response = %d %q, want 200 cached", first.Code, first.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	second := httptest.NewRecorder()
	srv.handleDirectProxy(second, req)

	if second.Code != http.StatusOK || second.Body.String() != "cached" {
		t.Fatalf("second response = %d %q, want stale cached", second.Code, second.Body.String())
	}
	if second.Header().Get("X-Substrate-Stale") != "true" {
		t.Fatalf("X-Substrate-Stale = %q, want true", second.Header().Get("X-Substrate-Stale"))
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
}

func TestDirectProxyServesStaleCachedGETWhenUpstreamStalls(t *testing.T) {
	t.Setenv("ATENET_DIRECT_PROXY_STALE_CACHE", "1")

	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.1"},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: 1,
					},
				},
			}, nil
		},
	}
	var upstreamCalls atomic.Int32
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if upstreamCalls.Add(1) == 1 {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("cached")),
					Request:    req,
				}, nil
			}
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
	}

	req := httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	first := httptest.NewRecorder()
	srv.handleDirectProxy(first, req)
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d, want 200", first.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	second := httptest.NewRecorder()
	start := time.Now()
	srv.handleDirectProxy(second, req)
	elapsed := time.Since(start)

	if second.Code != http.StatusOK || second.Body.String() != "cached" {
		t.Fatalf("second response = %d %q, want stale cached", second.Code, second.Body.String())
	}
	if second.Header().Get("X-Substrate-Stale") != "true" {
		t.Fatalf("X-Substrate-Stale = %q, want true", second.Header().Get("X-Substrate-Stale"))
	}
	if elapsed > time.Second {
		t.Fatalf("stale response took %s, want under 1s", elapsed)
	}
}

func TestDirectProxyServesStaleCachedGETWhenUpstreamBodyStalls(t *testing.T) {
	t.Setenv("ATENET_DIRECT_PROXY_STALE_CACHE", "1")

	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.1"},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: 1,
					},
				},
			}, nil
		},
	}
	var upstreamCalls atomic.Int32
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if upstreamCalls.Add(1) == 1 {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("cached")),
					Request:    req,
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       blockingReadCloser{ctx: req.Context()},
				Request:    req,
			}, nil
		}),
	}

	req := httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	first := httptest.NewRecorder()
	srv.handleDirectProxy(first, req)
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d, want 200", first.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	second := httptest.NewRecorder()
	srv.handleDirectProxy(second, req)

	if second.Code != http.StatusOK || second.Body.String() != "cached" {
		t.Fatalf("second response = %d %q, want stale cached", second.Code, second.Body.String())
	}
	if second.Header().Get("X-Substrate-Stale") != "true" {
		t.Fatalf("X-Substrate-Stale = %q, want true", second.Header().Get("X-Substrate-Stale"))
	}
}

func TestDirectProxyDoesNotServeStaleCachedGETByDefault(t *testing.T) {
	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.1"},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: 1,
					},
				},
			}, nil
		},
	}
	var upstreamCalls atomic.Int32
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if upstreamCalls.Add(1) == 1 {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("cached")),
					Request:    req,
				}, nil
			}
			return nil, fmt.Errorf("source paused")
		}),
	}

	req := httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	first := httptest.NewRecorder()
	srv.handleDirectProxy(first, req)
	if first.Code != http.StatusOK || first.Body.String() != "cached" {
		t.Fatalf("first response = %d %q, want 200 cached", first.Code, first.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	second := httptest.NewRecorder()
	srv.handleDirectProxy(second, req)

	if second.Code != http.StatusBadGateway {
		t.Fatalf("second status = %d body=%q, want 502 without stale cache", second.Code, second.Body.String())
	}
	if second.Header().Get("X-Substrate-Stale") != "" {
		t.Fatalf("X-Substrate-Stale = %q, want empty", second.Header().Get("X-Substrate-Stale"))
	}
}

func TestDirectProxyBoundsMigrationAttemptAndRetriesSwitchedTarget(t *testing.T) {
	var routeCalls atomic.Int32
	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			call := routeCalls.Add(1)
			ip := "10.0.0.1"
			phase := ateapipb.ActorRoute_PHASE_DRAINING
			gen := int64(1)
			if call > 2 {
				ip = "10.0.0.2"
				phase = ateapipb.ActorRoute_PHASE_SWITCHED
				gen = 2
			}
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: ip},
						Phase:      phase,
						Generation: gen,
					},
				},
			}, nil
		},
	}
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Host == "10.0.0.1:80" {
				<-req.Context().Done()
				return nil, req.Context().Err()
			}
			if req.URL.Host != "10.0.0.2:80" {
				t.Fatalf("unexpected upstream host %q", req.URL.Host)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("target")),
				Request:    req,
			}, nil
		}),
	}

	req := httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	rec := httptest.NewRecorder()
	start := time.Now()
	srv.handleDirectProxy(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q, want 200", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "target" {
		t.Fatalf("body = %q, want target", rec.Body.String())
	}
	if rec.Header().Get("X-Substrate-Stale") != "" {
		t.Fatalf("X-Substrate-Stale = %q, want empty", rec.Header().Get("X-Substrate-Stale"))
	}
	if elapsed > time.Second {
		t.Fatalf("retry took %s, want under 1s", elapsed)
	}
}

func TestDirectProxyBoundsRetryableActiveAttemptAndRetriesFreshRoute(t *testing.T) {
	t.Setenv("ATENET_DIRECT_PROXY_BOUND_ACTIVE_RETRYABLE_ATTEMPT", "1")
	t.Setenv("ATENET_DIRECT_PROXY_MIGRATION_ATTEMPT_TIMEOUT_MS", "30")
	t.Setenv("ATENET_DIRECT_PROXY_RETRY_DELAY_MS", "1")
	t.Setenv("ATENET_DIRECT_PROXY_RETRY_BUDGET_MS", "500")

	var routeCalls atomic.Int32
	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			call := routeCalls.Add(1)
			ip := "10.0.0.1"
			if call > 1 {
				ip = "10.0.0.2"
			}
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: ip},
						Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
						Generation: int64(call),
					},
				},
			}, nil
		},
	}
	resolver := NewActorRouteResolver(client, NewActorResumer(client))
	srv := &RouterServer{
		extprocSrv: &ExtProcServer{routeResolver: resolver},
		inflight:   newInFlightTracker(),
		directProxyTransport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Host == "10.0.0.1:80" {
				<-req.Context().Done()
				return nil, req.Context().Err()
			}
			if req.URL.Host != "10.0.0.2:80" {
				t.Fatalf("unexpected upstream host %q", req.URL.Host)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("target")),
				Request:    req,
			}, nil
		}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "http://my-actor.team-a.actors.resources.substrate.ate.dev/substrate/migration-state", nil).WithContext(ctx)
	req.Host = "my-actor.team-a.actors.resources.substrate.ate.dev"
	rec := httptest.NewRecorder()
	start := time.Now()
	srv.handleDirectProxy(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q, want 200", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "target" {
		t.Fatalf("body = %q, want target", rec.Body.String())
	}
	if got := routeCalls.Load(); got < 2 {
		t.Fatalf("route calls = %d, want retry with a fresh route", got)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("retry took %s, want under 500ms", elapsed)
	}
}

func TestDirectProxyPortDefaultsToHTTPPort(t *testing.T) {
	srv := &RouterServer{cfg: RouterConfig{HttpPort: 8080}}

	if got := srv.directHTTPProxyPort(); got != 8080 {
		t.Fatalf("directHTTPProxyPort() = %d, want 8080", got)
	}
}

func TestDirectProxyPortAllowsDedicatedListener(t *testing.T) {
	srv := &RouterServer{cfg: RouterConfig{HttpPort: 8080, DirectHTTPProxyPort: 18080}}

	if got := srv.directHTTPProxyPort(); got != 18080 {
		t.Fatalf("directHTTPProxyPort() = %d, want 18080", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type blockingReadCloser struct {
	ctx context.Context
}

func (b blockingReadCloser) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b blockingReadCloser) Close() error {
	return nil
}
