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
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
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

func TestDirectProxyServesStaleCachedGETAfterUpstreamFailure(t *testing.T) {
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

func TestDirectProxyServesStaleCachedGETWhenUpstreamStalls(t *testing.T) {
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
