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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
