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
	"errors"
	"testing"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type routeResolverMockClient struct {
	ateapipb.ControlClient
	getRouteFn func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error)
	resumeFn   func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error)
}

func (m *routeResolverMockClient) GetActorRoute(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
	if m.getRouteFn != nil {
		return m.getRouteFn(ctx, in, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "unimplemented")
}

func (m *routeResolverMockClient) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	if m.resumeFn != nil {
		return m.resumeFn(ctx, in, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "unimplemented")
}

func TestTargetForRouteActive(t *testing.T) {
	actor := &ateapipb.Actor{Route: &ateapipb.ActorRoute{
		Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.10"},
		Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
		Generation: 3,
	}}

	got, err := targetForActorRoute(actor)
	if err != nil {
		t.Fatal(err)
	}

	if got.IP != "10.0.0.10" || got.Port != "80" || got.Generation != 3 || got.Phase != ateapipb.ActorRoute_PHASE_ACTIVE {
		t.Fatalf("target = %+v", got)
	}
}

func TestTargetForRoutePreparingStaysOnActive(t *testing.T) {
	actor := &ateapipb.Actor{Route: &ateapipb.ActorRoute{
		Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.10"},
		Candidate:  &ateapipb.RouteTarget{AteomPodIp: "10.0.0.11"},
		Phase:      ateapipb.ActorRoute_PHASE_PREPARING,
		Generation: 4,
	}}

	got, err := targetForActorRoute(actor)
	if err != nil {
		t.Fatal(err)
	}

	if got.IP != "10.0.0.10" || got.Port != "80" || got.Generation != 4 || got.Phase != ateapipb.ActorRoute_PHASE_PREPARING {
		t.Fatalf("target = %+v", got)
	}
}

func TestTargetForRouteSwitched(t *testing.T) {
	actor := &ateapipb.Actor{Route: &ateapipb.ActorRoute{
		Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.11"},
		Phase:      ateapipb.ActorRoute_PHASE_SWITCHED,
		Generation: 5,
	}}

	got, err := targetForActorRoute(actor)
	if err != nil {
		t.Fatal(err)
	}

	if got.IP != "10.0.0.11" || got.Port != "80" || got.Generation != 5 || got.Phase != ateapipb.ActorRoute_PHASE_SWITCHED {
		t.Fatalf("target = %+v", got)
	}
}

func TestTargetForRouteFallbackToActorPodIP(t *testing.T) {
	actor := &ateapipb.Actor{
		AteomPodIp: "10.0.0.12",
		Route: &ateapipb.ActorRoute{
			Phase:      ateapipb.ActorRoute_PHASE_UNSPECIFIED,
			Generation: 0,
		},
	}

	got, err := targetForActorRoute(actor)
	if err != nil {
		t.Fatal(err)
	}

	if got.IP != "10.0.0.12" || got.Port != "80" || got.Generation != 0 || got.Phase != ateapipb.ActorRoute_PHASE_UNSPECIFIED {
		t.Fatalf("target = %+v", got)
	}
}

func TestTargetForRouteInvalidIPReturnsReqError(t *testing.T) {
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: "actor-a"},
		Route: &ateapipb.ActorRoute{
			Active: &ateapipb.RouteTarget{AteomPodIp: "not-an-ip"},
			Phase:  ateapipb.ActorRoute_PHASE_ACTIVE,
		},
	}

	_, err := targetForActorRoute(actor)
	if err == nil {
		t.Fatal("expected error")
	}

	var reqErr *reqError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error = %T, want *reqError", err)
	}
	if reqErr.statusCode != int(envoy_type.StatusCode_InternalServerError) {
		t.Fatalf("statusCode = %d", reqErr.statusCode)
	}
}

func TestActorRouteResolverUsesActiveRouteWithoutResume(t *testing.T) {
	var resumeCalled bool
	client := &routeResolverMockClient{
		getRouteFn: func(ctx context.Context, in *ateapipb.GetActorRouteRequest, opts ...grpc.CallOption) (*ateapipb.GetActorRouteResponse, error) {
			return &ateapipb.GetActorRouteResponse{
				Actor: &ateapipb.Actor{
					Status: ateapipb.Actor_STATUS_RUNNING,
					Route: &ateapipb.ActorRoute{
						Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.20"},
						Phase:      ateapipb.ActorRoute_PHASE_PREPARING,
						Generation: 7,
					},
				},
			}, nil
		},
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			resumeCalled = true
			return nil, status.Error(codes.Internal, "unexpected resume")
		},
	}
	resolver := NewActorRouteResolver(client, NewActorResumer(client))

	_, target, err := resolver.Resolve(context.Background(), "space-a", "actor-a")
	if err != nil {
		t.Fatal(err)
	}
	if resumeCalled {
		t.Fatal("ResumeActor should not be called when active route is available")
	}
	if target.IP != "10.0.0.20" || target.Generation != 7 || target.Phase != ateapipb.ActorRoute_PHASE_PREPARING {
		t.Fatalf("target = %+v", target)
	}
}

func TestActorRouteResolverFallsBackToResumeWhenGetRouteUnimplemented(t *testing.T) {
	client := &routeResolverMockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			return &ateapipb.ResumeActorResponse{
				Actor: &ateapipb.Actor{
					Status:     ateapipb.Actor_STATUS_RUNNING,
					AteomPodIp: "10.0.0.21",
				},
			}, nil
		},
	}
	resolver := NewActorRouteResolver(client, NewActorResumer(client))

	_, target, err := resolver.Resolve(context.Background(), "space-a", "actor-a")
	if err != nil {
		t.Fatal(err)
	}
	if target.IP != "10.0.0.21" {
		t.Fatalf("target = %+v", target)
	}
}
