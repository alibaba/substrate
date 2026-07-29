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
	"net"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type routeTarget struct {
	Namespace  string
	Pod        string
	IP         string
	Port       string
	WorkerPool string
	Node       string
	Generation int64
	Phase      ateapipb.ActorRoute_Phase
}

type ActorRouteResolver struct {
	apiClient ateapipb.ControlClient
	resumer   *ActorResumer
}

func NewActorRouteResolver(apiClient ateapipb.ControlClient, resumer *ActorResumer) *ActorRouteResolver {
	return &ActorRouteResolver{
		apiClient: apiClient,
		resumer:   resumer,
	}
}

func (r *ActorRouteResolver) Resolve(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, routeTarget, error) {
	actor, target, ok, err := r.tryGetRoute(ctx, atespace, actorName)
	if err != nil {
		return nil, routeTarget{}, err
	}
	if ok {
		return actor, target, nil
	}

	actor, err = r.resumer.ResumeActor(ctx, atespace, actorName)
	if err != nil {
		return nil, routeTarget{}, err
	}
	ensureActorRouteMetadata(actor, atespace, actorName)
	target, err = targetForActorRoute(actor)
	if err != nil {
		return actor, routeTarget{}, err
	}
	target = r.enrichTargetFromWorkers(ctx, actor, target)
	return actor, target, nil
}

func (r *ActorRouteResolver) tryGetRoute(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, routeTarget, bool, error) {
	resp, err := r.apiClient.GetActorRoute(ctx, &ateapipb.GetActorRouteRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actorName},
	})
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			return nil, routeTarget{}, false, nil
		}
		return nil, routeTarget{}, false, err
	}
	actor := resp.GetActor()
	ensureActorRouteMetadata(actor, atespace, actorName)
	if actor.Route == nil {
		actor.Route = resp.GetRoute()
	}
	if actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		return nil, routeTarget{}, false, nil
	}
	if actor.GetRoute().GetActive().GetAteomPodIp() == "" && actor.GetAteomPodIp() == "" {
		return nil, routeTarget{}, false, nil
	}
	target, err := targetForActorRoute(actor)
	if err != nil {
		return actor, routeTarget{}, false, err
	}
	target = r.enrichTargetFromWorkers(ctx, actor, target)
	return actor, target, true, nil
}

func (r *ActorRouteResolver) enrichTargetFromWorkers(ctx context.Context, actor *ateapipb.Actor, target routeTarget) routeTarget {
	if target.Namespace == "" || target.Pod == "" || target.Node != "" {
		return target
	}
	resp, err := r.apiClient.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
	if err != nil {
		return target
	}
	for _, worker := range resp.GetWorkers() {
		if worker.GetWorkerNamespace() != target.Namespace || worker.GetWorkerPod() != target.Pod {
			continue
		}
		target.Node = worker.GetNodeName()
		if target.WorkerPool == "" {
			target.WorkerPool = worker.GetWorkerPool()
		}
		return target
	}
	return target
}

func ensureActorRouteMetadata(actor *ateapipb.Actor, atespace, actorName string) {
	if actor.Metadata == nil {
		actor.Metadata = &ateapipb.ResourceMetadata{}
	}
	if actor.Metadata.Atespace == "" {
		actor.Metadata.Atespace = atespace
	}
	if actor.Metadata.Name == "" {
		actor.Metadata.Name = actorName
	}
}

func targetForActorRoute(actor *ateapipb.Actor) (routeTarget, error) {
	route := actor.GetRoute()
	target := route.GetActive()
	if target.GetAteomPodIp() == "" {
		target = &ateapipb.RouteTarget{
			AteomPodNamespace: actor.GetAteomPodNamespace(),
			AteomPodName:      actor.GetAteomPodName(),
			AteomPodIp:        actor.GetAteomPodIp(),
			AteomPodUid:       actor.GetAteomPodUid(),
			WorkerPoolName:    actor.GetWorkerPoolName(),
		}
	}

	ip := target.GetAteomPodIp()
	if net.ParseIP(ip) == nil {
		return routeTarget{}, newReqError(
			envoy_type.StatusCode_InternalServerError,
			"actor %q routing failed",
			actor.GetMetadata().GetName(),
		)
	}

	phase := route.GetPhase()
	if phase == ateapipb.ActorRoute_PHASE_UNSPECIFIED {
		phase = ateapipb.ActorRoute_PHASE_ACTIVE
	}
	generation := route.GetGeneration()
	if generation == 0 {
		generation = 1
	}

	return routeTarget{
		Namespace:  target.GetAteomPodNamespace(),
		Pod:        target.GetAteomPodName(),
		IP:         ip,
		Port:       "80",
		WorkerPool: target.GetWorkerPoolName(),
		Node:       target.GetNodeName(),
		Generation: generation,
		Phase:      phase,
	}, nil
}

func (t routeTarget) HostPort() string {
	return net.JoinHostPort(t.IP, t.Port)
}

func (t routeTarget) String() string {
	return fmt.Sprintf("%s generation=%d phase=%s", t.HostPort(), t.Generation, t.Phase)
}
