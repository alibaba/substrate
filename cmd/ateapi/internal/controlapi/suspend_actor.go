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

package controlapi

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *Service) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	if err := validateSuspendActorRequest(req); err != nil {
		return nil, err
	}

	if asyncSuspendDefaultEnabled() {
		actor, err := s.actorWorkflow.BeginSuspendActor(ctx, req.GetActor().GetAtespace(), req.GetActor().GetName())
		if err != nil {
			if errors.Is(err, store.ErrPersistenceRetry) {
				return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
			}
			if errors.Is(err, store.ErrNotFound) {
				return nil, status.Errorf(codes.NotFound, "Actor %s not found", req.GetActor().GetName())
			}
			return nil, err
		}
		if actor.GetStatus() == ateapipb.Actor_STATUS_SUSPENDING {
			atespace := req.GetActor().GetAtespace()
			name := req.GetActor().GetName()
			go s.finishSuspendAsync(atespace, name)
		}
		return &ateapipb.SuspendActorResponse{Actor: actor}, nil
	}

	actor, err := s.actorWorkflow.SuspendActor(ctx, req.GetActor().GetAtespace(), req.GetActor().GetName())
	if err != nil {
		if errors.Is(err, store.ErrPersistenceRetry) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", req.GetActor().GetName())
		}
		return nil, err
	}

	return &ateapipb.SuspendActorResponse{Actor: actor}, nil
}

func validateSuspendActorRequest(req *ateapipb.SuspendActorRequest) error {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.Actor, fldPath.Child("actor"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	if len(errs) > 0 {
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	return nil
}

func asyncSuspendDefaultEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("ATE_SUSPEND_ASYNC_DEFAULT"))
	return raw == "1" || strings.EqualFold(raw, "true")
}

func (s *Service) finishSuspendAsync(atespace, name string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), asyncSuspendTimeout())
	defer cancel()
	actor, err := s.actorWorkflow.SuspendActor(ctx, atespace, name)
	if err != nil {
		slog.ErrorContext(ctx, "Async suspend checkpoint failed",
			slog.String("atespace", atespace),
			slog.String("actor", name),
			slog.Duration("duration", time.Since(start)),
			slog.Any("err", err))
		return
	}
	slog.InfoContext(ctx, "Async suspend checkpoint completed",
		slog.String("atespace", atespace),
		slog.String("actor", name),
		slog.String("status", actor.GetStatus().String()),
		slog.Duration("duration", time.Since(start)))
}

func asyncSuspendTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv("ATE_SUSPEND_ASYNC_TIMEOUT"))
	if raw == "" {
		return 10 * time.Minute
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 10 * time.Minute
	}
	return d
}
