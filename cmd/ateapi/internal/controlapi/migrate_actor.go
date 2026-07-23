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
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *Service) PrepareActorMigration(ctx context.Context, req *ateapipb.PrepareActorMigrationRequest) (*ateapipb.PrepareActorMigrationResponse, error) {
	if err := validatePrepareActorMigrationRequest(req); err != nil {
		return nil, err
	}
	ref := req.GetActor()
	actor, err := s.actorWorkflow.PrepareActorMigration(ctx, ref.GetAtespace(), ref.GetName(), req.GetTargetWorkerSelector(), req.GetRequireCrossNode())
	if err != nil {
		return nil, mapMigrationWorkflowError(ref.GetName(), err)
	}
	return &ateapipb.PrepareActorMigrationResponse{Actor: actor}, nil
}

func (s *Service) CommitActorMigration(ctx context.Context, req *ateapipb.CommitActorMigrationRequest) (*ateapipb.CommitActorMigrationResponse, error) {
	if err := validateCommitActorMigrationRequest(req); err != nil {
		return nil, err
	}
	ref := req.GetActor()
	drain := time.Duration(req.GetDrainTimeoutMs()) * time.Millisecond
	if drain <= 0 {
		drain = 3 * time.Second
	}
	actor, err := s.actorWorkflow.CommitActorMigration(ctx, ref.GetAtespace(), ref.GetName(), drain, req.GetRequireQuiesce())
	if err != nil {
		return nil, mapMigrationWorkflowError(ref.GetName(), err)
	}
	return &ateapipb.CommitActorMigrationResponse{Actor: actor}, nil
}

func (s *Service) AbortActorMigration(ctx context.Context, req *ateapipb.AbortActorMigrationRequest) (*ateapipb.AbortActorMigrationResponse, error) {
	if err := validateAbortActorMigrationRequest(req); err != nil {
		return nil, err
	}
	ref := req.GetActor()
	actor, err := s.actorWorkflow.AbortActorMigration(ctx, ref.GetAtespace(), ref.GetName())
	if err != nil {
		return nil, mapMigrationWorkflowError(ref.GetName(), err)
	}
	return &ateapipb.AbortActorMigrationResponse{Actor: actor}, nil
}

func (s *Service) GetActorRoute(ctx context.Context, req *ateapipb.GetActorRouteRequest) (*ateapipb.GetActorRouteResponse, error) {
	if err := validateGetActorRouteRequest(req); err != nil {
		return nil, err
	}
	ref := req.GetActor()
	actor, err := s.persistence.GetActor(ctx, ref.GetAtespace(), ref.GetName())
	if err != nil {
		return nil, mapMigrationWorkflowError(ref.GetName(), err)
	}
	return &ateapipb.GetActorRouteResponse{
		Actor: actor,
		Route: actor.GetRoute(),
	}, nil
}

func validatePrepareActorMigrationRequest(req *ateapipb.PrepareActorMigrationRequest) error {
	return validateMigrationActorRef(req.GetActor())
}

func validateCommitActorMigrationRequest(req *ateapipb.CommitActorMigrationRequest) error {
	var errs field.ErrorList
	if err := validateMigrationActorRef(req.GetActor()); err != nil {
		return err
	}
	if req.GetDrainTimeoutMs() < 0 {
		errs = append(errs, field.Invalid(field.NewPath("drain_timeout_ms"), req.GetDrainTimeoutMs(), "must be non-negative"))
	}
	if len(errs) > 0 {
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	return nil
}

func validateAbortActorMigrationRequest(req *ateapipb.AbortActorMigrationRequest) error {
	return validateMigrationActorRef(req.GetActor())
}

func validateGetActorRouteRequest(req *ateapipb.GetActorRouteRequest) error {
	return validateMigrationActorRef(req.GetActor())
}

func validateMigrationActorRef(ref *ateapipb.ObjectRef) error {
	var fldPath *field.Path
	var errs field.ErrorList
	if ref == nil {
		errs = append(errs, field.Required(fldPath.Child("actor"), ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(ref, fldPath.Child("actor"))...)
	}
	if len(errs) > 0 {
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	return nil
}

func mapMigrationWorkflowError(actorName string, err error) error {
	if errors.Is(err, store.ErrPersistenceRetry) {
		return status.Error(codes.Aborted, "concurrent update conflict, please retry")
	}
	if errors.Is(err, store.ErrNotFound) {
		return status.Errorf(codes.NotFound, "Actor %s not found", actorName)
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return fmt.Errorf("while migrating actor %s: %w", actorName, err)
}
