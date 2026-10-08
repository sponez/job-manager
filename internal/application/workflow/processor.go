package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"time"
	"uuid"
)

// Process keeps a successful linear workflow in one worker until it finishes.
func (s *Service) Process(ctx context.Context, stepID uuid.UUID) {
	step, claimed, err := s.claimStep(ctx, stepID)
	if err != nil {
		s.logger.ErrorContext(ctx, "claim workflow step failed", "step_id", stepID, "error", err)
		return
	}
	if !claimed {
		return
	}
	s.processClaimed(ctx, step)
}

func (s *Service) processClaimed(ctx context.Context, step StepRecord) {
	var err error
	var claimed bool
	for {
		definition, ok := s.definitions[definitionKey(step.WorkflowType, step.DefinitionVersion)]
		if !ok {
			s.fail(ctx, step, ErrUnknownType, false)
			return
		}
		if step.Attempts > step.MaxAttempts {
			s.fail(ctx, step, errAttemptsExhausted, definition.CompensateOnFailure)
			return
		}
		behavior, ok := definition.Steps[step.Type]
		if !ok {
			s.fail(ctx, step, errors.New("unknown step type"), definition.CompensateOnFailure)
			return
		}
		execCtx, cancel := context.WithTimeout(ctx, stepTimeout)
		var result json.RawMessage
		var next *NewStep
		if step.Kind == KindCompensation {
			err = behavior.Compensate(execCtx, step.WorkflowID, step.TargetPayload, step.TargetResult)
		} else {
			result, err = behavior.Execute(execCtx, step.WorkflowID, step.Payload)
			if err == nil {
				next, err = behavior.Next(result)
				if err == nil && next != nil {
					if _, ok := definition.Steps[next.Type]; !ok {
						err = errors.New("step points to unknown next step")
					}
				}
			}
		}
		cancel()
		if err != nil {
			s.fail(ctx, step, err, definition.CompensateOnFailure)
			return
		}
		resultCtx, resultCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		nextID, exists, err := s.completeStep(resultCtx, step, result, next)
		resultCancel()
		if err != nil {
			s.logger.ErrorContext(ctx, "complete workflow step failed", "step_id", step.ID, "error", err)
			return
		}
		if !exists {
			return
		}
		if nextID == uuid.Nil() {
			return
		}
		step, claimed, err = s.claimStep(ctx, nextID)
		if err != nil {
			s.logger.ErrorContext(ctx, "claim workflow step failed", "step_id", nextID, "error", err)
			return
		}
		if !claimed {
			return
		}
	}
}

func (s *Service) fail(ctx context.Context, step StepRecord, cause error, compensate bool) {
	resultCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := s.recordFailure(resultCtx, step, cause, compensate); err != nil {
		s.logger.ErrorContext(ctx, "save workflow step failure failed", "step_id", step.ID, "error", err)
		return
	}
	s.logger.WarnContext(ctx, "workflow step failed", "step_id", step.ID, "attempt", step.Attempts, "error", cause)
}
