package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"time"
	"uuid"
)

var errTransitionConflict = errors.New("workflow status changed during step transition")

// completeStep persists the outcome and chooses the next forward or reverse step.
func (s *Service) completeStep(ctx context.Context, step StepRecord, result json.RawMessage, next *NewStep) (uuid.UUID, bool, error) {
	var nextID uuid.UUID
	var accepted bool
	err := s.repo.WithinTx(ctx, func(tx Transaction) error {
		var err error
		accepted, err = tx.UpdateStep(ctx, StepUpdate{
			ID: step.ID, Version: step.Version, ExpectedStatus: StatusRunning,
			Status: StatusCompleted, Result: result,
		})
		if err != nil || !accepted {
			return err
		}
		if step.Kind == KindForward {
			if next != nil {
				nextID = uuid.New()
				return tx.InsertStep(ctx, StepWrite{
					ID: nextID, WorkflowID: step.WorkflowID, Seq: step.Seq + 1,
					Type: next.Type, Kind: KindForward, Status: StatusPending,
					Payload: next.Payload, MaxAttempts: maxAttempts,
				})
			}
			return setWorkflowStatus(ctx, tx, step.WorkflowID, StatusRunning, StatusCompleted)
		}
		previous, found, err := tx.FindPreviousStep(ctx, step.WorkflowID, step.TargetSeq, KindForward, StatusCompleted)
		if err != nil {
			return err
		}
		if !found {
			return setWorkflowStatus(ctx, tx, step.WorkflowID, StatusCompensating, StatusCompensated)
		}
		nextID = uuid.New()
		return tx.InsertStep(ctx, StepWrite{
			ID: nextID, WorkflowID: step.WorkflowID, Seq: step.Seq + 1,
			Type: previous.Type, Kind: KindCompensation, Status: StatusPending,
			CompensatesStepID: previous.ID, MaxAttempts: maxAttempts,
		})
	})
	return nextID, accepted, err
}

// recordFailure decides whether to retry, compensate or stop the workflow.
func (s *Service) recordFailure(ctx context.Context, step StepRecord, cause error, compensate bool) error {
	message := cause.Error()
	if len(message) > 4096 {
		message = message[:4096]
	}
	failure, _ := json.Marshal(map[string]string{"message": message})
	return s.repo.WithinTx(ctx, func(tx Transaction) error {
		if step.Attempts < step.MaxAttempts {
			delay := time.Duration(1<<min(step.Attempts-1, 5)) * time.Second
			_, err := tx.UpdateStep(ctx, StepUpdate{
				ID: step.ID, Version: step.Version, ExpectedStatus: StatusRunning,
				Status:    StatusPending,
				LastError: failure, AvailableAfter: delay,
			})
			return err
		}
		accepted, err := tx.UpdateStep(ctx, StepUpdate{
			ID: step.ID, Version: step.Version, ExpectedStatus: StatusRunning,
			Status: StatusFailed, LastError: failure,
		})
		if err != nil || !accepted {
			return err
		}
		switch {
		case step.Kind == KindCompensation:
			return setWorkflowStatus(ctx, tx, step.WorkflowID, StatusCompensating, StatusCompensationFailed)
		case compensate:
			if err := setWorkflowStatus(ctx, tx, step.WorkflowID, StatusRunning, StatusCompensating); err != nil {
				return err
			}
			return tx.InsertStep(ctx, StepWrite{
				ID: uuid.New(), WorkflowID: step.WorkflowID, Seq: step.Seq + 1,
				Type: step.Type, Kind: KindCompensation, Status: StatusPending,
				CompensatesStepID: step.ID, MaxAttempts: maxAttempts,
			})
		default:
			return setWorkflowStatus(ctx, tx, step.WorkflowID, StatusRunning, StatusFailed)
		}
	})
}

func setWorkflowStatus(ctx context.Context, tx Transaction, id uuid.UUID, from, to string) error {
	changed, err := tx.SetWorkflowStatus(ctx, id, from, to)
	if err != nil {
		return err
	}
	if !changed {
		return errTransitionConflict
	}
	return nil
}
