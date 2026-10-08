package workflow

import (
	"context"
	"errors"
	"time"
	"uuid"

	"github.com/sponez/job-manager/internal/application/worker"
)

type Queue interface {
	Push(context.Context, worker.Task) error
}

// Tick starts pending workflows and offers reserved steps to the worker pool.
func (s *Service) Tick(ctx context.Context, queue Queue) error {
	if err := s.startPendingWorkflows(ctx); err != nil {
		return err
	}
	if len(s.definitions) == 0 {
		return nil
	}
	limit := 32
	if capacity, ok := queue.(interface{ AvailableSlots() int }); ok {
		limit = min(limit, capacity.AvailableSlots())
		if limit <= 0 {
			return nil
		}
	}
	var exclude []uuid.UUID
	s.inFlight.Range(func(key, _ any) bool {
		exclude = append(exclude, key.(uuid.UUID))
		return true
	})
	reservedAt := time.Now()
	reserved, err := s.repo.ReserveSteps(ctx, s.stepReserve(nil, exclude, limit))
	if err != nil {
		return err
	}
	var unqueued []StepReservation
	var pushErr error
	for i, reservation := range reserved {
		id := reservation.Step.ID
		if _, loaded := s.inFlight.LoadOrStore(id, struct{}{}); loaded {
			unqueued = append(unqueued, reservation)
			continue
		}
		err := queue.Push(ctx, func(taskCtx context.Context) {
			defer s.inFlight.Delete(id)
			s.processReserved(taskCtx, reservation.Step, reservedAt)
		})
		if err != nil {
			s.inFlight.Delete(id)
			unqueued = append(unqueued, reserved[i:]...)
			if !errors.Is(err, worker.ErrQueueIsFull) {
				pushErr = err
			}
			break
		}
	}
	if len(unqueued) > 0 {
		restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err := s.repo.RestoreSteps(restoreCtx, unqueued, StatusRunning)
		cancel()
		if err != nil {
			return err
		}
	}
	return pushErr
}

func (s *Service) processReserved(ctx context.Context, step StepRecord, reservedAt time.Time) {
	if time.Since(reservedAt) >= stepLease/4 {
		valid, err := s.repo.RefreshStepLease(ctx, step.ID, step.Version, StatusRunning, stepLease)
		if err != nil {
			s.logger.ErrorContext(ctx, "refresh workflow step lease failed", "step_id", step.ID, "error", err)
			return
		}
		if !valid {
			return
		}
	}
	s.processClaimed(ctx, step)
}

func (s *Service) startPendingWorkflows(ctx context.Context) error {
	return s.repo.WithinTx(ctx, func(tx Transaction) error {
		pending, err := tx.LockWorkflowsByStatus(ctx, StatusPending, 32)
		if err != nil {
			return err
		}
		ids := make([]uuid.UUID, 0, len(pending))
		steps := make([]StepWrite, 0, len(pending))
		for _, wf := range pending {
			definition, ok := s.definitions[definitionKey(wf.Type, wf.DefinitionVersion)]
			if !ok {
				s.logger.ErrorContext(ctx, "unknown workflow definition", "workflow_id", wf.ID)
				continue
			}
			first, err := definition.First(wf.Payload)
			if err != nil {
				s.logger.ErrorContext(ctx, "invalid pending workflow payload", "workflow_id", wf.ID, "error", err)
				continue
			}
			ids = append(ids, wf.ID)
			steps = append(steps, StepWrite{
				ID: uuid.New(), WorkflowID: wf.ID, Seq: 1, Type: first.Type,
				Kind: KindForward, Status: StatusPending, Payload: first.Payload,
				MaxAttempts: maxAttempts,
			})
		}
		if len(ids) == 0 {
			return nil
		}
		changed, err := tx.SetWorkflowStatuses(ctx, ids, StatusPending, StatusRunning)
		if err != nil {
			return err
		}
		if changed != int64(len(ids)) {
			return errTransitionConflict
		}
		return tx.InsertSteps(ctx, steps)
	})
}

func (s *Service) Run(ctx context.Context, queue Queue) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := s.Tick(ctx, queue); err != nil && ctx.Err() == nil {
			s.logger.ErrorContext(ctx, "workflow scheduler failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
