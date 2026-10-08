package postgres

import (
	"context"
	"errors"
	"fmt"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	app "github.com/sponez/job-manager/internal/application/workflow"
)

type workflowTx struct{ tx pgx.Tx }

var _ app.Transaction = workflowTx{}

func (r *WorkflowRepository) WithinTx(ctx context.Context, fn func(app.Transaction) error) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin workflow transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := fn(workflowTx{tx: tx}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit workflow transaction: %w", err)
	}
	return nil
}

func (t workflowTx) LockWorkflowsByStatus(ctx context.Context, status string, limit int) ([]app.Workflow, error) {
	rows, err := t.tx.Query(ctx, `SELECT id, type, definition_version, status,
		payload, COALESCE(idempotency_key, '') FROM workflows
		WHERE status = $1 ORDER BY created_at, id LIMIT $2 FOR UPDATE SKIP LOCKED`, status, limit)
	if err != nil {
		return nil, fmt.Errorf("lock workflows: %w", err)
	}
	defer rows.Close()
	var workflows []app.Workflow
	for rows.Next() {
		workflow, err := scanWorkflow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan locked workflow: %w", err)
		}
		workflows = append(workflows, workflow)
	}
	return workflows, rows.Err()
}

func (t workflowTx) SetWorkflowStatuses(ctx context.Context, ids []uuid.UUID, from, to string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	tag, err := t.tx.Exec(ctx, `UPDATE workflows SET status = $3, updated_at = now()
		WHERE id = ANY($1::uuid[]) AND status = $2`, workflowDBIDs(ids), from, to)
	if err != nil {
		return 0, fmt.Errorf("update workflow statuses: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (t workflowTx) InsertSteps(ctx context.Context, steps []app.StepWrite) error {
	if len(steps) == 0 {
		return nil
	}
	_, err := t.tx.CopyFrom(ctx, pgx.Identifier{"workflow_steps"},
		[]string{"id", "workflow_id", "seq", "type", "kind", "compensates_step_id", "status", "payload", "max_attempts"},
		pgx.CopyFromSlice(len(steps), func(i int) ([]any, error) {
			step := steps[i]
			var target any
			if step.CompensatesStepID != uuid.Nil() {
				target = workflowDBID(step.CompensatesStepID)
			}
			return []any{workflowDBID(step.ID), workflowDBID(step.WorkflowID), step.Seq,
				step.Type, step.Kind, target, step.Status, step.Payload, step.MaxAttempts}, nil
		}))
	if err != nil {
		return fmt.Errorf("insert workflow steps: %w", err)
	}
	return nil
}

func (t workflowTx) SetWorkflowStatus(ctx context.Context, id uuid.UUID, from, to string) (bool, error) {
	tag, err := t.tx.Exec(ctx, `UPDATE workflows SET status = $3, updated_at = now()
		WHERE id = $1 AND status = $2`, workflowDBID(id), from, to)
	if err != nil {
		return false, fmt.Errorf("update workflow status: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (t workflowTx) InsertStep(ctx context.Context, step app.StepWrite) error {
	var target pgtype.UUID
	if step.CompensatesStepID != uuid.Nil() {
		target = workflowDBID(step.CompensatesStepID)
	}
	_, err := t.tx.Exec(ctx, `INSERT INTO workflow_steps
		(id, workflow_id, seq, type, kind, compensates_step_id, status, payload, max_attempts)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		workflowDBID(step.ID), workflowDBID(step.WorkflowID), step.Seq, step.Type,
		step.Kind, target, step.Status, step.Payload, step.MaxAttempts)
	if err != nil {
		return fmt.Errorf("insert workflow step: %w", err)
	}
	return nil
}

func (t workflowTx) UpdateStep(ctx context.Context, step app.StepUpdate) (bool, error) {
	tag, err := t.tx.Exec(ctx, `UPDATE workflow_steps SET status = $3, result = $4,
		last_error = $5, available_at = now() + ($6::bigint * interval '1 millisecond'),
		updated_at = now()
		WHERE id = $1 AND version = $2 AND status = $7`,
		workflowDBID(step.ID), step.Version, step.Status, step.Result,
		step.LastError, step.AvailableAfter.Milliseconds(), step.ExpectedStatus)
	if err != nil {
		return false, fmt.Errorf("update workflow step: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (t workflowTx) FindPreviousStep(ctx context.Context, ownerID uuid.UUID, beforeSeq int64, kind, status string) (app.StepSummary, bool, error) {
	var previous app.StepSummary
	var id pgtype.UUID
	err := t.tx.QueryRow(ctx, `SELECT id, type, seq FROM workflow_steps
		WHERE workflow_id = $1 AND seq < $2 AND kind = $3 AND status = $4
		ORDER BY seq DESC LIMIT 1`, workflowDBID(ownerID), beforeSeq, kind, status).Scan(
		&id, &previous.Type, &previous.Seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.StepSummary{}, false, nil
	}
	if err != nil {
		return app.StepSummary{}, false, fmt.Errorf("find previous workflow step: %w", err)
	}
	previous.ID = workflowID(id)
	return previous, true, nil
}
