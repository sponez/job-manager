package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5/pgtype"
	app "github.com/sponez/job-manager/internal/application/workflow"
)

// ReserveSteps locks a batch and records its leases in one transaction.
// Eligibility and the new status are supplied by the application.
func (r *WorkflowRepository) ReserveSteps(ctx context.Context, request app.StepReserve) ([]app.StepReservation, error) {
	kinds := make([]string, 0, len(request.WorkflowStates))
	workflowStatuses := make([]string, 0, len(request.WorkflowStates))
	for _, state := range request.WorkflowStates {
		kinds = append(kinds, state.Kind)
		workflowStatuses = append(workflowStatuses, state.WorkflowStatus)
	}
	definitionTypes := make([]string, 0, len(request.Definitions))
	definitionVersions := make([]int64, 0, len(request.Definitions))
	for _, definition := range request.Definitions {
		definitionTypes = append(definitionTypes, definition.Type)
		definitionVersions = append(definitionVersions, int64(definition.Version))
	}
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin step reservation: %w", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT s.id, s.workflow_id, w.type, w.definition_version,
		s.type, s.kind, s.status, s.payload, s.result, target.payload, target.result,
		s.seq, target.seq, s.attempts, s.max_attempts, s.version
		FROM workflow_steps s JOIN workflows w ON w.id = s.workflow_id
		LEFT JOIN workflow_steps target ON target.id = s.compensates_step_id
		WHERE s.status = ANY($1::text[]) AND s.available_at <= now()
		AND EXISTS (
			SELECT 1 FROM unnest($2::text[], $3::text[]) AS allowed(kind, workflow_status)
			WHERE allowed.kind = s.kind AND allowed.workflow_status = w.status
		)
		AND ($4::boolean OR s.id = ANY($5::uuid[]))
		AND s.id <> ALL($6::uuid[])
		AND ($8::boolean OR EXISTS (
			SELECT 1 FROM unnest($9::text[], $10::bigint[]) AS known(type, version)
			WHERE known.type = w.type AND known.version = w.definition_version
		))
		ORDER BY s.available_at, s.id LIMIT $7 FOR UPDATE OF s, w SKIP LOCKED`,
		request.StepStatuses, kinds, workflowStatuses,
		len(request.OnlyIDs) == 0, workflowDBIDs(request.OnlyIDs),
		workflowDBIDs(request.ExcludeIDs), request.Limit,
		len(request.Definitions) == 0, definitionTypes, definitionVersions)
	if err != nil {
		return nil, fmt.Errorf("lock available steps: %w", err)
	}
	var reserved []app.StepReservation
	for rows.Next() {
		var item app.StepReservation
		var stepID, wfID pgtype.UUID
		var targetSeq pgtype.Int8
		err := rows.Scan(&stepID, &wfID, &item.Step.WorkflowType, &item.Step.DefinitionVersion,
			&item.Step.Type, &item.Step.Kind, &item.PreviousStatus,
			&item.Step.Payload, &item.Step.Result, &item.Step.TargetPayload, &item.Step.TargetResult,
			&item.Step.Seq, &targetSeq, &item.Step.Attempts, &item.Step.MaxAttempts, &item.Step.Version)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan available step: %w", err)
		}
		item.Step.ID, item.Step.WorkflowID = workflowID(stepID), workflowID(wfID)
		if targetSeq.Valid {
			item.Step.TargetSeq = targetSeq.Int64
		}
		reserved = append(reserved, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read available steps: %w", err)
	}
	if len(reserved) == 0 {
		return nil, nil
	}
	ids := make([]uuid.UUID, len(reserved))
	for i, item := range reserved {
		ids[i] = item.Step.ID
	}
	tag, err := tx.Exec(ctx, `UPDATE workflow_steps
		SET status = $2, attempts = attempts + 1, version = version + 1,
			available_at = now() + ($3::bigint * interval '1 millisecond'), updated_at = now()
		WHERE id = ANY($1::uuid[])`, workflowDBIDs(ids), request.NewStatus, request.Lease.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("reserve workflow steps: %w", err)
	}
	if tag.RowsAffected() != int64(len(reserved)) {
		return nil, errors.New("reserved step count changed")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit step reservations: %w", err)
	}
	for i := range reserved {
		reserved[i].Step.Status = request.NewStatus
		reserved[i].Step.Attempts++
		reserved[i].Step.Version++
	}
	return reserved, nil
}

// RestoreSteps makes reservations rejected by the local queue available again.
func (r *WorkflowRepository) RestoreSteps(ctx context.Context, items []app.StepReservation, expectedStatus string) error {
	if len(items) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, len(items))
	versions := make([]int64, len(items))
	statuses := make([]string, len(items))
	for i, item := range items {
		ids[i], versions[i], statuses[i] = item.Step.ID, item.Step.Version, item.PreviousStatus
	}
	tag, err := r.db.Exec(ctx, `UPDATE workflow_steps s
		SET status = previous.status, attempts = s.attempts - 1,
			version = s.version + 1, available_at = now(), updated_at = now()
		FROM unnest($1::uuid[], $2::bigint[], $3::text[]) AS previous(id, version, status)
		WHERE s.id = previous.id AND s.version = previous.version AND s.status = $4`,
		workflowDBIDs(ids), versions, statuses, expectedStatus)
	if err != nil {
		return fmt.Errorf("restore unqueued steps: %w", err)
	}
	if tag.RowsAffected() != int64(len(items)) {
		return errors.New("unqueued step count changed")
	}
	return nil
}

func (r *WorkflowRepository) RefreshStepLease(ctx context.Context, id uuid.UUID, version int64, status string, lease time.Duration) (bool, error) {
	tag, err := r.db.Exec(ctx, `UPDATE workflow_steps
		SET available_at = now() + ($4::bigint * interval '1 millisecond'), updated_at = now()
		WHERE id = $1 AND version = $2 AND status = $3`,
		workflowDBID(id), version, status, lease.Milliseconds())
	if err != nil {
		return false, fmt.Errorf("refresh step lease: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
