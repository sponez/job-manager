package postgres

import (
	"context"
	"errors"
	"fmt"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	app "github.com/sponez/job-manager/internal/application/workflow"
)

type WorkflowRepository struct{ db *pgxpool.Pool }

var _ app.Repository = (*WorkflowRepository)(nil)

func NewWorkflowRepository(db *pgxpool.Pool) *WorkflowRepository { return &WorkflowRepository{db: db} }

func workflowDBID(id uuid.UUID) pgtype.UUID { return pgtype.UUID{Bytes: [16]byte(id), Valid: true} }
func workflowID(id pgtype.UUID) uuid.UUID   { return uuid.UUID(id.Bytes) }

func workflowDBIDs(ids []uuid.UUID) []pgtype.UUID {
	values := make([]pgtype.UUID, len(ids))
	for i, id := range ids {
		values[i] = workflowDBID(id)
	}
	return values
}

func (r *WorkflowRepository) Create(ctx context.Context, value app.Workflow) (app.Workflow, error) {
	var id pgtype.UUID
	err := r.db.QueryRow(ctx, `INSERT INTO workflows
		(id, type, definition_version, status, idempotency_key, payload)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6)
		ON CONFLICT (type, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING id`, workflowDBID(value.ID), value.Type, value.DefinitionVersion,
		value.Status, value.IdempotencyKey, value.Payload).Scan(&id)
	if err == nil {
		return value, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) || value.IdempotencyKey == "" {
		return app.Workflow{}, fmt.Errorf("create workflow: %w", err)
	}
	err = r.db.QueryRow(ctx, `SELECT id FROM workflows WHERE type = $1 AND idempotency_key = $2`,
		value.Type, value.IdempotencyKey).Scan(&id)
	if err != nil {
		return app.Workflow{}, fmt.Errorf("find idempotent workflow: %w", err)
	}
	return r.Get(ctx, workflowID(id))
}

func (r *WorkflowRepository) Get(ctx context.Context, id uuid.UUID) (app.Workflow, error) {
	value, err := scanWorkflow(r.db.QueryRow(ctx, `SELECT id, type, definition_version, status,
		payload, COALESCE(idempotency_key, '') FROM workflows WHERE id = $1`, workflowDBID(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Workflow{}, app.ErrNotFound
	}
	if err != nil {
		return app.Workflow{}, fmt.Errorf("get workflow: %w", err)
	}
	return value, nil
}

func scanWorkflow(row pgx.Row) (app.Workflow, error) {
	var value app.Workflow
	var id pgtype.UUID
	err := row.Scan(&id, &value.Type, &value.DefinitionVersion, &value.Status,
		&value.Payload, &value.IdempotencyKey)
	value.ID = workflowID(id)
	return value, err
}
