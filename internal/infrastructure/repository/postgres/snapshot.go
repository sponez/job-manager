package postgres

import (
	"context"
	"errors"
	"fmt"
	"uuid"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sponez/job-manager/internal/application/snapshot"
)

type SnapshotRepository struct{ db *pgxpool.Pool }

var _ snapshot.Store = (*SnapshotRepository)(nil)
var _ snapshot.Reader = (*SnapshotRepository)(nil)

func NewSnapshotRepository(db *pgxpool.Pool) *SnapshotRepository {
	return &SnapshotRepository{db: db}
}

// Save is idempotent: retries preserve the first successfully saved snapshot.
func (r *SnapshotRepository) Save(ctx context.Context, workflowID uuid.UUID, value snapshot.Snapshot) error {
	if workflowID == uuid.Nil() {
		return errors.New("workflow ID must not be nil")
	}
	if value.SourceURL == "" {
		return errors.New("snapshot source URL must not be empty")
	}
	body := value.Body
	if body == nil {
		body = []byte{}
	}
	_, err := r.db.Exec(ctx, `INSERT INTO snapshots (workflow_id, source_url, content_type, body)
		VALUES ($1, $2, $3, $4) ON CONFLICT (workflow_id) DO NOTHING`,
		pgtype.UUID{Bytes: [16]byte(workflowID), Valid: true}, value.SourceURL, value.ContentType, body)
	if err != nil {
		return fmt.Errorf("save snapshot for workflow %s: %w", workflowID, err)
	}
	return nil
}

// Delete is idempotent and can be called during compensation after an uncertain save.
func (r *SnapshotRepository) Delete(ctx context.Context, workflowID uuid.UUID) error {
	if workflowID == uuid.Nil() {
		return errors.New("workflow ID must not be nil")
	}
	if _, err := r.db.Exec(ctx, `DELETE FROM snapshots WHERE workflow_id = $1`, pgtype.UUID{Bytes: [16]byte(workflowID), Valid: true}); err != nil {
		return fmt.Errorf("delete snapshot for workflow %s: %w", workflowID, err)
	}
	return nil
}

func (r *SnapshotRepository) Exists(ctx context.Context, workflowID uuid.UUID) (bool, error) {
	if workflowID == uuid.Nil() {
		return false, errors.New("workflow ID must not be nil")
	}
	var exists bool
	if err := r.db.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM snapshots WHERE workflow_id = $1
	)`, pgtype.UUID{Bytes: [16]byte(workflowID), Valid: true}).Scan(&exists); err != nil {
		return false, fmt.Errorf("check snapshot for workflow %s: %w", workflowID, err)
	}
	return exists, nil
}
