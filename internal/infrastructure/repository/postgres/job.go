package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	appjob "github.com/sponez/job-manager/internal/application/job"
	"github.com/sponez/job-manager/internal/domain/job"
)

type JobRepository struct {
	db *pgxpool.Pool
}

var _ appjob.JobRepository = (*JobRepository)(nil)

// NewJobRepository borrows the pool; the application owns its lifetime.
func NewJobRepository(db *pgxpool.Pool) *JobRepository {
	return &JobRepository{db: db}
}

func (r *JobRepository) CreateJob(ctx context.Context, j *job.Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if j == nil {
		return errors.New("job must not be nil")
	}
	_, err := r.db.Exec(ctx, `INSERT INTO jobs (id, kind, status) VALUES ($1, $2, $3)`,
		databaseID(j.ID()), string(j.Kind()), string(j.Status()))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "jobs_pkey" {
			return appjob.ErrJobAlreadyExists
		}
		return fmt.Errorf("insert job: %w", queryError(ctx, err))
	}
	return nil
}

func (r *JobRepository) GetJob(ctx context.Context, id job.ID) (*job.Job, error) {
	j, err := scanJob(r.db.QueryRow(ctx,
		`SELECT id, kind, status FROM jobs WHERE id = $1`, databaseID(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, appjob.ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select job: %w", queryError(ctx, err))
	}
	return j, nil
}

func (r *JobRepository) UpdateStatusByID(ctx context.Context, id job.ID, status job.Status) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !status.Valid() {
		return job.ErrStatusIsNotValid
	}
	tag, err := r.db.Exec(ctx, `UPDATE jobs SET status = $1 WHERE id = $2`,
		string(status), databaseID(id))
	if err != nil {
		return fmt.Errorf("update job status: %w", queryError(ctx, err))
	}
	if tag.RowsAffected() == 0 {
		return appjob.ErrJobNotFound
	}
	return nil
}

func (r *JobRepository) ListJobs(ctx context.Context) ([]*job.Job, error) {
	rows, err := r.db.Query(ctx, `SELECT id, kind, status FROM jobs ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", queryError(ctx, err))
	}
	defer rows.Close()

	jobs := make([]*job.Job, 0)
	var skipped []error
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			// Only domain validation failures are recoverable per-record errors.
			if errors.Is(err, job.ErrKindIsNotValid) || errors.Is(err, job.ErrStatusIsNotValid) {
				skipped = append(skipped, err)
				continue
			}
			return nil, fmt.Errorf("scan jobs: %w", queryError(ctx, err))
		}
		jobs = append(jobs, j)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read jobs: %w", queryError(ctx, err))
	}
	return jobs, errors.Join(skipped...)
}

func scanJob(row pgx.Row) (*job.Job, error) {
	var id pgtype.UUID
	var kind, status string
	if err := row.Scan(&id, &kind, &status); err != nil {
		return nil, err
	}
	j, err := job.New(job.ID(id.Bytes), job.Kind(kind), job.Status(status))
	if err != nil {
		return nil, fmt.Errorf("decode job %s: %w", job.ID(id.Bytes), err)
	}
	return j, nil
}

func databaseID(id job.ID) pgtype.UUID {
	return pgtype.UUID{Bytes: [16]byte(id), Valid: true}
}

// Preserve context errors for HTTP error mapping when the driver reports a
// cancellation as a network error.
func queryError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
