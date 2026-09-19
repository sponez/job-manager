package job

import (
	"context"
	"errors"

	"github.com/sponez/job-manager/internal/domain/job"
)

var (
	ErrJobNotFound      = errors.New("job not found")
	ErrJobAlreadyExists = errors.New("job already exists")
)

type JobRepository interface {
	CreateJob(ctx context.Context, j *job.Job) error
	GetJob(ctx context.Context, id job.ID) (*job.Job, error)
	UpdateStatusByID(ctx context.Context, id job.ID, s job.Status) error
	// ListJobs may return readable jobs together with errors for skipped records.
	// Callers must inspect both results. Cancellation returns no jobs.
	ListJobs(ctx context.Context) ([]*job.Job, error)
}
