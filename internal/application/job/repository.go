package job

import (
	"errors"
	"uuid"

	"github.com/sponez/job-manager/internal/domain/job"
)

var (
	ErrJobNotFound = errors.New("job not found")
)

type JobRepository interface {
	CreateJob(j *job.Job) error
	GetJob(id uuid.UUID) (*job.Job, error)
	UpdateStatusById(id uuid.UUID, s job.Status) error
	ListJobs() ([]*job.Job, error)
}
