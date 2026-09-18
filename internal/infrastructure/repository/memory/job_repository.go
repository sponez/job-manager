package memory

import (
	"errors"
	"fmt"
	"uuid"

	appjob "github.com/sponez/job-manager/internal/application/job"
	"github.com/sponez/job-manager/internal/domain/job"
)

type MemoryJobRepository struct {
	jobs map[uuid.UUID]*JobDto
}

func New() *MemoryJobRepository {
	return &MemoryJobRepository{
		jobs: make(map[uuid.UUID]*JobDto),
	}
}

func (r *MemoryJobRepository) CreateJob(j *job.Job) error {
	dto := JobDtofromDomain(j)
	r.jobs[dto.ID] = dto

	return nil
}

func (r *MemoryJobRepository) GetJob(id uuid.UUID) (*job.Job, error) {
	if dto, ok := r.jobs[id]; ok {
		return dto.toDomain()
	}

	return nil, appjob.ErrJobNotFound
}

func (r *MemoryJobRepository) UpdateStatusById(id uuid.UUID, s job.Status) error {
	if dto, ok := r.jobs[id]; ok {
		dto.Status = string(s)
		return nil
	}

	return appjob.ErrJobNotFound
}

func (r *MemoryJobRepository) ListJobs() ([]*job.Job, error) {
	dJobs := make([]*job.Job, 0, len(r.jobs))
	var errs []error

	for id, j := range r.jobs {
		dj, err := j.toDomain()
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to validate job %v: %w", id, err))
			continue
		}

		dJobs = append(dJobs, dj)
	}

	if len(errs) > 0 {
		return dJobs, errors.Join(errs...)
	}

	return dJobs, nil
}
