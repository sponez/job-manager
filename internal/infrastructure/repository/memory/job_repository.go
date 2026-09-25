package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"uuid"

	appjob "github.com/sponez/job-manager/internal/application/job"
	"github.com/sponez/job-manager/internal/domain/job"
)

type MemoryJobRepository struct {
	mu   sync.RWMutex
	jobs map[uuid.UUID]*JobDTO
}

var _ appjob.JobRepository = (*MemoryJobRepository)(nil)

func New() *MemoryJobRepository {
	return &MemoryJobRepository{
		jobs: make(map[uuid.UUID]*JobDTO),
	}
}

func (r *MemoryJobRepository) CreateJob(ctx context.Context, j *job.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if j == nil {
		return errors.New("job must not be nil")
	}
	dto := JobDTOFromDomain(j)
	if _, exists := r.jobs[dto.ID]; exists {
		return appjob.ErrJobAlreadyExists
	}
	if r.jobs == nil {
		r.jobs = make(map[uuid.UUID]*JobDTO)
	}
	r.jobs[dto.ID] = dto

	return nil
}

func (r *MemoryJobRepository) GetJob(ctx context.Context, id job.ID) (*job.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if dto, ok := r.jobs[id]; ok {
		return dto.toDomain()
	}

	return nil, appjob.ErrJobNotFound
}

func (r *MemoryJobRepository) UpdateStatusByID(ctx context.Context, id job.ID, s job.Status) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if dto, ok := r.jobs[id]; ok {
		r.jobs[id] = &JobDTO{
			ID:     dto.ID,
			Kind:   dto.Kind,
			Status: string(s),
		}
		return nil
	}

	return appjob.ErrJobNotFound
}

func (r *MemoryJobRepository) ListJobs(ctx context.Context) ([]*job.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dJobs := make([]*job.Job, 0, len(r.jobs))
	var errs []error

	for id, j := range r.jobs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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

func (r *MemoryJobRepository) DeleteJob(ctx context.Context, id job.ID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	delete(r.jobs, id)

	return nil
}
