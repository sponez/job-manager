package job

import (
	"context"
	"fmt"
	"uuid"

	"github.com/sponez/job-manager/internal/domain/job"
)

type JobService struct {
	jobRepository JobRepository
}

func New(jobRepository JobRepository) *JobService {
	return &JobService{jobRepository: jobRepository}
}

func (js *JobService) CreateJob(ctx context.Context, kind string) (*job.Job, error) {
	j, err := job.New(uuid.New(), job.Kind(kind), job.StatusPending)
	if err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}

	if err := js.jobRepository.CreateJob(ctx, j); err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}

	return j, nil
}

func (js *JobService) GetJob(ctx context.Context, id job.ID) (*job.Job, error) {
	j, err := js.jobRepository.GetJob(ctx, id)

	if err != nil {
		return nil, fmt.Errorf("get job %s: %w", id, err)
	}

	return j, nil
}

func (js *JobService) CompleteJob(ctx context.Context, id job.ID) error {
	err := js.jobRepository.UpdateStatusByID(ctx, id, job.StatusDone)

	if err != nil {
		return fmt.Errorf("complete job %s: %w", id, err)
	}

	return nil
}

// ListJobs preserves partial results when some records cannot be read.
func (js *JobService) ListJobs(ctx context.Context) ([]*job.Job, error) {
	jobs, err := js.jobRepository.ListJobs(ctx)

	if err != nil {
		return jobs, fmt.Errorf("list jobs: %w", err)
	}

	return jobs, nil
}
