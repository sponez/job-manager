package job

import (
	"errors"
	"fmt"
	"uuid"

	"github.com/sponez/job-manager/internal/domain/job"
)

type JobService struct {
	jobRepository JobRepository
}

func New(jobRepository JobRepository) *JobService {
	return &JobService{jobRepository}
}

func (js *JobService) CreateJob(name string) (*job.Job, error) {
	j, err := job.TryNewJob(uuid.New(), name, string(job.StatusPending))
	if err != nil {
		return nil, fmt.Errorf("job name is not valid")
	}

	if err := js.jobRepository.CreateJob(j); err != nil {
		return nil, fmt.Errorf("error while creating a job: %w", err)
	}

	return j, nil
}

func (js *JobService) GetJob(id uuid.UUID) (*job.Job, error) {
	j, err := js.jobRepository.GetJob(id)

	if errors.Is(err, ErrJobNotFound) {
		return nil, fmt.Errorf("job %v is not found", id)
	}

	if err != nil {
		return nil, fmt.Errorf("error while getting a job: %w", err)
	}

	return j, nil
}

func (js *JobService) CompleteJob(id uuid.UUID) error {
	err := js.jobRepository.UpdateStatusById(id, job.StatusDone)

	if errors.Is(err, ErrJobNotFound) {
		return fmt.Errorf("job %v is not found", id)
	}

	if err != nil {
		return fmt.Errorf("error while completing job %v: %w", id, err)
	}

	return nil
}

func (js *JobService) ListJobs() ([]*job.Job, error) {
	jobs, err := js.jobRepository.ListJobs()

	if err != nil {
		return nil, fmt.Errorf("error while getting jobs: %w", err)
	}

	return jobs, nil
}
