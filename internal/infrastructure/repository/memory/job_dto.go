package memory

import (
	"uuid"

	"github.com/sponez/job-manager/internal/domain/job"
)

type JobDTO struct {
	ID     uuid.UUID
	Kind   string
	Status string
}

func JobDTOFromDomain(j *job.Job) *JobDTO {
	return &JobDTO{
		ID:     j.ID(),
		Kind:   string(j.Kind()),
		Status: string(j.Status()),
	}
}

func (j *JobDTO) toDomain() (*job.Job, error) {
	return job.New(j.ID, job.Kind(j.Kind), job.Status(j.Status))
}
