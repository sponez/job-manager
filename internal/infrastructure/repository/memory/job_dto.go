package memory

import (
	"uuid"

	"github.com/sponez/job-manager/internal/domain/job"
)

type JobDTO struct {
	ID     uuid.UUID
	Name   string
	Status string
}

func JobDTOFromDomain(j *job.Job) *JobDTO {
	return &JobDTO{
		ID:     j.ID(),
		Name:   string(j.Name()),
		Status: string(j.Status()),
	}
}

func (j *JobDTO) toDomain() (*job.Job, error) {
	return job.New(j.ID, job.Name(j.Name), job.Status(j.Status))
}
