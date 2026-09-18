package memory

import (
	"uuid"

	"github.com/sponez/job-manager/internal/domain/job"
)

type JobDto struct {
	ID     uuid.UUID
	Name   string
	Status string
}

func JobDtofromDomain(j *job.Job) *JobDto {
	return &JobDto{
		ID:     j.Id(),
		Name:   string(j.Name()),
		Status: string(j.Status()),
	}
}

func (j *JobDto) toDomain() (*job.Job, error) {
	return job.TryNewJob(j.ID, j.Name, j.Status)
}
