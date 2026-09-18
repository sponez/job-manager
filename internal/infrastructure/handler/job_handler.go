package handler

import (
	"context"
	"fmt"
	"net/http"
	"uuid"

	"github.com/danielgtaylor/huma/v2"
	"github.com/sponez/job-manager/internal/application/job"
	"github.com/sponez/job-manager/internal/infrastructure/handler/dtos"
)

type JobHandler struct {
	jobService *job.JobService
}

func NewJobHandler(jobService *job.JobService) *JobHandler {
	return &JobHandler{jobService}
}

func (jh *JobHandler) Register(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "create-job",
		Method:      http.MethodPost,
		Path:        "/jobs",
		Summary:     "Create job",
		Tags:        []string{"Jobs"},
	}, jh.createJob)

	huma.Register(api, huma.Operation{
		OperationID: "get-job",
		Method:      http.MethodGet,
		Path:        "/job/{id}",
		Summary:     "Get job",
		Tags:        []string{"Jobs"},
	}, jh.getJob)

	huma.Register(api, huma.Operation{
		OperationID: "complete-job",
		Method:      http.MethodPost,
		Path:        "/job/{id}/complete",
		Summary:     "Complete job",
		Tags:        []string{"Jobs"},
	}, jh.completeJob)

	huma.Register(api, huma.Operation{
		OperationID: "get-all-job",
		Method:      http.MethodGet,
		Path:        "/jobs",
		Summary:     "Get all jobs",
		Tags:        []string{"Jobs"},
	}, jh.getJobs)
}

func (jh *JobHandler) createJob(context context.Context, input *dtos.CreateJobInput) (*dtos.CreateJobOutput, error) {
	j, err := jh.jobService.CreateJob(input.Body.Name)
	if err != nil {
		return nil, err
	}

	return &dtos.CreateJobOutput{
		Body: dtos.JobResponse{
			ID:     j.Id().String(),
			Name:   string(j.Name()),
			Status: string(j.Status()),
		},
	}, nil
}

func (jh *JobHandler) getJob(context context.Context, input *dtos.GetJobInput) (*dtos.GetJobOutput, error) {
	id, err := uuid.Parse(input.ID)
	if err != nil {
		return nil, fmt.Errorf("id %v is not uuid", input.ID)
	}

	j, err := jh.jobService.GetJob(id)
	if err != nil {
		return nil, err
	}

	return &dtos.GetJobOutput{
		Body: dtos.JobResponse{
			ID:     j.Id().String(),
			Name:   string(j.Name()),
			Status: string(j.Status()),
		},
	}, nil
}

func (jh *JobHandler) completeJob(context context.Context, input *dtos.CompleteJobInput) (*dtos.CompleteJobOutput, error) {
	id, err := uuid.Parse(input.ID)
	if err != nil {
		return nil, fmt.Errorf("id %v is not uuid", input.ID)
	}

	if err := jh.jobService.CompleteJob(id); err != nil {
		return nil, err
	}

	return &dtos.CompleteJobOutput{}, nil
}

func (jh *JobHandler) getJobs(context context.Context, input *dtos.GetJobsInput) (*dtos.GetJobsOutput, error) {
	jobs, err := jh.jobService.ListJobs()
	if err != nil {
		return nil, err
	}

	list := make([]dtos.JobResponse, 0, len(jobs))
	for _, j := range jobs {
		jr := dtos.JobResponse{
			ID:     j.Id().String(),
			Name:   string(j.Name()),
			Status: string(j.Status()),
		}
		list = append(list, jr)
	}

	return &dtos.GetJobsOutput{
		Body: list,
	}, nil
}
