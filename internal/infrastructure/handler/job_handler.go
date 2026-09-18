package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"uuid"

	"github.com/danielgtaylor/huma/v2"
	"github.com/sponez/job-manager/internal/application/job"
	domainjob "github.com/sponez/job-manager/internal/domain/job"
	"github.com/sponez/job-manager/internal/infrastructure/handler/dtos"
)

type JobHandler struct {
	jobService *job.JobService
}

func NewJobHandler(jobService *job.JobService) *JobHandler {
	return &JobHandler{jobService: jobService}
}

func (jh *JobHandler) Register(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID:   "create-job",
		Method:        http.MethodPost,
		Path:          "/jobs",
		Summary:       "Create job",
		Tags:          []string{"Jobs"},
		DefaultStatus: http.StatusCreated,
		MaxBodyBytes:  4096,
		Errors: []int{
			http.StatusBadRequest, http.StatusUnprocessableEntity, http.StatusConflict,
			http.StatusRequestEntityTooLarge, http.StatusRequestTimeout, http.StatusGatewayTimeout,
		},
	}, jh.createJob)

	huma.Register(api, huma.Operation{
		OperationID: "get-job",
		Method:      http.MethodGet,
		Path:        "/jobs/{id}",
		Summary:     "Get job",
		Tags:        []string{"Jobs"},
		Errors:      []int{http.StatusBadRequest, http.StatusNotFound, http.StatusRequestTimeout, http.StatusGatewayTimeout},
	}, jh.getJob)

	huma.Register(api, huma.Operation{
		OperationID:   "complete-job",
		Method:        http.MethodPost,
		Path:          "/jobs/{id}/complete",
		Summary:       "Complete job",
		Tags:          []string{"Jobs"},
		DefaultStatus: http.StatusNoContent,
		Errors:        []int{http.StatusBadRequest, http.StatusNotFound, http.StatusRequestTimeout, http.StatusGatewayTimeout},
	}, jh.completeJob)

	huma.Register(api, huma.Operation{
		OperationID: "list-jobs",
		Method:      http.MethodGet,
		Path:        "/jobs",
		Summary:     "Get all jobs",
		Tags:        []string{"Jobs"},
		Errors:      []int{http.StatusRequestTimeout, http.StatusGatewayTimeout},
	}, jh.getJobs)
}

func (jh *JobHandler) createJob(ctx context.Context, input *dtos.CreateJobInput) (*dtos.CreateJobOutput, error) {
	j, err := jh.jobService.CreateJob(ctx, input.Body.Name)
	if errors.Is(err, domainjob.ErrNameIsNotValid) {
		return nil, huma.Error422UnprocessableEntity("name must be Send email or Get page")
	}
	if err != nil {
		return nil, jobHTTPError(ctx, err)
	}

	return &dtos.CreateJobOutput{
		Location: "/jobs/" + j.ID().String(),
		Body: dtos.JobResponse{
			ID:     j.ID().String(),
			Name:   string(j.Name()),
			Status: string(j.Status()),
		},
	}, nil
}

func (jh *JobHandler) getJob(ctx context.Context, input *dtos.GetJobInput) (*dtos.GetJobOutput, error) {
	id, err := uuid.Parse(input.ID)
	if err != nil {
		return nil, huma.Error400BadRequest("id must be a valid UUID")
	}

	j, err := jh.jobService.GetJob(ctx, id)
	if err != nil {
		return nil, jobHTTPError(ctx, err)
	}

	return &dtos.GetJobOutput{
		Body: dtos.JobResponse{
			ID:     j.ID().String(),
			Name:   string(j.Name()),
			Status: string(j.Status()),
		},
	}, nil
}

func (jh *JobHandler) completeJob(ctx context.Context, input *dtos.CompleteJobInput) (*dtos.CompleteJobOutput, error) {
	id, err := uuid.Parse(input.ID)
	if err != nil {
		return nil, huma.Error400BadRequest("id must be a valid UUID")
	}

	if err := jh.jobService.CompleteJob(ctx, id); err != nil {
		return nil, jobHTTPError(ctx, err)
	}

	return &dtos.CompleteJobOutput{}, nil
}

func (jh *JobHandler) getJobs(ctx context.Context, _ *dtos.GetJobsInput) (*dtos.GetJobsOutput, error) {
	jobs, err := jh.jobService.ListJobs(ctx)
	warnings := make([]string, 0)
	if err != nil {
		if len(jobs) == 0 || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, jobHTTPError(ctx, err)
		}
		slog.WarnContext(ctx, "some jobs could not be read", "error", err, "returned_jobs", len(jobs))
		warnings = append(warnings, "Some jobs could not be read")
	}

	list := make([]dtos.JobResponse, 0, len(jobs))
	for _, j := range jobs {
		jr := dtos.JobResponse{
			ID:     j.ID().String(),
			Name:   string(j.Name()),
			Status: string(j.Status()),
		}
		list = append(list, jr)
	}

	return &dtos.GetJobsOutput{
		Body: dtos.GetJobsOutputBody{
			Jobs:     list,
			Partial:  err != nil,
			Warnings: warnings,
		},
	}, nil
}

func jobHTTPError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, job.ErrJobNotFound):
		return huma.Error404NotFound("job not found")
	case errors.Is(err, job.ErrJobAlreadyExists):
		return huma.Error409Conflict("job already exists")
	case errors.Is(err, context.Canceled):
		return huma.Error408RequestTimeout("request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return huma.Error504GatewayTimeout("request deadline exceeded")
	default:
		slog.ErrorContext(ctx, "job operation failed", "error", err)
		return huma.Error500InternalServerError("internal server error")
	}
}
