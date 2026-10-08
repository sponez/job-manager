package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"uuid"

	"github.com/danielgtaylor/huma/v2"
	"github.com/sponez/job-manager/internal/application/snapshot"
	"github.com/sponez/job-manager/internal/application/workflow"
)

type SnapshotWorkflowService interface {
	Create(context.Context, string, json.RawMessage, string) (workflow.Workflow, error)
}

type SnapshotHandler struct {
	service SnapshotWorkflowService
	reader  snapshot.Reader
}

func NewSnapshotHandler(service SnapshotWorkflowService, reader snapshot.Reader) *SnapshotHandler {
	return &SnapshotHandler{service: service, reader: reader}
}

type SaveSnapshotInput struct {
	Body struct {
		URL            string `json:"url"`
		IdempotencyKey string `json:"idempotency_key,omitempty"`
	}
}

type SaveSnapshotOutput struct {
	Location string `header:"Location"`
	Body     struct {
		ID string `json:"id"`
	}
}

type GetSnapshotInput struct {
	ID string `path:"id"`
}

type GetSnapshotOutput struct {
	Body struct {
		Saved bool `json:"saved"`
	}
}

func (h *SnapshotHandler) Register(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "save-snapshot", Method: http.MethodPost, Path: "/snapshots",
		Summary: "Save a page snapshot", Tags: []string{"Snapshots"},
		DefaultStatus: http.StatusAccepted, MaxBodyBytes: 4096,
	}, h.save)
	huma.Register(api, huma.Operation{
		OperationID: "get-snapshot-status", Method: http.MethodGet, Path: "/snapshots/{id}",
		Summary: "Check whether a snapshot was saved", Tags: []string{"Snapshots"},
	}, h.get)
}

func (h *SnapshotHandler) save(ctx context.Context, input *SaveSnapshotInput) (*SaveSnapshotOutput, error) {
	payload, err := json.Marshal(workflow.PageInput{URL: input.Body.URL})
	if err != nil {
		return nil, huma.Error400BadRequest("invalid snapshot input")
	}
	wf, err := h.service.Create(ctx, workflow.PageWorkflowType, payload, input.Body.IdempotencyKey)
	if err != nil {
		return nil, snapshotHTTPError(ctx, err)
	}
	output := &SaveSnapshotOutput{Location: "/snapshots/" + wf.ID.String()}
	output.Body.ID = wf.ID.String()
	return output, nil
}

func (h *SnapshotHandler) get(ctx context.Context, input *GetSnapshotInput) (*GetSnapshotOutput, error) {
	id, err := uuid.Parse(input.ID)
	if err != nil {
		return nil, huma.Error400BadRequest("id must be a valid UUID")
	}
	saved, err := h.reader.Exists(ctx, id)
	if err != nil {
		return nil, snapshotHTTPError(ctx, err)
	}
	output := &GetSnapshotOutput{}
	output.Body.Saved = saved
	return output, nil
}

func snapshotHTTPError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, workflow.ErrUnknownType), errors.Is(err, workflow.ErrInvalidPayload):
		return huma.Error422UnprocessableEntity(err.Error())
	case errors.Is(err, context.Canceled):
		return huma.Error408RequestTimeout("request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return huma.Error504GatewayTimeout("request deadline exceeded")
	default:
		slog.ErrorContext(ctx, "snapshot operation failed", "error", err)
		return huma.Error500InternalServerError("internal server error")
	}
}
