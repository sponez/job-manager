package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"uuid"

	"github.com/sponez/job-manager/internal/application/snapshot"
)

const PageWorkflowType = "fetch_page"

type PageInput struct {
	URL string `json:"url"`
}

func NewPageDefinition(fetcher snapshot.Fetcher, store snapshot.Store) Definition {
	return Definition{
		Type: PageWorkflowType, Version: 1, CompensateOnFailure: true,
		First: func(payload json.RawMessage) (NewStep, error) {
			var input PageInput
			if err := json.Unmarshal(payload, &input); err != nil {
				return NewStep{}, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
			}
			u, err := url.Parse(input.URL)
			if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
				return NewStep{}, fmt.Errorf("%w: URL must be an absolute HTTP or HTTPS URL without credentials", ErrInvalidPayload)
			}
			return NewStep{Type: "fetch_page", Payload: payload}, nil
		},
		Steps: map[string]Step{
			"fetch_page":    fetchPageStep{fetcher: fetcher},
			"save_snapshot": saveSnapshotStep{store: store},
		},
	}
}

type fetchPageStep struct{ fetcher snapshot.Fetcher }

func (s fetchPageStep) Execute(ctx context.Context, _ uuid.UUID, payload json.RawMessage) (json.RawMessage, error) {
	var input PageInput
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, err
	}
	value, err := s.fetcher.Fetch(ctx, input.URL)
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func (fetchPageStep) Compensate(context.Context, uuid.UUID, json.RawMessage, json.RawMessage) error {
	return nil
}

func (fetchPageStep) Next(result json.RawMessage) (*NewStep, error) {
	return &NewStep{Type: "save_snapshot", Payload: result}, nil
}

type saveSnapshotStep struct{ store snapshot.Store }

func (s saveSnapshotStep) Execute(ctx context.Context, workflowID uuid.UUID, payload json.RawMessage) (json.RawMessage, error) {
	var value snapshot.Snapshot
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, err
	}
	return nil, s.store.Save(ctx, workflowID, value)
}

func (s saveSnapshotStep) Compensate(ctx context.Context, workflowID uuid.UUID, _, _ json.RawMessage) error {
	return s.store.Delete(ctx, workflowID)
}

func (saveSnapshotStep) Next(json.RawMessage) (*NewStep, error) { return nil, nil }
