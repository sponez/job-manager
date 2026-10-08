package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"uuid"

	"github.com/sponez/job-manager/internal/application/workflow"
	"github.com/sponez/job-manager/internal/infrastructure/handler"
)

type snapshotWorkflowStub struct{ id uuid.UUID }

func (s snapshotWorkflowStub) Create(_ context.Context, kind string, payload json.RawMessage, key string) (workflow.Workflow, error) {
	var input workflow.PageInput
	if err := json.Unmarshal(payload, &input); err != nil || kind != workflow.PageWorkflowType ||
		input.URL != "https://example.com" || key != "request-1" {
		panic("unexpected snapshot request")
	}
	return workflow.Workflow{ID: s.id, Type: kind, Status: workflow.StatusPending}, nil
}

type snapshotReaderStub struct{ saved bool }

func (s *snapshotReaderStub) Exists(_ context.Context, _ uuid.UUID) (bool, error) {
	return s.saved, nil
}

func TestSnapshotHTTPCreateAndCheckSaved(t *testing.T) {
	id := uuid.New()
	reader := &snapshotReaderStub{}
	api := newHandler(handler.NewSnapshotHandler(snapshotWorkflowStub{id: id}, reader))
	created := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/snapshots", strings.NewReader(
		`{"url":"https://example.com","idempotency_key":"request-1"}`))
	request.Header.Set("Content-Type", "application/json")
	api.ServeHTTP(created, request)
	if created.Code != http.StatusAccepted || created.Header().Get("Location") != "/snapshots/"+id.String() {
		t.Fatalf("create: status %d, location %q, body %s", created.Code, created.Header().Get("Location"), created.Body.String())
	}
	var createBody map[string]any
	err := json.Unmarshal(created.Body.Bytes(), &createBody)
	_, hasType := createBody["type"]
	if err != nil || createBody["id"] != id.String() || hasType {
		t.Fatalf("create body = %s, error = %v", created.Body.String(), err)
	}
	check := func(want bool) {
		t.Helper()
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/snapshots/"+id.String(), nil))
		var body struct {
			Saved bool `json:"saved"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || response.Code != http.StatusOK || body.Saved != want {
			t.Fatalf("saved = %v: status %d, body %s, error %v", want, response.Code, response.Body.String(), err)
		}
	}
	check(false)
	reader.saved = true
	check(true)
}
