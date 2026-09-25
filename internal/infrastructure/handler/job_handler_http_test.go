package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"uuid"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	appjob "github.com/sponez/job-manager/internal/application/job"
	domainjob "github.com/sponez/job-manager/internal/domain/job"
	"github.com/sponez/job-manager/internal/infrastructure/handler/dtos"
)

// Use the same router adapter as main. ServeHTTP processes requests in memory;
// no listening socket or running application is needed.
func newTestRouter(service JobService) http.Handler {
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Job handler tests", "1.0.0"))
	NewJobHandler(service, &taskQueueStub{}).Register(api)
	return mux
}

func TestJobHandlerCreateJobHTTP(t *testing.T) {
	j := newTestJob(t, domainjob.KindSendEmail, domainjob.StatusPending)
	calls := 0
	service := &jobServiceStub{t: t, createJob: func(_ context.Context, kind string) (*domainjob.Job, error) {
		calls++
		if kind != "Send email" {
			t.Errorf("kind = %q, want Send email", kind)
		}
		return j, nil
	}}
	router := newTestRouter(service)
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"kind":"Send email"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if calls != 1 {
		t.Errorf("CreateJob calls = %d, want 1", calls)
	}
	if resp.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body = %s", resp.Code, resp.Body.String())
	}
	if got := resp.Header().Get("Location"); got != "/jobs/"+j.ID().String() {
		t.Errorf("Location = %q, want /jobs/%s", got, j.ID())
	}
	// Check JSON field names independently of the output DTO's tags.
	var body struct {
		ID     string `json:"id"`
		Kind   string `json:"kind"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.ID != j.ID().String() || body.Kind != "Send email" || body.Status != "pending" {
		t.Errorf("unexpected response body: %+v", body)
	}
}

func TestJobHandlerGetJobHTTP(t *testing.T) {
	j := newTestJob(t, domainjob.KindGetPage, domainjob.StatusDone)
	calls := 0
	service := &jobServiceStub{t: t, getJob: func(_ context.Context, id domainjob.ID) (*domainjob.Job, error) {
		calls++
		if id != j.ID() {
			t.Errorf("id = %v, want %v", id, j.ID())
		}
		return j, nil
	}}
	req := httptest.NewRequest(http.MethodGet, "/jobs/"+j.ID().String(), nil)
	resp := httptest.NewRecorder()

	newTestRouter(service).ServeHTTP(resp, req)

	if calls != 1 {
		t.Errorf("GetJob calls = %d, want 1", calls)
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", resp.Code, resp.Body.String())
	}
	var body dtos.JobResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	want := dtos.JobResponse{ID: j.ID().String(), Kind: "Get page", Status: "done"}
	if body != want {
		t.Errorf("body = %+v, want %+v", body, want)
	}
}

func TestJobHandlerCompleteJobHTTP(t *testing.T) {
	wantID := uuid.New()
	calls := 0
	service := &jobServiceStub{t: t, completeJob: func(_ context.Context, id domainjob.ID) error {
		calls++
		if id != wantID {
			t.Errorf("id = %v, want %v", id, wantID)
		}
		return nil
	}}
	req := httptest.NewRequest(http.MethodPost, "/jobs/"+wantID.String()+"/complete", nil)
	resp := httptest.NewRecorder()

	newTestRouter(service).ServeHTTP(resp, req)

	if calls != 1 {
		t.Errorf("CompleteJob calls = %d, want 1", calls)
	}
	if resp.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body = %s", resp.Code, resp.Body.String())
	}
	if resp.Body.Len() != 0 {
		t.Errorf("204 response body = %q, want empty", resp.Body.String())
	}
}

func TestJobHandlerEmptyListHTTP(t *testing.T) {
	service := &jobServiceStub{t: t, listJobs: func(context.Context) ([]*domainjob.Job, error) {
		return nil, nil
	}}
	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	resp := httptest.NewRecorder()

	newTestRouter(service).ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", resp.Code, resp.Body.String())
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	// Missing fields and null are different from the promised empty arrays.
	for key, want := range map[string]string{"jobs": "[]", "partial": "false", "warnings": "[]"} {
		if got := string(body[key]); got != want {
			t.Errorf("JSON field %q = %q, want %q", key, got, want)
		}
	}
}

func TestJobHandlerInvalidCreateRequestHTTP(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{name: "malformed JSON", body: `{"kind":`, wantStatus: http.StatusBadRequest},
		{name: "missing kind", body: `{}`, wantStatus: http.StatusUnprocessableEntity},
		{name: "wrong kind type", body: `{"kind":123}`, wantStatus: http.StatusUnprocessableEntity},
		{name: "body too large", body: `{"kind":"` + strings.Repeat("a", 4096) + `"}`, wantStatus: http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Huma should reject these requests before calling the service.
			router := newTestRouter(&jobServiceStub{t: t})
			req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()
			router.ServeHTTP(resp, req)
			if resp.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body = %s", resp.Code, tt.wantStatus, resp.Body.String())
			}
		})
	}
}

func TestJobHandlerServiceErrorHTTP(t *testing.T) {
	id := uuid.New()
	tests := []struct {
		name       string
		serviceErr error
		wantStatus int
		wantDetail string
	}{
		{"not found", appjob.ErrJobNotFound, http.StatusNotFound, "job not found"},
		{"internal error", errors.New("private storage details"), http.StatusInternalServerError, "internal server error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &jobServiceStub{t: t, getJob: func(context.Context, domainjob.ID) (*domainjob.Job, error) {
				return nil, tt.serviceErr
			}}
			req := httptest.NewRequest(http.MethodGet, "/jobs/"+id.String(), nil)
			resp := httptest.NewRecorder()
			newTestRouter(service).ServeHTTP(resp, req)
			if resp.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", resp.Code, tt.wantStatus, resp.Body.String())
			}
			var body struct {
				Status int    `json:"status"`
				Detail string `json:"detail"`
			}
			if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if body.Status != tt.wantStatus || body.Detail != tt.wantDetail {
				t.Errorf("unexpected error response: %+v", body)
			}
			if strings.Contains(resp.Body.String(), "private storage details") {
				t.Error("response exposes internal service error")
			}
		})
	}
}
