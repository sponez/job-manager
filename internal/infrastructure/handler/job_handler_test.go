package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"
	"uuid"

	"github.com/danielgtaylor/huma/v2"
	appjob "github.com/sponez/job-manager/internal/application/job"
	"github.com/sponez/job-manager/internal/application/worker"
	domainjob "github.com/sponez/job-manager/internal/domain/job"
	"github.com/sponez/job-manager/internal/infrastructure/handler/dtos"
)

type taskQueueStub struct {
	push func(context.Context, worker.Task) error
	task worker.Task
}

func (q *taskQueueStub) Push(ctx context.Context, task worker.Task) error {
	if q.push != nil {
		return q.push(ctx, task)
	}
	q.task = task
	return nil
}

// Only the method needed by a test is configured. Any other call fails the test.
type jobServiceStub struct {
	t           *testing.T
	createJob   func(context.Context, string) (*domainjob.Job, error)
	getJob      func(context.Context, domainjob.ID) (*domainjob.Job, error)
	completeJob func(context.Context, domainjob.ID) error
	listJobs    func(context.Context) ([]*domainjob.Job, error)
	deleteJob   func(context.Context, domainjob.ID) error
	processJob  func(context.Context, domainjob.ID)
}

func (s *jobServiceStub) CreateJob(ctx context.Context, kind string) (*domainjob.Job, error) {
	s.t.Helper()
	if s.createJob == nil {
		s.t.Fatal("unexpected service CreateJob call")
	}
	return s.createJob(ctx, kind)
}

func (s *jobServiceStub) GetJob(ctx context.Context, id domainjob.ID) (*domainjob.Job, error) {
	s.t.Helper()
	if s.getJob == nil {
		s.t.Fatal("unexpected service GetJob call")
	}
	return s.getJob(ctx, id)
}

func (s *jobServiceStub) CompleteJob(ctx context.Context, id domainjob.ID) error {
	s.t.Helper()
	if s.completeJob == nil {
		s.t.Fatal("unexpected service CompleteJob call")
	}
	return s.completeJob(ctx, id)
}

func (s *jobServiceStub) ListJobs(ctx context.Context) ([]*domainjob.Job, error) {
	s.t.Helper()
	if s.listJobs == nil {
		s.t.Fatal("unexpected service ListJobs call")
	}
	return s.listJobs(ctx)
}

func (s *jobServiceStub) DeleteJob(ctx context.Context, id domainjob.ID) error {
	s.t.Helper()
	if s.deleteJob == nil {
		s.t.Fatal("unexpected service DeleteJob call")
	}
	return s.deleteJob(ctx, id)
}

func (s *jobServiceStub) ProcessJob(ctx context.Context, id domainjob.ID) {
	s.t.Helper()
	if s.processJob == nil {
		s.t.Fatal("unexpected service ProcessJob call")
	}
	s.processJob(ctx, id)
}

// The real constructor is used only to prepare a valid service response.
func newTestJob(t *testing.T, kind domainjob.Kind, status domainjob.Status) *domainjob.Job {
	t.Helper()
	j, err := domainjob.New(uuid.New(), kind, status)
	if err != nil {
		t.Fatalf("prepare job: %v", err)
	}
	return j
}

func requireHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()
	var statusErr huma.StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("error = %v, want huma.StatusError with status %d", err, want)
	}
	if got := statusErr.GetStatus(); got != want {
		t.Errorf("HTTP status = %d, want %d", got, want)
	}
}

func TestJobHandlerCreateJob(t *testing.T) {
	j := newTestJob(t, domainjob.KindSendEmail, domainjob.StatusPending)
	tests := []struct {
		name       string
		kind       string
		serviceErr error
		wantStatus int // Zero means that the direct method call should succeed.
	}{
		{name: "created", kind: "Send email"},
		{
			name: "invalid kind", kind: "unknown",
			serviceErr: fmt.Errorf("create job: %w", domainjob.ErrKindIsNotValid),
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "conflict", kind: "Send email",
			serviceErr: appjob.ErrJobAlreadyExists, wantStatus: http.StatusConflict,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			service := &jobServiceStub{t: t, createJob: func(gotCtx context.Context, kind string) (*domainjob.Job, error) {
				calls++
				if gotCtx != ctx || kind != tt.kind {
					t.Errorf("CreateJob received context %v and kind %q, want original context and %q", gotCtx, kind, tt.kind)
				}
				if tt.serviceErr != nil {
					return nil, tt.serviceErr
				}
				return j, nil
			}}
			input := &dtos.CreateJobInput{Body: dtos.CreateJobInputBody{Kind: tt.kind}}
			queue := &taskQueueStub{}
			got, err := NewJobHandler(service, queue).createJob(ctx, input)
			if calls != 1 {
				t.Errorf("CreateJob calls = %d, want 1", calls)
			}
			if tt.wantStatus != 0 {
				if queue.task != nil {
					t.Error("task enqueued after CreateJob failed")
				}
				requireHTTPStatus(t, err, tt.wantStatus)
				if got != nil {
					t.Errorf("output = %+v, want nil on error", got)
				}
				return
			}
			if err != nil || got == nil {
				t.Fatalf("createJob() = %v, %v, want output without error", got, err)
			}
			if queue.task == nil {
				t.Fatal("created job was not enqueued")
			}
			want := dtos.JobResponse{ID: j.ID().String(), Kind: "Send email", Status: "pending"}
			if got.Body != want {
				t.Errorf("body = %+v, want %+v", got.Body, want)
			}
			if got.Location != "/jobs/"+j.ID().String() {
				t.Errorf("Location = %q, want /jobs/%s", got.Location, j.ID())
			}
		})
	}
}

func TestCreateJobExecutesWithWorkerContext(t *testing.T) {
	j := newTestJob(t, domainjob.KindGetPage, domainjob.StatusPending)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	processed := false
	service := &jobServiceStub{
		t:         t,
		createJob: func(context.Context, string) (*domainjob.Job, error) { return j, nil },
		processJob: func(ctx context.Context, id domainjob.ID) {
			if ctx != workerCtx || ctx.Err() != nil || id != j.ID() {
				t.Errorf("ProcessJob received wrong context or ID: %v, %v", ctx, id)
			}
			processed = true
		},
	}
	var task worker.Task
	queue := &taskQueueStub{push: func(ctx context.Context, got worker.Task) error {
		if ctx != requestCtx {
			t.Error("Push did not receive the request context")
		}
		task = got
		return nil
	}}
	_, err := NewJobHandler(service, queue).createJob(requestCtx,
		&dtos.CreateJobInput{Body: dtos.CreateJobInputBody{Kind: string(j.Kind())}})
	if err != nil || task == nil {
		t.Fatalf("createJob = %v, queued task = %v", err, task != nil)
	}
	if processed {
		t.Fatal("task ran inside the request")
	}
	cancelRequest()
	task(workerCtx)
	if !processed {
		t.Fatal("queued task did not process the job")
	}
}

func TestCreateJobQueueFailure(t *testing.T) {
	storageErr := errors.New("storage unavailable")
	for _, tt := range []struct {
		name       string
		queueErr   error
		deleteErr  error
		cancel     bool
		wantStatus int
	}{
		{"full", fmt.Errorf("enqueue: %w", worker.ErrQueueIsFull), nil, false, http.StatusServiceUnavailable},
		{"closed", worker.ErrClosed, nil, false, http.StatusServiceUnavailable},
		{"not started", worker.ErrIsNotStarted, nil, false, http.StatusInternalServerError},
		{"nil task", worker.ErrNilTask, nil, false, http.StatusInternalServerError},
		{"canceled request", context.Canceled, nil, true, http.StatusRequestTimeout},
		{"deadline", context.DeadlineExceeded, nil, true, http.StatusGatewayTimeout},
		{"rollback failed", worker.ErrQueueIsFull, storageErr, false, http.StatusInternalServerError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			j := newTestJob(t, domainjob.KindGetPage, domainjob.StatusPending)
			type contextKey struct{}
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "request value"))
			defer cancel()
			var cleanupCtx context.Context
			deleted := 0
			service := &jobServiceStub{
				t:         t,
				createJob: func(context.Context, string) (*domainjob.Job, error) { return j, nil },
				deleteJob: func(ctx context.Context, id domainjob.ID) error {
					deleted++
					cleanupCtx = ctx
					if ctx.Err() != nil || id != j.ID() || ctx.Value(contextKey{}) != "request value" {
						t.Errorf("rollback context or ID invalid: %v, %v", ctx, id)
					}
					deadline, ok := ctx.Deadline()
					if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 5*time.Second {
						t.Error("rollback must have its own bounded deadline")
					}
					return tt.deleteErr
				},
			}
			queue := &taskQueueStub{push: func(context.Context, worker.Task) error {
				if tt.cancel {
					cancel()
				}
				return tt.queueErr
			}}
			out, err := NewJobHandler(service, queue).createJob(ctx,
				&dtos.CreateJobInput{Body: dtos.CreateJobInputBody{Kind: string(j.Kind())}})
			requireHTTPStatus(t, err, tt.wantStatus)
			if out != nil || deleted != 1 {
				t.Fatalf("output = %v, delete calls = %d", out, deleted)
			}
			if !errors.Is(cleanupCtx.Err(), context.Canceled) {
				t.Error("rollback context was not released")
			}
		})
	}
}

func TestJobHandlerGetJob(t *testing.T) {
	j := newTestJob(t, domainjob.KindGetPage, domainjob.StatusDone)
	tests := []struct {
		name       string
		id         string
		serviceErr error
		wantStatus int
		wantCalls  int
	}{
		{name: "found", id: j.ID().String(), wantCalls: 1},
		{name: "invalid id", id: "not-a-uuid", wantStatus: http.StatusBadRequest},
		{name: "not found", id: j.ID().String(), serviceErr: appjob.ErrJobNotFound, wantStatus: http.StatusNotFound, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			service := &jobServiceStub{t: t, getJob: func(gotCtx context.Context, id domainjob.ID) (*domainjob.Job, error) {
				calls++
				if gotCtx != ctx || id != j.ID() {
					t.Errorf("GetJob received context %v and id %v, want original context and %v", gotCtx, id, j.ID())
				}
				if tt.serviceErr != nil {
					return nil, tt.serviceErr
				}
				return j, nil
			}}
			got, err := NewJobHandler(service, &taskQueueStub{}).getJob(ctx, &dtos.GetJobInput{ID: tt.id})
			if calls != tt.wantCalls {
				t.Errorf("GetJob calls = %d, want %d", calls, tt.wantCalls)
			}
			if tt.wantStatus != 0 {
				requireHTTPStatus(t, err, tt.wantStatus)
				if got != nil {
					t.Errorf("output = %+v, want nil on error", got)
				}
				return
			}
			if err != nil || got == nil {
				t.Fatalf("getJob() = %v, %v, want output without error", got, err)
			}
			want := dtos.JobResponse{ID: j.ID().String(), Kind: "Get page", Status: "done"}
			if got.Body != want {
				t.Errorf("body = %+v, want %+v", got.Body, want)
			}
		})
	}
}

func TestJobHandlerCompleteJob(t *testing.T) {
	id := uuid.New()
	tests := []struct {
		name       string
		id         string
		serviceErr error
		wantStatus int
		wantCalls  int
	}{
		{name: "completed", id: id.String(), wantCalls: 1},
		{name: "invalid id", id: "not-a-uuid", wantStatus: http.StatusBadRequest},
		{name: "not found", id: id.String(), serviceErr: appjob.ErrJobNotFound, wantStatus: http.StatusNotFound, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			service := &jobServiceStub{t: t, completeJob: func(gotCtx context.Context, gotID domainjob.ID) error {
				calls++
				if gotCtx != ctx || gotID != id {
					t.Errorf("CompleteJob received context %v and id %v, want original context and %v", gotCtx, gotID, id)
				}
				return tt.serviceErr
			}}
			got, err := NewJobHandler(service, &taskQueueStub{}).completeJob(ctx, &dtos.CompleteJobInput{ID: tt.id})
			if calls != tt.wantCalls {
				t.Errorf("CompleteJob calls = %d, want %d", calls, tt.wantCalls)
			}
			if tt.wantStatus != 0 {
				requireHTTPStatus(t, err, tt.wantStatus)
				if got != nil {
					t.Errorf("output = %+v, want nil on error", got)
				}
				return
			}
			if err != nil || got == nil {
				t.Fatalf("completeJob() = %v, %v, want output without error", got, err)
			}
		})
	}
}

func TestJobHandlerGetJobs(t *testing.T) {
	first := newTestJob(t, domainjob.KindSendEmail, domainjob.StatusPending)
	second := newTestJob(t, domainjob.KindGetPage, domainjob.StatusDone)
	firstResponse := dtos.JobResponse{ID: first.ID().String(), Kind: "Send email", Status: "pending"}
	secondResponse := dtos.JobResponse{ID: second.ID().String(), Kind: "Get page", Status: "done"}
	readErr := errors.New("cannot decode stored job: internal details")
	tests := []struct {
		name         string
		jobs         []*domainjob.Job
		serviceErr   error
		wantJobs     []dtos.JobResponse
		wantPartial  bool
		wantWarnings []string
		wantStatus   int
	}{
		{name: "full list", jobs: []*domainjob.Job{first, second}, wantJobs: []dtos.JobResponse{firstResponse, secondResponse}},
		{name: "empty list", jobs: []*domainjob.Job{}},
		{name: "nil list"},
		{
			name: "partial list", jobs: []*domainjob.Job{first}, serviceErr: readErr,
			wantJobs: []dtos.JobResponse{firstResponse}, wantPartial: true,
			wantWarnings: []string{"Some jobs could not be read"},
		},
		{name: "no readable jobs", jobs: []*domainjob.Job{}, serviceErr: readErr, wantStatus: http.StatusInternalServerError},
		{name: "service failure", serviceErr: readErr, wantStatus: http.StatusInternalServerError},
		{
			name: "canceled with partial results", jobs: []*domainjob.Job{first},
			serviceErr: fmt.Errorf("list jobs: %w", context.Canceled), wantStatus: http.StatusRequestTimeout,
		},
		{
			name: "deadline with partial results", jobs: []*domainjob.Job{first},
			serviceErr: fmt.Errorf("list jobs: %w", context.DeadlineExceeded), wantStatus: http.StatusGatewayTimeout,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			service := &jobServiceStub{t: t, listJobs: func(gotCtx context.Context) ([]*domainjob.Job, error) {
				calls++
				if gotCtx != ctx {
					t.Error("ListJobs did not receive the original context")
				}
				return tt.jobs, tt.serviceErr
			}}
			got, err := NewJobHandler(service, &taskQueueStub{}).getJobs(ctx, &dtos.GetJobsInput{})
			if calls != 1 {
				t.Errorf("ListJobs calls = %d, want 1", calls)
			}
			if tt.wantStatus != 0 {
				requireHTTPStatus(t, err, tt.wantStatus)
				if got != nil {
					t.Errorf("output = %+v, want nil on error", got)
				}
				return
			}
			if err != nil || got == nil {
				t.Fatalf("getJobs() = %v, %v, want output without error", got, err)
			}
			if !slices.Equal(got.Body.Jobs, tt.wantJobs) {
				t.Errorf("jobs = %+v, want %+v", got.Body.Jobs, tt.wantJobs)
			}
			if got.Body.Partial != tt.wantPartial {
				t.Errorf("partial = %v, want %v", got.Body.Partial, tt.wantPartial)
			}
			if !slices.Equal(got.Body.Warnings, tt.wantWarnings) {
				t.Errorf("warnings = %v, want %v", got.Body.Warnings, tt.wantWarnings)
			}
			// Non-nil slices must be encoded as [] rather than null in JSON.
			if got.Body.Jobs == nil || got.Body.Warnings == nil {
				t.Error("jobs and warnings must be non-nil slices")
			}
		})
	}
}

func TestJobHTTPError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantText   string
	}{
		{"not found", appjob.ErrJobNotFound, http.StatusNotFound, "job not found"},
		{"conflict", appjob.ErrJobAlreadyExists, http.StatusConflict, "job already exists"},
		{"canceled", context.Canceled, http.StatusRequestTimeout, "request canceled"},
		{"deadline", context.DeadlineExceeded, http.StatusGatewayTimeout, "request deadline exceeded"},
		{"unknown", errors.New("database password: secret"), http.StatusInternalServerError, "internal server error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, wrapped := range []bool{false, true} {
				t.Run(fmt.Sprintf("wrapped=%v", wrapped), func(t *testing.T) {
					err := tt.err
					if wrapped {
						err = fmt.Errorf("service operation: %w", err)
					}
					got := jobHTTPError(context.Background(), err)
					requireHTTPStatus(t, got, tt.wantStatus)
					if got.Error() != tt.wantText {
						t.Errorf("public error = %q, want %q", got.Error(), tt.wantText)
					}
				})
			}
		})
	}
}
