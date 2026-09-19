package job

import (
	"context"
	"errors"
	"slices"
	"testing"
	"uuid"

	"github.com/sponez/job-manager/internal/domain/job"
)

type jobRepositoryStub struct {
	t                *testing.T
	createJob        func(ctx context.Context, j *job.Job) error
	getJob           func(ctx context.Context, id job.ID) (*job.Job, error)
	updateStatusByID func(ctx context.Context, id job.ID, s job.Status) error
	listJobs         func(ctx context.Context) ([]*job.Job, error)
}

func (r *jobRepositoryStub) CreateJob(ctx context.Context, j *job.Job) error {
	r.t.Helper()
	if r.createJob == nil {
		r.t.Fatal("unexpected repository CreateJob call")
	}
	return r.createJob(ctx, j)
}

func (r *jobRepositoryStub) GetJob(ctx context.Context, id job.ID) (*job.Job, error) {
	r.t.Helper()
	if r.getJob == nil {
		r.t.Fatal("unexpected repository GetJob call")
	}
	return r.getJob(ctx, id)
}

func (r *jobRepositoryStub) UpdateStatusByID(ctx context.Context, id job.ID, s job.Status) error {
	r.t.Helper()
	if r.updateStatusByID == nil {
		r.t.Fatal("unexpected repository UpdateStatusByID call")
	}
	return r.updateStatusByID(ctx, id, s)
}

func (r *jobRepositoryStub) ListJobs(ctx context.Context) ([]*job.Job, error) {
	r.t.Helper()
	if r.listJobs == nil {
		r.t.Fatal("unexpected repository ListJobs call")
	}
	return r.listJobs(ctx)
}

func TestCreateJob(t *testing.T) {
	storageErr := errors.New("storage unavailable")
	tests := []struct {
		name      string
		kind      string
		repoErr   error
		wantErr   error
		wantCalls int
	}{
		{
			name:      "creates pending email",
			kind:      string(job.KindSendEmail),
			wantCalls: 1,
		},
		{
			name:      "creates pending page",
			kind:      string(job.KindGetPage),
			wantCalls: 1,
		},
		{
			name:    "invalid kind is not saved",
			kind:    "unknown",
			wantErr: job.ErrKindIsNotValid,
		},
		{
			name:      "storage error",
			kind:      string(job.KindSendEmail),
			repoErr:   storageErr,
			wantErr:   storageErr,
			wantCalls: 1,
		},
		{
			name:      "duplicate ID",
			kind:      string(job.KindSendEmail),
			repoErr:   ErrJobAlreadyExists,
			wantErr:   ErrJobAlreadyExists,
			wantCalls: 1,
		},
		{
			name:      "repository cancellation",
			kind:      string(job.KindSendEmail),
			repoErr:   context.Canceled,
			wantErr:   context.Canceled,
			wantCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if errors.Is(tt.repoErr, context.Canceled) {
				cancel()
			}

			calls := 0
			var saved *job.Job
			repo := &jobRepositoryStub{
				t: t,
				createJob: func(gotCtx context.Context, j *job.Job) error {
					calls++
					if gotCtx != ctx {
						t.Error("repository received a different context")
					}
					if j == nil {
						t.Fatal("repository received nil job")
					}
					if string(j.Kind()) != tt.kind || j.Status() != job.StatusPending {
						t.Errorf("saved job kind=%q, status=%q; want kind=%q, status=%q", j.Kind(), j.Status(), tt.kind, job.StatusPending)
					}
					if j.ID() == (job.ID{}) {
						t.Error("saved job has a zero ID")
					}
					saved = j
					return tt.repoErr
				},
			}

			got, err := New(repo).CreateJob(ctx, tt.kind)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("CreateJob() error = %v, want %v", err, tt.wantErr)
			}
			if calls != tt.wantCalls {
				t.Errorf("repository CreateJob calls = %d, want %d", calls, tt.wantCalls)
			}
			if tt.wantErr != nil {
				if got != nil {
					t.Errorf("CreateJob() = %+v, want nil on error", got)
				}
				return
			}
			if got == nil || got != saved {
				t.Error("CreateJob() did not return the job passed to the repository")
			}
		})
	}
}

func TestGetJob(t *testing.T) {
	// The service only passes this object through; its contents are irrelevant here.
	expectedJob := &job.Job{}
	storageErr := errors.New("storage unavailable")
	tests := []struct {
		name    string
		repoJob *job.Job
		repoErr error
	}{
		{name: "found", repoJob: expectedJob},
		{name: "not found", repoErr: ErrJobNotFound},
		{name: "storage error", repoErr: storageErr},
		{name: "deadline exceeded", repoErr: context.DeadlineExceeded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			id := uuid.New()
			calls := 0
			repo := &jobRepositoryStub{
				t: t,
				getJob: func(gotCtx context.Context, gotID job.ID) (*job.Job, error) {
					calls++
					if gotCtx != ctx {
						t.Error("repository received a different context")
					}
					if gotID != id {
						t.Errorf("repository received ID %s, want %s", gotID, id)
					}
					return tt.repoJob, tt.repoErr
				},
			}

			got, err := New(repo).GetJob(ctx, id)
			if !errors.Is(err, tt.repoErr) {
				t.Errorf("GetJob() error = %v, want %v", err, tt.repoErr)
			}
			if got != tt.repoJob {
				t.Errorf("GetJob() = %p, want %p", got, tt.repoJob)
			}
			if calls != 1 {
				t.Errorf("repository GetJob calls = %d, want 1", calls)
			}
		})
	}
}

func TestCompleteJob(t *testing.T) {
	storageErr := errors.New("storage unavailable")
	tests := []struct {
		name    string
		repoErr error
	}{
		{name: "completed"},
		{name: "not found", repoErr: ErrJobNotFound},
		{name: "storage error", repoErr: storageErr},
		{name: "deadline exceeded", repoErr: context.DeadlineExceeded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			id := uuid.New()
			calls := 0
			repo := &jobRepositoryStub{
				t: t,
				updateStatusByID: func(gotCtx context.Context, gotID job.ID, status job.Status) error {
					calls++
					if gotCtx != ctx {
						t.Error("repository received a different context")
					}
					if gotID != id {
						t.Errorf("repository received ID %s, want %s", gotID, id)
					}
					if status != job.StatusDone {
						t.Errorf("repository received status %q, want %q", status, job.StatusDone)
					}
					return tt.repoErr
				},
			}

			if err := New(repo).CompleteJob(ctx, id); !errors.Is(err, tt.repoErr) {
				t.Errorf("CompleteJob() error = %v, want %v", err, tt.repoErr)
			}
			if calls != 1 {
				t.Errorf("repository UpdateStatusByID calls = %d, want 1", calls)
			}
		})
	}
}

func TestListJobs(t *testing.T) {
	first, second := &job.Job{}, &job.Job{}
	storageErr := errors.New("storage unavailable")
	firstRecordErr := errors.New("first record could not be read")
	secondRecordErr := errors.New("second record could not be read")
	partialErr := errors.Join(firstRecordErr, secondRecordErr)
	tests := []struct {
		name     string
		repoJobs []*job.Job
		repoErr  error
	}{
		{name: "full list", repoJobs: []*job.Job{first, second}},
		{name: "empty list", repoJobs: []*job.Job{}},
		{name: "nil list"},
		{name: "storage error", repoErr: storageErr},
		{name: "partial list", repoJobs: []*job.Job{first}, repoErr: partialErr},
		{name: "no readable records", repoJobs: []*job.Job{}, repoErr: partialErr},
		{name: "canceled", repoErr: context.Canceled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if errors.Is(tt.repoErr, context.Canceled) {
				cancel()
			}
			calls := 0
			repo := &jobRepositoryStub{
				t: t,
				listJobs: func(gotCtx context.Context) ([]*job.Job, error) {
					calls++
					if gotCtx != ctx {
						t.Error("repository received a different context")
					}
					return tt.repoJobs, tt.repoErr
				},
			}

			got, err := New(repo).ListJobs(ctx)
			if !errors.Is(err, tt.repoErr) {
				t.Errorf("ListJobs() error = %v, want %v", err, tt.repoErr)
			}
			if tt.repoErr == partialErr {
				for _, cause := range []error{firstRecordErr, secondRecordErr} {
					if !errors.Is(err, cause) {
						t.Errorf("ListJobs() error = %v, lost cause %v", err, cause)
					}
				}
			}
			if !slices.Equal(got, tt.repoJobs) || (got == nil) != (tt.repoJobs == nil) {
				t.Errorf("ListJobs() = %#v, want %#v", got, tt.repoJobs)
			}
			if calls != 1 {
				t.Errorf("repository ListJobs calls = %d, want 1", calls)
			}
		})
	}
}
