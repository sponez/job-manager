package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
	"uuid"

	appjob "github.com/sponez/job-manager/internal/application/job"
	"github.com/sponez/job-manager/internal/domain/job"
)

func newTestJob(t *testing.T, id job.ID, kind job.Kind, status job.Status) *job.Job {
	t.Helper()
	j, err := job.New(id, kind, status)
	if err != nil {
		t.Fatalf("prepare job: %v", err)
	}
	return j
}

func requireSameJob(t *testing.T, got, want *job.Job) {
	t.Helper()
	if got == nil {
		t.Fatal("got nil job")
	}
	if got.ID() != want.ID() || got.Kind() != want.Kind() || got.Status() != want.Status() {
		t.Errorf("job = %+v, want %+v", got, want)
	}
}

func TestCreateAndGetJob(t *testing.T) {
	tests := []struct {
		name          string
		newRepository func() *MemoryJobRepository
	}{
		{name: "constructor", newRepository: New},
		{name: "zero value", newRepository: func() *MemoryJobRepository { return &MemoryJobRepository{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.newRepository()
			ctx := context.Background()
			want := newTestJob(t, uuid.New(), job.KindSendEmail, job.StatusPending)
			if err := r.CreateJob(ctx, want); err != nil {
				t.Fatalf("CreateJob: %v", err)
			}
			got, err := r.GetJob(ctx, want.ID())
			if err != nil {
				t.Fatalf("GetJob: %v", err)
			}
			requireSameJob(t, got, want)
		})
	}
}

func TestCreateJobDuplicate(t *testing.T) {
	r := New()
	ctx := context.Background()
	id := uuid.New()
	original := newTestJob(t, id, job.KindSendEmail, job.StatusPending)
	duplicate := newTestJob(t, id, job.KindGetPage, job.StatusDone)
	if err := r.CreateJob(ctx, original); err != nil {
		t.Fatalf("prepare repository: %v", err)
	}
	if err := r.CreateJob(ctx, duplicate); !errors.Is(err, appjob.ErrJobAlreadyExists) {
		t.Fatalf("CreateJob duplicate error = %v, want ErrJobAlreadyExists", err)
	}
	got, err := r.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	// A rejected duplicate must not replace the original record.
	requireSameJob(t, got, original)
}

func TestCreateJobNil(t *testing.T) {
	r := New()
	if err := r.CreateJob(context.Background(), nil); err == nil {
		t.Fatal("CreateJob(nil) succeeded, want error")
	}
	jobs, err := r.ListJobs(context.Background())
	if err != nil || len(jobs) != 0 {
		t.Errorf("ListJobs() = %v, %v, want empty repository without error", jobs, err)
	}
}

func TestMissingJob(t *testing.T) {
	r := New()
	ctx := context.Background()
	id := uuid.New()
	got, err := r.GetJob(ctx, id)
	if !errors.Is(err, appjob.ErrJobNotFound) || got != nil {
		t.Errorf("GetJob() = %v, %v, want nil, ErrJobNotFound", got, err)
	}
	if err := r.UpdateStatusByID(ctx, id, job.StatusDone); !errors.Is(err, appjob.ErrJobNotFound) {
		t.Errorf("UpdateStatusByID error = %v, want ErrJobNotFound", err)
	}
	jobs, err := r.ListJobs(ctx)
	if err != nil || len(jobs) != 0 {
		t.Errorf("ListJobs() = %v, %v, want empty repository without error", jobs, err)
	}
}

func TestUpdateStatusByID(t *testing.T) {
	r := New()
	ctx := context.Background()
	original := newTestJob(t, uuid.New(), job.KindGetPage, job.StatusPending)
	if err := r.CreateJob(ctx, original); err != nil {
		t.Fatalf("prepare repository: %v", err)
	}
	before, err := r.GetJob(ctx, original.ID())
	if err != nil {
		t.Fatalf("GetJob before update: %v", err)
	}
	if err := r.UpdateStatusByID(ctx, original.ID(), job.StatusDone); err != nil {
		t.Fatalf("UpdateStatusByID: %v", err)
	}
	got, err := r.GetJob(ctx, original.ID())
	if err != nil {
		t.Fatalf("GetJob after update: %v", err)
	}
	want := newTestJob(t, original.ID(), job.KindGetPage, job.StatusDone)
	requireSameJob(t, got, want)
	// Updating storage must not mutate objects previously given to the caller.
	requireSameJob(t, before, original)
	if original.Status() != job.StatusPending {
		t.Error("updating storage changed the original job")
	}
}

func TestGetJobCorruptRecord(t *testing.T) {
	r := New()
	id := uuid.New()
	// Simulate corrupt stored data, bypassing normal creation on purpose.
	r.jobs[id] = &JobDTO{ID: id, Kind: "unknown", Status: "invalid"}
	got, err := r.GetJob(context.Background(), id)
	if got != nil {
		t.Errorf("GetJob() = %v, want nil", got)
	}
	if !errors.Is(err, job.ErrKindIsNotValid) || !errors.Is(err, job.ErrStatusIsNotValid) {
		t.Errorf("error = %v, want both validation errors", err)
	}
}

func TestListJobs(t *testing.T) {
	first := newTestJob(t, uuid.New(), job.KindSendEmail, job.StatusPending)
	second := newTestJob(t, uuid.New(), job.KindGetPage, job.StatusDone)
	badKindID, badStatusID := uuid.New(), uuid.New()
	badRecords := []*JobDTO{
		{ID: badKindID, Kind: "unknown", Status: "pending"},
		{ID: badStatusID, Kind: "Get page", Status: "invalid"},
	}
	tests := []struct {
		name           string
		validJobs      []*job.Job
		corruptRecords []*JobDTO
	}{
		{name: "empty"},
		{name: "all readable", validJobs: []*job.Job{first, second}},
		{name: "partial results", validJobs: []*job.Job{first, second}, corruptRecords: badRecords},
		{name: "none readable", corruptRecords: badRecords},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New()
			ctx := context.Background()
			for _, j := range tt.validJobs {
				if err := r.CreateJob(ctx, j); err != nil {
					t.Fatalf("prepare repository: %v", err)
				}
			}
			for _, dto := range tt.corruptRecords {
				r.jobs[dto.ID] = dto
			}
			got, err := r.ListJobs(ctx)
			if len(tt.corruptRecords) == 0 {
				if err != nil {
					t.Fatalf("ListJobs: %v", err)
				}
			} else if !errors.Is(err, job.ErrKindIsNotValid) || !errors.Is(err, job.ErrStatusIsNotValid) {
				t.Errorf("error = %v, want both skipped records' errors", err)
			}
			if len(got) != len(tt.validJobs) {
				t.Fatalf("job count = %d, want %d", len(got), len(tt.validJobs))
			}
			// Map iteration order is unspecified, so compare by ID, not position.
			byID := make(map[job.ID]*job.Job, len(got))
			for _, j := range got {
				if j == nil {
					t.Fatal("ListJobs returned a nil job")
				}
				byID[j.ID()] = j
			}
			for _, want := range tt.validJobs {
				requireSameJob(t, byID[want.ID()], want)
			}
		})
	}
}

func TestDeleteJob(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		r := New()
		j := newTestJob(t, uuid.New(), job.KindGetPage, job.StatusPending)
		r.CreateJob(ctx, j)

		if rj, err := r.GetJob(ctx, j.ID()); rj == nil || err != nil {
			t.Fatalf("failed to create a job")
		} else {
			requireSameJob(t, rj, j)
		}

		r.DeleteJob(ctx, j.ID())

		if rj, err := r.GetJob(ctx, j.ID()); err != appjob.ErrJobNotFound {
			t.Errorf("DeleteJob() expected %v to be deleted, but this not happened", rj)
		}
	})
}

func TestRepositoryCanceledContext(t *testing.T) {
	tests := []struct {
		name       string
		newContext func() (context.Context, context.CancelFunc)
		wantErr    error
	}{
		{
			name: "canceled",
			newContext: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			wantErr: context.Canceled,
		},
		{
			name: "deadline exceeded",
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			wantErr: context.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New()
			original := newTestJob(t, uuid.New(), job.KindSendEmail, job.StatusPending)
			if err := r.CreateJob(context.Background(), original); err != nil {
				t.Fatalf("prepare repository: %v", err)
			}
			ctx, cancel := tt.newContext()
			defer cancel()
			newJob := newTestJob(t, uuid.New(), job.KindGetPage, job.StatusPending)
			if err := r.CreateJob(ctx, newJob); !errors.Is(err, tt.wantErr) {
				t.Errorf("CreateJob error = %v, want %v", err, tt.wantErr)
			}
			if got, err := r.GetJob(ctx, original.ID()); got != nil || !errors.Is(err, tt.wantErr) {
				t.Errorf("GetJob() = %v, %v, want nil, %v", got, err, tt.wantErr)
			}
			if err := r.UpdateStatusByID(ctx, original.ID(), job.StatusDone); !errors.Is(err, tt.wantErr) {
				t.Errorf("UpdateStatusByID error = %v, want %v", err, tt.wantErr)
			}
			if got, err := r.ListJobs(ctx); got != nil || !errors.Is(err, tt.wantErr) {
				t.Errorf("ListJobs() = %v, %v, want nil, %v", got, err, tt.wantErr)
			}
			if err := r.DeleteJob(ctx, original.ID()); !errors.Is(err, tt.wantErr) {
				t.Errorf("DeleteJob error = %v, want %v", err, tt.wantErr)
			}
			// Check that rejected writes left storage unchanged, using a live context.
			jobs, err := r.ListJobs(context.Background())
			if err != nil || len(jobs) != 1 {
				t.Fatalf("repository after canceled writes = %v, %v, want one job", jobs, err)
			}
			requireSameJob(t, jobs[0], original)
		})
	}
}

func TestRepositoryConcurrentAccess(t *testing.T) {
	r := New()
	ctx := context.Background()
	const workers = 16
	jobs := make([]*job.Job, workers)
	for i := range jobs {
		jobs[i] = newTestJob(t, uuid.New(), job.KindSendEmail, job.StatusPending)
	}
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func(j *job.Job) {
			defer wg.Done()
			<-start
			if err := r.CreateJob(ctx, j); err != nil {
				errs <- fmt.Errorf("create: %w", err)
				return
			}
			if _, err := r.GetJob(ctx, j.ID()); err != nil {
				errs <- fmt.Errorf("get: %w", err)
				return
			}
			if err := r.UpdateStatusByID(ctx, j.ID(), job.StatusDone); err != nil {
				errs <- fmt.Errorf("update: %w", err)
				return
			}
			if _, err := r.ListJobs(ctx); err != nil {
				errs <- fmt.Errorf("list: %w", err)
				return
			}
			if err := r.DeleteJob(ctx, j.ID()); err != nil {
				errs <- fmt.Errorf("delete: %w", err)
			}
		}(j)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	got, err := r.ListJobs(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("ListJobs() count = %d, error = %v, want empty repository", len(got), err)
	}
}
