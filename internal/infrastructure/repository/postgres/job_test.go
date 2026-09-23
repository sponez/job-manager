package postgres

import (
	"context"
	"errors"
	"testing"
	"time"
	"uuid"

	appjob "github.com/sponez/job-manager/internal/application/job"
	"github.com/sponez/job-manager/internal/domain/job"
)

func TestJobRepositoryCreateAndGet(t *testing.T) {
	ctx, repository := newTestJobRepository(t)
	want := createTestJob(t, ctx, repository, job.KindSendEmail, job.StatusPending)
	got, err := repository.GetJob(ctx, want.ID())
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.ID() != want.ID() || got.Kind() != want.Kind() || got.Status() != want.Status() {
		t.Fatalf("job = %+v, want %+v", got, want)
	}
}

func TestJobRepositoryCreateDuplicate(t *testing.T) {
	ctx, repository := newTestJobRepository(t)
	j := createTestJob(t, ctx, repository, job.KindGetPage, job.StatusPending)
	if err := repository.CreateJob(ctx, j); !errors.Is(err, appjob.ErrJobAlreadyExists) {
		t.Fatalf("duplicate error = %v, want ErrJobAlreadyExists", err)
	}
}

func TestJobRepositoryGetNotFound(t *testing.T) {
	ctx, repository := newTestJobRepository(t)
	if _, err := repository.GetJob(ctx, uuid.New()); !errors.Is(err, appjob.ErrJobNotFound) {
		t.Fatalf("get missing job error = %v, want ErrJobNotFound", err)
	}
}

func TestJobRepositoryUpdateStatus(t *testing.T) {
	ctx, repository := newTestJobRepository(t)
	want := createTestJob(t, ctx, repository, job.KindSendEmail, job.StatusPending)
	if err := repository.UpdateStatusByID(ctx, want.ID(), job.StatusDone); err != nil {
		t.Fatalf("update status: %v", err)
	}
	got, err := repository.GetJob(ctx, want.ID())
	if err != nil {
		t.Fatalf("get updated job: %v", err)
	}
	if got.ID() != want.ID() || got.Kind() != want.Kind() || got.Status() != job.StatusDone {
		t.Fatalf("unexpected job after update: %+v", got)
	}
}

func TestJobRepositoryUpdateStatusNotFound(t *testing.T) {
	ctx, repository := newTestJobRepository(t)
	if err := repository.UpdateStatusByID(ctx, uuid.New(), job.StatusDone); !errors.Is(err, appjob.ErrJobNotFound) {
		t.Fatalf("update missing job error = %v, want ErrJobNotFound", err)
	}
}

func TestJobRepositoryListEmpty(t *testing.T) {
	ctx, repository := newTestJobRepository(t)
	jobs, err := repository.ListJobs(ctx)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("list empty database: jobs = %v, error = %v", jobs, err)
	}
}

func TestJobRepositoryList(t *testing.T) {
	ctx, repository := newTestJobRepository(t)
	want := createTestJob(t, ctx, repository, job.KindSendEmail, job.StatusDone)
	jobs, err := repository.ListJobs(ctx)
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID() != want.ID() || jobs[0].Kind() != want.Kind() || jobs[0].Status() != want.Status() {
		t.Fatalf("unexpected jobs: %+v", jobs)
	}
}

// Tests share the pool and run sequentially; each starts with an empty jobs table.
func newTestJobRepository(t *testing.T) (context.Context, *JobRepository) {
	t.Helper()
	pool := repositoryTestPool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	if _, err := pool.Exec(ctx, "TRUNCATE TABLE jobs"); err != nil {
		t.Fatalf("reset jobs: %v", err)
	}
	return ctx, NewJobRepository(pool)
}

func createTestJob(t *testing.T, ctx context.Context, repository *JobRepository, kind job.Kind, status job.Status) *job.Job {
	t.Helper()
	j, err := job.New(uuid.New(), kind, status)
	if err != nil {
		t.Fatalf("prepare job: %v", err)
	}
	if err := repository.CreateJob(ctx, j); err != nil {
		t.Fatalf("create job: %v", err)
	}
	return j
}
