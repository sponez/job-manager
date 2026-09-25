package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	appjob "github.com/sponez/job-manager/internal/application/job"
	"github.com/sponez/job-manager/internal/domain/job"
)

func requireSameJob(t *testing.T, got, want *job.Job) {
	t.Helper()
	if got == nil {
		t.Fatal("got nil job")
	}
	if got.ID() != want.ID() || got.Kind() != want.Kind() || got.Status() != want.Status() {
		t.Errorf("job = %+v, want %+v", got, want)
	}
}

func TestJobRepositoryCreateAndGet(t *testing.T) {
	ctx, repository := newTestJobRepository(t)
	want := createTestJob(t, ctx, repository, job.KindSendEmail, job.StatusPending)
	got, err := repository.GetJob(ctx, want.ID())
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	requireSameJob(t, got, want)
}

func TestJobRepositoryCreateDuplicate(t *testing.T) {
	ctx, repository := newTestJobRepository(t)
	j := createTestJob(t, ctx, repository, job.KindGetPage, job.StatusPending)
	duplicate, err := job.New(j.ID(), job.KindSendEmail, job.StatusDone)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.CreateJob(ctx, duplicate); !errors.Is(err, appjob.ErrJobAlreadyExists) {
		t.Fatalf("duplicate error = %v, want ErrJobAlreadyExists", err)
	}
	got, err := repository.GetJob(ctx, j.ID())
	if err != nil {
		t.Fatalf("get original after duplicate: %v", err)
	}
	requireSameJob(t, got, j)
}

func TestJobRepositoryCreateNil(t *testing.T) {
	ctx, repository := newTestJobRepository(t)
	if err := repository.CreateJob(ctx, nil); err == nil {
		t.Fatal("CreateJob(nil) succeeded, want error")
	}
	jobs, err := repository.ListJobs(ctx)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("repository after nil job: jobs = %v, error = %v, want empty repository", jobs, err)
	}
}

func TestJobRepositoryGetCorruptRecord(t *testing.T) {
	tests := []struct {
		name      string
		kind      string
		status    string
		badKind   bool
		badStatus bool
	}{
		{name: "invalid kind", kind: "unknown", status: string(job.StatusPending), badKind: true},
		{name: "invalid status", kind: string(job.KindSendEmail), status: "invalid", badStatus: true},
		{name: "both invalid", kind: "unknown", status: "invalid", badKind: true, badStatus: true},
		{name: "empty kind", kind: "", status: string(job.StatusPending), badKind: true},
		{name: "empty status", kind: string(job.KindSendEmail), status: "", badStatus: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, repository := newTestJobRepository(t)
			id := uuid.New()
			insertRawTestJob(t, ctx, id, tt.kind, tt.status)
			got, err := repository.GetJob(ctx, id)
			if got != nil {
				t.Errorf("GetJob returned %v, want nil for corrupt data", got)
			}
			if errors.Is(err, job.ErrKindIsNotValid) != tt.badKind || errors.Is(err, job.ErrStatusIsNotValid) != tt.badStatus {
				t.Fatalf("error = %v, want invalid kind = %t, invalid status = %t", err, tt.badKind, tt.badStatus)
			}
			if err == nil || !strings.Contains(err.Error(), id.String()) {
				t.Fatalf("error = %v, want corrupt record ID %s", err, id)
			}
			if errors.Is(err, appjob.ErrJobNotFound) {
				t.Fatal("corrupt record must not be reported as missing")
			}
		})
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

func TestJobRepositoryListCorruptRecords(t *testing.T) {
	for _, allCorrupt := range []bool{false, true} {
		name := "partial results"
		if allCorrupt {
			name = "all records corrupt"
		}
		t.Run(name, func(t *testing.T) {
			ctx, repository := newTestJobRepository(t)
			// Insert in reverse order, with good rows before and after bad rows
			// in UUID order, to verify sorting and continued reading after errors.
			rows := []struct {
				id      job.ID
				kind    string
				status  string
				corrupt bool
			}{
				{id: job.ID{15: 5}, kind: string(job.KindGetPage), status: string(job.StatusDone)},
				{id: job.ID{15: 4}, kind: "unknown", status: "invalid", corrupt: true},
				{id: job.ID{15: 3}, kind: string(job.KindSendEmail), status: "invalid", corrupt: true},
				{id: job.ID{15: 2}, kind: "unknown", status: string(job.StatusPending), corrupt: true},
				{id: job.ID{15: 1}, kind: string(job.KindSendEmail), status: string(job.StatusPending)},
			}
			var want []*job.Job
			var corruptIDs []job.ID
			for _, row := range rows {
				if allCorrupt && !row.corrupt {
					continue
				}
				insertRawTestJob(t, ctx, row.id, row.kind, row.status)
				if row.corrupt {
					corruptIDs = append(corruptIDs, row.id)
					continue
				}
				j, err := job.New(row.id, job.Kind(row.kind), job.Status(row.status))
				if err != nil {
					t.Fatal(err)
				}
				want = append([]*job.Job{j}, want...)
			}
			got, err := repository.ListJobs(ctx)
			if !errors.Is(err, job.ErrKindIsNotValid) || !errors.Is(err, job.ErrStatusIsNotValid) {
				t.Fatalf("error = %v, want both validation errors", err)
			}
			for _, id := range corruptIDs {
				if !strings.Contains(err.Error(), id.String()) {
					t.Errorf("error = %v, missing corrupt record ID %s", err, id)
				}
			}
			if got == nil || len(got) != len(want) {
				t.Fatalf("ListJobs returned %v, want a non-nil slice with %d jobs", got, len(want))
			}
			for i, j := range want {
				requireSameJob(t, got[i], j)
			}
			var count int
			if err := repositoryTestPool(t).QueryRow(ctx, "SELECT count(*) FROM jobs").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != len(want)+len(corruptIDs) {
				t.Fatal("listing jobs must not delete corrupt records")
			}
		})
	}
}

func TestDeleteJob(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx, repository := newTestJobRepository(t)
		j := createTestJob(t, ctx, repository, job.KindGetPage, job.StatusPending)

		if rj, err := repository.GetJob(ctx, j.ID()); rj == nil || err != nil {
			t.Fatalf("failed to create a job")
		} else {
			requireSameJob(t, rj, j)
		}

		repository.DeleteJob(ctx, j.ID())

		if rj, err := repository.GetJob(ctx, j.ID()); err != appjob.ErrJobNotFound {
			t.Errorf("DeleteJob() expected %v to be deleted, but this not happened", rj)
		}
	})
}

func TestJobRepositoryDatabaseErrors(t *testing.T) {
	sharedPool := repositoryTestPool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	// An isolated pool cannot see jobs, without changing the shared schema or pool.
	cfg := sharedPool.Config()
	cfg.ConnConfig.RuntimeParams["search_path"] = "pg_catalog"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("connect isolated pool: %v", err)
	}
	repository := NewJobRepository(pool)
	j, err := job.New(uuid.New(), job.KindSendEmail, job.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		run  func(*testing.T) error
	}{
		{name: "create", run: func(t *testing.T) error { return repository.CreateJob(ctx, j) }},
		{name: "get", run: func(t *testing.T) error {
			got, err := repository.GetJob(ctx, j.ID())
			if got != nil {
				t.Error("GetJob must return nil on database failure")
			}
			return err
		}},
		{name: "update", run: func(t *testing.T) error { return repository.UpdateStatusByID(ctx, j.ID(), job.StatusDone) }},
		{name: "list", run: func(t *testing.T) error {
			got, err := repository.ListJobs(ctx)
			if got != nil {
				t.Error("ListJobs must return nil on database failure")
			}
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run(t)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
				t.Fatalf("error = %v, want wrapped PostgreSQL undefined_table error", err)
			}
			if errors.Is(err, appjob.ErrJobNotFound) || errors.Is(err, appjob.ErrJobAlreadyExists) {
				t.Fatal("database failure must not become a domain error")
			}
		})
	}
}

// Insert raw values directly to simulate corruption that domain constructors reject.
func insertRawTestJob(t *testing.T, ctx context.Context, id job.ID, kind, status string) {
	t.Helper()
	if _, err := repositoryTestPool(t).Exec(ctx, "INSERT INTO jobs (id, kind, status) VALUES ($1, $2, $3)", databaseID(id), kind, status); err != nil {
		t.Fatalf("insert raw test job: %v", err)
	}
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
			originalCtx, r := newTestJobRepository(t)
			ctx, cancel := tt.newContext()
			defer cancel()

			j := createTestJob(t, originalCtx, r, job.KindSendEmail, job.StatusDone)
			if err := r.CreateJob(ctx, j); !errors.Is(err, tt.wantErr) {
				t.Errorf("CreateJob error = %v, want %v", err, tt.wantErr)
			}
			if got, err := r.GetJob(ctx, j.ID()); got != nil || !errors.Is(err, tt.wantErr) {
				t.Errorf("GetJob() = %v, %v, want nil, %v", got, err, tt.wantErr)
			}
			if err := r.UpdateStatusByID(ctx, j.ID(), job.StatusDone); !errors.Is(err, tt.wantErr) {
				t.Errorf("UpdateStatusByID error = %v, want %v", err, tt.wantErr)
			}
			if got, err := r.ListJobs(ctx); got != nil || !errors.Is(err, tt.wantErr) {
				t.Errorf("ListJobs() = %v, %v, want nil, %v", got, err, tt.wantErr)
			}
			if err := r.DeleteJob(ctx, j.ID()); !errors.Is(err, tt.wantErr) {
				t.Errorf("DeleteJob() error = %v, want %v", err, tt.wantErr)
			}
			// Check that rejected writes left storage unchanged, using a live context.
			jobs, err := r.ListJobs(originalCtx)
			if err != nil || len(jobs) != 1 {
				t.Fatalf("repository after canceled writes = %v, %v, want one job", jobs, err)
			}
			requireSameJob(t, jobs[0], j)
		})
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
