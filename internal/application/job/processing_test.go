package job

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
	"uuid"

	"github.com/sponez/job-manager/internal/domain/job"
)

func TestExecJobPersistsOutcome(t *testing.T) {
	for _, tt := range []struct {
		name       string
		execErr    error
		cancel     bool
		wantStatus job.Status
	}{
		{"success", nil, false, job.StatusDone},
		{"failure", errors.New("execution failed"), false, job.StatusError},
		{"cancellation", context.Canceled, true, job.StatusError},
		{"deadline", context.DeadlineExceeded, true, job.StatusError},
		{"completed before cancellation", nil, true, job.StatusDone},
	} {
		t.Run(tt.name, func(t *testing.T) {
			type contextKey struct{}
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "value"))
			defer cancel()
			id := uuid.New()
			var resultCtx context.Context
			writes := 0
			service := New(&jobRepositoryStub{t: t,
				updateStatusByID: func(ctx context.Context, gotID job.ID, status job.Status) error {
					writes++
					resultCtx = ctx
					if ctx.Err() != nil || ctx.Value(contextKey{}) != "value" {
						t.Errorf("invalid result context: %v", ctx)
					}
					if gotID != id || status != tt.wantStatus {
						t.Errorf("saved %v, %v; want %v, %v", gotID, status, id, tt.wantStatus)
					}
					deadline, ok := ctx.Deadline()
					if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 5*time.Second {
						t.Error("result context must have a bounded deadline")
					}
					return nil
				},
			})
			service.execJob(ctx, id, func(gotCtx context.Context) error {
				if gotCtx != ctx {
					t.Error("executor did not receive the task context")
				}
				if tt.cancel {
					cancel()
				}
				return tt.execErr
			})
			if writes != 1 {
				t.Fatalf("outcome writes = %d, want 1", writes)
			}
			if !errors.Is(resultCtx.Err(), context.Canceled) {
				t.Error("result context was not released")
			}
		})
	}
}

func TestProcessJob(t *testing.T) {
	for _, kind := range []job.Kind{job.KindGetPage, job.KindSendEmail} {
		for _, canceled := range []bool{false, true} {
			name := string(kind) + "/completed"
			if canceled {
				name = string(kind) + "/canceled"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					id := uuid.New()
					j, err := job.New(id, kind, job.StatusPending)
					if err != nil {
						t.Fatal(err)
					}
					statuses := make(chan job.Status, 2)
					service := New(&jobRepositoryStub{t: t,
						getJob: func(gotCtx context.Context, gotID job.ID) (*job.Job, error) {
							if gotCtx != ctx || gotID != id {
								t.Error("GetJob received wrong context or ID")
							}
							return j, nil
						},
						updateStatusByID: func(ctx context.Context, gotID job.ID, status job.Status) error {
							if ctx.Err() != nil || gotID != id {
								t.Error("status update received canceled context or wrong ID")
							}
							statuses <- status
							return ctx.Err()
						},
					})
					done := make(chan struct{})
					start := time.Now()
					go func() {
						service.ProcessJob(ctx, id)
						close(done)
					}()
					synctest.Wait()
					if status := <-statuses; status != job.StatusInProgress {
						t.Fatalf("initial status = %v, want in progress", status)
					}
					if canceled {
						cancel()
					}
					<-done
					if len(statuses) != 1 {
						t.Fatalf("outcome writes = %d, want 1", len(statuses))
					}
					outcome := <-statuses
					if canceled {
						if outcome != job.StatusError || time.Since(start) != 0 {
							t.Errorf("cancellation did not promptly persist error: %v, %v", outcome, time.Since(start))
						}
					} else {
						if outcome != job.StatusDone && outcome != job.StatusError {
							t.Errorf("nonterminal outcome: %v", outcome)
						}
						wantDuration := 2 * time.Second
						if kind == job.KindSendEmail {
							wantDuration = 15 * time.Second
						}
						if time.Since(start) != wantDuration {
							t.Errorf("execution duration = %v, want %v", time.Since(start), wantDuration)
						}
					}
				})
			})
		}
	}
}

func TestProcessJobRepositoryFailures(t *testing.T) {
	for _, readFails := range []bool{true, false} {
		id := uuid.New()
		j, err := job.New(id, job.KindSendEmail, job.StatusPending)
		if err != nil {
			t.Fatal(err)
		}
		writes := 0
		storageErr := errors.New("storage unavailable")
		service := New(&jobRepositoryStub{t: t,
			getJob: func(context.Context, job.ID) (*job.Job, error) {
				if readFails {
					return nil, storageErr
				}
				return j, nil
			},
			updateStatusByID: func(_ context.Context, _ job.ID, status job.Status) error {
				writes++
				if status != job.StatusInProgress {
					t.Errorf("unexpected outcome write after repository failure: %v", status)
				}
				return storageErr
			},
		})
		service.ProcessJob(context.Background(), id)
		wantWrites := 1
		if readFails {
			wantWrites = 0
		}
		if writes != wantWrites {
			t.Errorf("writes = %d, want %d", writes, wantWrites)
		}
	}
}

func TestDeleteJob(t *testing.T) {
	storageErr := errors.New("delete failed")
	for _, repoErr := range []error{nil, storageErr, context.Canceled} {
		ctx := context.Background()
		id := uuid.New()
		calls := 0
		service := New(&jobRepositoryStub{t: t, deleteJob: func(gotCtx context.Context, gotID job.ID) error {
			calls++
			if gotCtx != ctx || gotID != id {
				t.Error("DeleteJob received wrong context or ID")
			}
			return repoErr
		}})
		if err := service.DeleteJob(ctx, id); !errors.Is(err, repoErr) || calls != 1 {
			t.Errorf("DeleteJob = %v, calls = %d; want %v, 1", err, calls, repoErr)
		}
	}
}
