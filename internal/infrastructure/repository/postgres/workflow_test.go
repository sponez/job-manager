package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sponez/job-manager/internal/application/snapshot"
	"github.com/sponez/job-manager/internal/application/worker"
	app "github.com/sponez/job-manager/internal/application/workflow"
	"github.com/sponez/job-manager/internal/infrastructure/snapshothttp"
)

type inlineQueue struct{}

func (inlineQueue) Push(ctx context.Context, task worker.Task) error {
	task(ctx)
	return nil
}

type fullQueue struct{}

func (fullQueue) Push(context.Context, worker.Task) error { return worker.ErrQueueIsFull }

func cleanupWorkflow(t *testing.T, repo *WorkflowRepository, id uuid.UUID) {
	t.Helper()
	t.Cleanup(func() {
		_, _ = repo.db.Exec(context.Background(), `DELETE FROM workflows WHERE id = $1`, workflowDBID(id))
	})
}

func TestPageWorkflowFromPendingToSnapshot(t *testing.T) {
	db := repositoryTestPool(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, "<p>saved</p>")
	}))
	defer server.Close()
	repo := NewWorkflowRepository(db)
	service, err := app.NewService(repo, nil,
		app.NewPageDefinition(snapshothttp.New(nil, nil), NewSnapshotRepository(db)))
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(app.PageInput{URL: server.URL})
	wf, err := service.Create(context.Background(), app.PageWorkflowType, payload, "")
	if err != nil {
		t.Fatal(err)
	}
	cleanupWorkflow(t, repo, wf.ID)
	if err := service.Tick(context.Background(), inlineQueue{}); err != nil {
		t.Fatal(err)
	}
	stored, err := service.Get(context.Background(), wf.ID)
	if err != nil || stored.Status != app.StatusCompleted {
		t.Fatalf("workflow = %+v, %v", stored, err)
	}
	var body []byte
	if err := db.QueryRow(context.Background(), `SELECT body FROM snapshots WHERE workflow_id = $1`,
		workflowDBID(wf.ID)).Scan(&body); err != nil || string(body) != "<p>saved</p>" {
		t.Fatalf("snapshot body = %q, error = %v", body, err)
	}
}

func TestWorkflowTransactionRollsBackAndCreationIsIdempotent(t *testing.T) {
	repo := NewWorkflowRepository(repositoryTestPool(t))
	ctx := context.Background()
	payload, _ := json.Marshal(app.PageInput{URL: "https://example.com"})
	wf, err := repo.Create(ctx, app.Workflow{
		ID: uuid.New(), Type: app.PageWorkflowType, DefinitionVersion: 1,
		Status: app.StatusPending, Payload: payload, IdempotencyKey: uuid.New().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupWorkflow(t, repo, wf.ID)
	duplicate, err := repo.Create(ctx, app.Workflow{
		ID: uuid.New(), Type: wf.Type, DefinitionVersion: 1,
		Status: app.StatusPending, Payload: payload, IdempotencyKey: wf.IdempotencyKey,
	})
	if err != nil || duplicate.ID != wf.ID {
		t.Fatalf("idempotent create = %+v, %v", duplicate, err)
	}
	err = repo.WithinTx(ctx, func(tx app.Transaction) error {
		changed, err := tx.SetWorkflowStatus(ctx, wf.ID, app.StatusPending, app.StatusRunning)
		if err != nil || !changed {
			t.Fatalf("status update = %v, %v", changed, err)
		}
		return tx.InsertStep(ctx, app.StepWrite{
			ID: uuid.New(), WorkflowID: wf.ID, Seq: 1, Type: "fetch_page",
			Kind: app.KindForward, Status: app.StatusPending, MaxAttempts: 3,
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	err = repo.WithinTx(ctx, func(tx app.Transaction) error {
		_, err := tx.SetWorkflowStatus(ctx, wf.ID, app.StatusRunning, app.StatusCompleted)
		if err != nil {
			return err
		}
		// Duplicate sequence rejects the second write and rolls back the status.
		return tx.InsertStep(ctx, app.StepWrite{
			ID: uuid.New(), WorkflowID: wf.ID, Seq: 1, Type: "save_snapshot",
			Kind: app.KindForward, Status: app.StatusPending, MaxAttempts: 3,
		})
	})
	if err == nil {
		t.Fatal("duplicate step should fail")
	}
	stored, err := repo.Get(ctx, wf.ID)
	if err != nil || stored.Status != app.StatusRunning {
		t.Fatalf("transaction was not rolled back: %+v, %v", stored, err)
	}
}

func TestWorkflowStepVersionRejectsStaleUpdate(t *testing.T) {
	repo := NewWorkflowRepository(repositoryTestPool(t))
	ctx := context.Background()
	wf, err := repo.Create(ctx, app.Workflow{
		ID: uuid.New(), Type: app.PageWorkflowType, DefinitionVersion: 1, Status: app.StatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupWorkflow(t, repo, wf.ID)
	id := uuid.New()
	err = repo.WithinTx(ctx, func(tx app.Transaction) error {
		if _, err := tx.SetWorkflowStatus(ctx, wf.ID, app.StatusPending, app.StatusRunning); err != nil {
			return err
		}
		return tx.InsertStep(ctx, app.StepWrite{
			ID: id, WorkflowID: wf.ID, Seq: 1, Type: "fetch_page",
			Kind: app.KindForward, Status: app.StatusPending, MaxAttempts: 3,
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	stale := incrementStepAttemptForTest(t, repo, id, 0)
	current := incrementStepAttemptForTest(t, repo, id, time.Minute)
	if current.Version <= stale.Version {
		t.Fatalf("second attempt = %+v; first = %+v", current, stale)
	}
	valid, err := repo.RefreshStepLease(ctx, id, stale.Version, app.StatusRunning, time.Minute)
	if err != nil || valid {
		t.Fatalf("stale lease refresh = %v, %v", valid, err)
	}
	valid, err = repo.RefreshStepLease(ctx, id, current.Version, app.StatusRunning, time.Minute)
	if err != nil || !valid {
		t.Fatalf("current lease refresh = %v, %v", valid, err)
	}
	var accepted bool
	err = repo.WithinTx(ctx, func(tx app.Transaction) error {
		var err error
		accepted, err = tx.UpdateStep(ctx, app.StepUpdate{
			ID: stale.ID, Version: stale.Version, ExpectedStatus: app.StatusRunning,
			Status: app.StatusCompleted,
		})
		return err
	})
	if err != nil || accepted {
		t.Fatalf("stale update = %v, %v", accepted, err)
	}
	err = repo.WithinTx(ctx, func(tx app.Transaction) error {
		var err error
		accepted, err = tx.UpdateStep(ctx, app.StepUpdate{
			ID: current.ID, Version: current.Version, ExpectedStatus: app.StatusRunning,
			Status: app.StatusCompleted,
		})
		return err
	})
	if err != nil || !accepted {
		t.Fatalf("current update = %v, %v", accepted, err)
	}
}

func incrementStepAttemptForTest(t *testing.T, repo *WorkflowRepository, id uuid.UUID, lease time.Duration) app.StepRecord {
	t.Helper()
	request := testStepReserve(id, lease)
	reserved, err := repo.ReserveSteps(context.Background(), request)
	if err != nil || len(reserved) != 1 {
		t.Fatalf("reserve step = %+v, %v", reserved, err)
	}
	return reserved[0].Step
}

func testStepReserve(id uuid.UUID, lease time.Duration) app.StepReserve {
	return app.StepReserve{
		StepStatuses: []string{app.StatusPending, app.StatusRunning},
		WorkflowStates: []app.StepWorkflowStatus{
			{Kind: app.KindForward, WorkflowStatus: app.StatusRunning},
			{Kind: app.KindCompensation, WorkflowStatus: app.StatusCompensating},
		},
		OnlyIDs: []uuid.UUID{id}, Limit: 1, NewStatus: app.StatusRunning, Lease: lease,
	}
}

func TestWorkflowReservationSkipsLockedRow(t *testing.T) {
	repo := NewWorkflowRepository(repositoryTestPool(t))
	ctx := context.Background()
	wf, err := repo.Create(ctx, app.Workflow{
		ID: uuid.New(), Type: app.PageWorkflowType, DefinitionVersion: 1, Status: app.StatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupWorkflow(t, repo, wf.ID)
	id := uuid.New()
	err = repo.WithinTx(ctx, func(tx app.Transaction) error {
		if _, err := tx.SetWorkflowStatus(ctx, wf.ID, app.StatusPending, app.StatusRunning); err != nil {
			return err
		}
		return tx.InsertStep(ctx, app.StepWrite{
			ID: id, WorkflowID: wf.ID, Seq: 1, Type: "fetch_page",
			Kind: app.KindForward, Status: app.StatusPending, MaxAttempts: 3,
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	locker, err := repo.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(ctx)
	var lockedID pgtype.UUID
	if err := locker.QueryRow(ctx, `SELECT id FROM workflow_steps WHERE id = $1 FOR UPDATE`,
		workflowDBID(id)).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}
	claimCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	reserved, err := repo.ReserveSteps(claimCtx, testStepReserve(id, time.Minute))
	if err != nil || len(reserved) != 0 {
		t.Fatalf("reserve locked step = %+v, %v", reserved, err)
	}
	if err := locker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	reserved, err = repo.ReserveSteps(ctx, testStepReserve(id, time.Minute))
	if err != nil || len(reserved) != 1 {
		t.Fatalf("reserve after unlock = %+v, %v", reserved, err)
	}
}

func TestWorkflowBatchReservationSkipsLockedStep(t *testing.T) {
	repo := NewWorkflowRepository(repositoryTestPool(t))
	ctx := context.Background()
	ids := make([]uuid.UUID, 0, 3)
	for range 3 {
		wf, err := repo.Create(ctx, app.Workflow{
			ID: uuid.New(), Type: app.PageWorkflowType, DefinitionVersion: 1, Status: app.StatusPending,
		})
		if err != nil {
			t.Fatal(err)
		}
		cleanupWorkflow(t, repo, wf.ID)
		id := uuid.New()
		err = repo.WithinTx(ctx, func(tx app.Transaction) error {
			if _, err := tx.SetWorkflowStatus(ctx, wf.ID, app.StatusPending, app.StatusRunning); err != nil {
				return err
			}
			return tx.InsertStep(ctx, app.StepWrite{
				ID: id, WorkflowID: wf.ID, Seq: 1, Type: "fetch_page",
				Kind: app.KindForward, Status: app.StatusPending, MaxAttempts: 3,
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	locker, err := repo.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(ctx)
	var lockedID pgtype.UUID
	if err := locker.QueryRow(ctx, `SELECT id FROM workflow_steps WHERE id = $1 FOR UPDATE`,
		workflowDBID(ids[0])).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}
	request := testStepReserve(uuid.Nil(), time.Minute)
	request.OnlyIDs = nil
	request.Limit = 3
	reserveCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	reserved, err := repo.ReserveSteps(reserveCtx, request)
	if err != nil || len(reserved) != 2 {
		t.Fatalf("batch reservation = %+v, %v", reserved, err)
	}
	if err := locker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	reserved, err = repo.ReserveSteps(ctx, request)
	if err != nil || len(reserved) != 1 || reserved[0].Step.ID != ids[0] {
		t.Fatalf("reservation after unlock = %+v, %v", reserved, err)
	}
}

func TestWorkflowProcessDoesNotClaimStepBeforeWorkflowStarts(t *testing.T) {
	repo := NewWorkflowRepository(repositoryTestPool(t))
	ctx := context.Background()
	wf, err := repo.Create(ctx, app.Workflow{
		ID: uuid.New(), Type: app.PageWorkflowType, DefinitionVersion: 1, Status: app.StatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupWorkflow(t, repo, wf.ID)
	id := uuid.New()
	err = repo.WithinTx(ctx, func(tx app.Transaction) error {
		return tx.InsertStep(ctx, app.StepWrite{
			ID: id, WorkflowID: wf.ID, Seq: 1, Type: "fetch_page",
			Kind: app.KindForward, Status: app.StatusPending, MaxAttempts: 3,
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := app.NewService(repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	service.Process(ctx, id)
	var status string
	var attempts int
	if err := repo.db.QueryRow(ctx, `SELECT status, attempts FROM workflow_steps WHERE id = $1`,
		workflowDBID(id)).Scan(&status, &attempts); err != nil || status != app.StatusPending || attempts != 0 {
		t.Fatalf("unstarted step = %s, attempts %d, error %v", status, attempts, err)
	}
}

func TestWorkflowStartSkipsLockedWorkflow(t *testing.T) {
	repo := NewWorkflowRepository(repositoryTestPool(t))
	definition := app.Definition{
		Type: "locked_start", Version: 1,
		First: func(json.RawMessage) (app.NewStep, error) {
			return app.NewStep{Type: "noop"}, nil
		},
		Steps: map[string]app.Step{"noop": alwaysFailStep{}},
	}
	service, err := app.NewService(repo, nil, definition)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	wf, err := service.Create(ctx, definition.Type, json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatal(err)
	}
	cleanupWorkflow(t, repo, wf.ID)

	locker, err := repo.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(ctx)
	var lockedID pgtype.UUID
	if err := locker.QueryRow(ctx, `SELECT id FROM workflows WHERE id = $1 FOR UPDATE`,
		workflowDBID(wf.ID)).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}
	tickCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := service.Tick(tickCtx, fullQueue{}); err != nil {
		t.Fatalf("tick with locked workflow: %v", err)
	}
	stored, err := repo.Get(ctx, wf.ID)
	if err != nil || stored.Status != app.StatusPending {
		t.Fatalf("locked workflow = %+v, %v", stored, err)
	}
	if err := locker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	otherService, err := app.NewService(NewWorkflowRepository(repo.db), nil, definition)
	if err != nil {
		t.Fatal(err)
	}
	if err := otherService.Tick(ctx, fullQueue{}); err != nil {
		t.Fatal(err)
	}
	if err := service.Tick(ctx, fullQueue{}); err != nil {
		t.Fatal(err)
	}
	stored, err = repo.Get(ctx, wf.ID)
	if err != nil || stored.Status != app.StatusRunning {
		t.Fatalf("started workflow = %+v, %v", stored, err)
	}
	var steps int
	if err := repo.db.QueryRow(ctx, `SELECT count(*) FROM workflow_steps WHERE workflow_id = $1`,
		workflowDBID(wf.ID)).Scan(&steps); err != nil || steps != 1 {
		t.Fatalf("first steps = %d, %v", steps, err)
	}
	var stepStatus string
	var attempts int
	if err := repo.db.QueryRow(ctx, `SELECT status, attempts FROM workflow_steps WHERE workflow_id = $1`,
		workflowDBID(wf.ID)).Scan(&stepStatus, &attempts); err != nil || stepStatus != app.StatusPending || attempts != 0 {
		t.Fatalf("unqueued step = %s, attempts %d, error %v", stepStatus, attempts, err)
	}
}

func TestWorkflowBatchStartSkipsLockedWorkflow(t *testing.T) {
	repo := NewWorkflowRepository(repositoryTestPool(t))
	definition := app.Definition{
		Type: "batch_start", Version: 1,
		First: func(json.RawMessage) (app.NewStep, error) {
			return app.NewStep{Type: "noop"}, nil
		},
		Steps: map[string]app.Step{"noop": alwaysFailStep{}},
	}
	service, err := app.NewService(repo, nil, definition)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var workflows []app.Workflow
	for range 3 {
		wf, err := service.Create(ctx, definition.Type, json.RawMessage(`{}`), "")
		if err != nil {
			t.Fatal(err)
		}
		cleanupWorkflow(t, repo, wf.ID)
		workflows = append(workflows, wf)
	}
	locker, err := repo.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(ctx)
	var lockedID pgtype.UUID
	if err := locker.QueryRow(ctx, `SELECT id FROM workflows WHERE id = $1 FOR UPDATE`,
		workflowDBID(workflows[0].ID)).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}
	tickCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := service.Tick(tickCtx, fullQueue{}); err != nil {
		t.Fatal(err)
	}
	for i, wf := range workflows {
		stored, err := repo.Get(ctx, wf.ID)
		want := app.StatusRunning
		if i == 0 {
			want = app.StatusPending
		}
		if err != nil || stored.Status != want {
			t.Fatalf("workflow %d = %+v, %v; want %s", i, stored, err, want)
		}
	}
	if err := locker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.Tick(ctx, fullQueue{}); err != nil {
		t.Fatal(err)
	}
	for i, wf := range workflows {
		var count int
		if err := repo.db.QueryRow(ctx, `SELECT count(*) FROM workflow_steps WHERE workflow_id = $1`,
			workflowDBID(wf.ID)).Scan(&count); err != nil || count != 1 {
			t.Fatalf("workflow %d first steps = %d, %v", i, count, err)
		}
	}
}

type fixedFetcher struct{ value snapshot.Snapshot }

func (f fixedFetcher) Fetch(context.Context, string) (snapshot.Snapshot, error) { return f.value, nil }

type failingStore struct{ deletes *int }

func (f failingStore) Save(context.Context, uuid.UUID, snapshot.Snapshot) error {
	return errors.New("save unavailable")
}
func (f failingStore) Delete(context.Context, uuid.UUID) error {
	*f.deletes++
	return nil
}

func TestPageWorkflowRetriesThenCompensatesInReverse(t *testing.T) {
	repo := NewWorkflowRepository(repositoryTestPool(t))
	ctx := context.Background()
	deletes := 0
	service, err := app.NewService(repo, nil, app.NewPageDefinition(
		fixedFetcher{value: snapshot.Snapshot{SourceURL: "https://example.com", Body: []byte("page")}},
		failingStore{deletes: &deletes},
	))
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(app.PageInput{URL: "https://example.com"})
	wf, err := service.Create(ctx, app.PageWorkflowType, payload, "")
	if err != nil {
		t.Fatal(err)
	}
	cleanupWorkflow(t, repo, wf.ID)
	for attempt := 1; attempt <= 3; attempt++ {
		if attempt > 1 {
			_, err := repo.db.Exec(ctx, `UPDATE workflow_steps SET available_at = now()
				WHERE workflow_id = $1 AND status = 'pending'`, workflowDBID(wf.ID))
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := service.Tick(ctx, inlineQueue{}); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := repo.Get(ctx, wf.ID)
	if err != nil || stored.Status != app.StatusCompensating {
		t.Fatalf("after retries = %+v, %v", stored, err)
	}
	if err := service.Tick(ctx, inlineQueue{}); err != nil {
		t.Fatal(err)
	}
	stored, err = repo.Get(ctx, wf.ID)
	if err != nil || stored.Status != app.StatusCompensated || deletes != 1 {
		t.Fatalf("compensation = %+v, deletes %d, error %v", stored, deletes, err)
	}
}

type alwaysFailStep struct{}

func (alwaysFailStep) Execute(context.Context, uuid.UUID, json.RawMessage) (json.RawMessage, error) {
	return nil, errors.New("planned failure")
}
func (alwaysFailStep) Compensate(context.Context, uuid.UUID, json.RawMessage, json.RawMessage) error {
	return nil
}
func (alwaysFailStep) Next(json.RawMessage) (*app.NewStep, error) { return nil, nil }

type countedStep struct{ executions *int }

func (s countedStep) Execute(context.Context, uuid.UUID, json.RawMessage) (json.RawMessage, error) {
	*s.executions++
	return nil, nil
}
func (countedStep) Compensate(context.Context, uuid.UUID, json.RawMessage, json.RawMessage) error {
	return nil
}
func (countedStep) Next(json.RawMessage) (*app.NewStep, error) { return nil, nil }

func TestWorkflowAbandonedFinalAttemptDoesNotExecuteAgain(t *testing.T) {
	repo := NewWorkflowRepository(repositoryTestPool(t))
	ctx := context.Background()
	executions := 0
	service, err := app.NewService(repo, nil, app.Definition{
		Type: "abandoned_final_attempt", Version: 1,
		First: func(json.RawMessage) (app.NewStep, error) {
			return app.NewStep{Type: "count"}, nil
		},
		Steps: map[string]app.Step{"count": countedStep{executions: &executions}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wf, err := service.Create(ctx, "abandoned_final_attempt", json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatal(err)
	}
	cleanupWorkflow(t, repo, wf.ID)
	if err := service.Tick(ctx, fullQueue{}); err != nil {
		t.Fatal(err)
	}
	var stepID pgtype.UUID
	if err := repo.db.QueryRow(ctx, `SELECT id FROM workflow_steps WHERE workflow_id = $1`,
		workflowDBID(wf.ID)).Scan(&stepID); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		reserved, err := repo.ReserveSteps(ctx, testStepReserve(workflowID(stepID), 0))
		if err != nil || len(reserved) != 1 || reserved[0].Step.Attempts != attempt {
			t.Fatalf("abandoned attempt %d = %+v, %v", attempt, reserved, err)
		}
	}
	service.Process(ctx, workflowID(stepID))
	stored, err := repo.Get(ctx, wf.ID)
	if err != nil || stored.Status != app.StatusFailed || executions != 0 {
		t.Fatalf("recovered workflow = %+v, executions %d, error %v", stored, executions, err)
	}
}

func TestWorkflowPodSkipsUnknownDefinitionVersion(t *testing.T) {
	repo := NewWorkflowRepository(repositoryTestPool(t))
	ctx := context.Background()
	executions := 0
	definition := func(version int) app.Definition {
		return app.Definition{
			Type: "rolling_version", Version: version,
			First: func(json.RawMessage) (app.NewStep, error) {
				return app.NewStep{Type: "count"}, nil
			},
			Steps: map[string]app.Step{"count": countedStep{executions: &executions}},
		}
	}
	known, err := app.NewService(repo, nil, definition(1))
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := app.NewService(repo, nil, definition(2))
	if err != nil {
		t.Fatal(err)
	}
	wf, err := known.Create(ctx, "rolling_version", json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatal(err)
	}
	cleanupWorkflow(t, repo, wf.ID)
	if err := known.Tick(ctx, fullQueue{}); err != nil {
		t.Fatal(err)
	}
	if err := unknown.Tick(ctx, inlineQueue{}); err != nil {
		t.Fatal(err)
	}
	var attempts int
	if err := repo.db.QueryRow(ctx, `SELECT attempts FROM workflow_steps WHERE workflow_id = $1`,
		workflowDBID(wf.ID)).Scan(&attempts); err != nil || attempts != 0 || executions != 0 {
		t.Fatalf("unknown pod used attempt: attempts %d, executions %d, error %v", attempts, executions, err)
	}
	if err := known.Tick(ctx, inlineQueue{}); err != nil {
		t.Fatal(err)
	}
	stored, err := repo.Get(ctx, wf.ID)
	if err != nil || stored.Status != app.StatusCompleted || executions != 1 {
		t.Fatalf("known pod result = %+v, executions %d, error %v", stored, executions, err)
	}
}

func TestWorkflowWithoutCompensationFailsAfterRetries(t *testing.T) {
	repo := NewWorkflowRepository(repositoryTestPool(t))
	ctx := context.Background()
	service, err := app.NewService(repo, nil, app.Definition{
		Type: "fail_without_compensation", Version: 1,
		First: func(json.RawMessage) (app.NewStep, error) {
			return app.NewStep{Type: "fail"}, nil
		},
		Steps: map[string]app.Step{"fail": alwaysFailStep{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wf, err := service.Create(ctx, "fail_without_compensation", json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatal(err)
	}
	cleanupWorkflow(t, repo, wf.ID)
	for attempt := 1; attempt <= 3; attempt++ {
		if attempt > 1 {
			_, err := repo.db.Exec(ctx, `UPDATE workflow_steps SET available_at = now()
				WHERE workflow_id = $1 AND status = 'pending'`, workflowDBID(wf.ID))
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := service.Tick(ctx, inlineQueue{}); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := repo.Get(ctx, wf.ID)
	if err != nil || stored.Status != app.StatusFailed {
		t.Fatalf("workflow status = %+v, %v", stored, err)
	}
	var compensations int
	if err := repo.db.QueryRow(ctx, `SELECT count(*) FROM workflow_steps
		WHERE workflow_id = $1 AND kind = 'compensation'`, workflowDBID(wf.ID)).Scan(&compensations); err != nil || compensations != 0 {
		t.Fatalf("compensations = %d, error = %v", compensations, err)
	}
}
