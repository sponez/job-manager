package workflow

import (
	"context"
	"encoding/json"
	"testing"
	"time"
	"uuid"

	"github.com/sponez/job-manager/internal/application/snapshot"
	"github.com/sponez/job-manager/internal/application/worker"
)

type repositoryStub struct {
	workflow Workflow
	stepID   uuid.UUID
	started  bool
	restored bool
}

func (r *repositoryStub) Create(context.Context, Workflow) (Workflow, error) {
	panic("unexpected Create")
}
func (r *repositoryStub) Get(context.Context, uuid.UUID) (Workflow, error) { panic("unexpected Get") }

func (r *repositoryStub) ReserveSteps(_ context.Context, request StepReserve) ([]StepReservation, error) {
	if !r.started || len(request.StepStatuses) != 2 || request.StepStatuses[0] != StatusPending ||
		request.StepStatuses[1] != StatusRunning || len(request.WorkflowStates) != 2 ||
		request.WorkflowStates[0] != (StepWorkflowStatus{KindForward, StatusRunning}) ||
		request.WorkflowStates[1] != (StepWorkflowStatus{KindCompensation, StatusCompensating}) ||
		request.NewStatus != StatusRunning {
		panic("unexpected available step filter")
	}
	return []StepReservation{{Step: StepRecord{ID: r.stepID, Status: StatusRunning, Attempts: 1, Version: 1},
		PreviousStatus: StatusPending}}, nil
}
func (r *repositoryStub) RestoreSteps(_ context.Context, steps []StepReservation, status string) error {
	if len(steps) != 1 || steps[0].Step.ID != r.stepID || status != StatusRunning {
		panic("unexpected step restore")
	}
	r.restored = true
	return nil
}
func (r *repositoryStub) RefreshStepLease(context.Context, uuid.UUID, int64, string, time.Duration) (bool, error) {
	panic("unexpected RefreshStepLease")
}
func (r *repositoryStub) WithinTx(ctx context.Context, fn func(Transaction) error) error {
	return fn(transactionStub{repository: r})
}

type transactionStub struct{ repository *repositoryStub }

func (t transactionStub) LockWorkflowsByStatus(_ context.Context, status string, _ int) ([]Workflow, error) {
	if status != StatusPending {
		panic("unexpected workflow filter")
	}
	workflow := t.repository.workflow
	workflow.Status = StatusPending
	return []Workflow{workflow}, nil
}
func (t transactionStub) SetWorkflowStatuses(_ context.Context, ids []uuid.UUID, from, to string) (int64, error) {
	if len(ids) != 1 || ids[0] != t.repository.workflow.ID || from != StatusPending || to != StatusRunning {
		panic("unexpected workflow status batch")
	}
	return 1, nil
}
func (t transactionStub) InsertSteps(_ context.Context, steps []StepWrite) error {
	if len(steps) != 1 || steps[0].WorkflowID != t.repository.workflow.ID || steps[0].Seq != 1 ||
		steps[0].Type != "fetch_page" || steps[0].Kind != KindForward || steps[0].Status != StatusPending {
		panic("unexpected first step batch")
	}
	t.repository.started = true
	return nil
}

func (transactionStub) SetWorkflowStatus(context.Context, uuid.UUID, string, string) (bool, error) {
	panic("unexpected SetWorkflowStatus")
}
func (transactionStub) InsertStep(context.Context, StepWrite) error {
	panic("unexpected InsertStep")
}
func (transactionStub) UpdateStep(context.Context, StepUpdate) (bool, error) {
	panic("unexpected UpdateStep")
}
func (transactionStub) FindPreviousStep(context.Context, uuid.UUID, int64, string, string) (StepSummary, bool, error) {
	panic("unexpected FindPreviousStep")
}

type queueStub func(context.Context, worker.Task) error

func (q queueStub) Push(ctx context.Context, task worker.Task) error { return q(ctx, task) }

type fetcherStub func(context.Context, string) (snapshot.Snapshot, error)

func (f fetcherStub) Fetch(ctx context.Context, url string) (snapshot.Snapshot, error) {
	return f(ctx, url)
}

type storeStub struct{}

func (storeStub) Save(context.Context, uuid.UUID, snapshot.Snapshot) error { panic("unexpected Save") }
func (storeStub) Delete(context.Context, uuid.UUID) error                  { panic("unexpected Delete") }

func TestTickLeavesStepInStorageWhenPoolIsFull(t *testing.T) {
	payload, _ := json.Marshal(PageInput{URL: "https://example.com/page"})
	repo := &repositoryStub{workflow: Workflow{
		ID: uuid.New(), Type: PageWorkflowType, DefinitionVersion: 1, Payload: payload,
	}, stepID: uuid.New()}
	service, err := NewService(repo, nil, NewPageDefinition(
		fetcherStub(func(context.Context, string) (snapshot.Snapshot, error) { panic("unexpected Fetch") }),
		storeStub{},
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Tick(context.Background(), queueStub(func(context.Context, worker.Task) error {
		return worker.ErrQueueIsFull
	})); err != nil {
		t.Fatal(err)
	}
	if !repo.started || !repo.restored {
		t.Fatal("first step was not saved and restored after queue rejection")
	}
}
