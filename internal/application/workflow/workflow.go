package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"time"
	"uuid"
)

const (
	StatusPending            = "pending"
	StatusRunning            = "running"
	StatusCompensating       = "compensating"
	StatusCompleted          = "completed"
	StatusCompensated        = "compensated"
	StatusFailed             = "failed"
	StatusCompensationFailed = "compensation_failed"

	KindForward      = "forward"
	KindCompensation = "compensation"

	stepTimeout = 30 * time.Second
	stepLease   = time.Minute
	maxAttempts = 3
)

var (
	ErrNotFound          = errors.New("workflow not found")
	ErrUnknownType       = errors.New("unknown workflow type")
	ErrInvalidPayload    = errors.New("invalid workflow payload")
	errAttemptsExhausted = errors.New("step lease expired after the final attempt")
)

type Workflow struct {
	ID                uuid.UUID
	Type              string
	DefinitionVersion int
	Status            string
	Payload           json.RawMessage
	IdempotencyKey    string
}

type NewStep struct {
	Type    string
	Payload json.RawMessage
}

type StepRecord struct {
	ID                uuid.UUID
	WorkflowID        uuid.UUID
	WorkflowType      string
	DefinitionVersion int
	Type              string
	Kind              string
	Status            string
	Payload           json.RawMessage
	Result            json.RawMessage
	TargetPayload     json.RawMessage
	TargetResult      json.RawMessage
	Seq               int64
	TargetSeq         int64
	Attempts          int
	MaxAttempts       int
	Version           int64
}

// Step describes behavior; the application owns every state transition.
// Execute and Compensate must tolerate repetition after a worker loses its lease.
type Step interface {
	Execute(ctx context.Context, workflowID uuid.UUID, payload json.RawMessage) (json.RawMessage, error)
	Compensate(ctx context.Context, workflowID uuid.UUID, payload, result json.RawMessage) error
	Next(result json.RawMessage) (*NewStep, error)
}

// Keep each registered version available while workflows created with it exist.
type Definition struct {
	Type                string
	Version             int
	CompensateOnFailure bool
	First               func(json.RawMessage) (NewStep, error)
	Steps               map[string]Step
}

type Repository interface {
	Create(context.Context, Workflow) (Workflow, error)
	Get(context.Context, uuid.UUID) (Workflow, error)
	ReserveSteps(context.Context, StepReserve) ([]StepReservation, error)
	RestoreSteps(context.Context, []StepReservation, string) error
	RefreshStepLease(context.Context, uuid.UUID, int64, string, time.Duration) (bool, error)
	WithinTx(context.Context, func(Transaction) error) error
}

type StepWorkflowStatus struct {
	Kind           string
	WorkflowStatus string
}

type DefinitionRef struct {
	Type    string
	Version int
}

type StepReserve struct {
	StepStatuses   []string
	WorkflowStates []StepWorkflowStatus
	Definitions    []DefinitionRef
	OnlyIDs        []uuid.UUID
	ExcludeIDs     []uuid.UUID
	Limit          int
	NewStatus      string
	Lease          time.Duration
}

type StepReservation struct {
	Step           StepRecord
	PreviousStatus string
}

type StepWrite struct {
	ID                uuid.UUID
	WorkflowID        uuid.UUID
	Seq               int64
	Type              string
	Kind              string
	Status            string
	CompensatesStepID uuid.UUID
	Payload           json.RawMessage
	MaxAttempts       int
}

type StepUpdate struct {
	ID             uuid.UUID
	Version        int64
	ExpectedStatus string
	Status         string
	Result         json.RawMessage
	LastError      json.RawMessage
	AvailableAfter time.Duration
}

type StepSummary struct {
	ID   uuid.UUID
	Type string
	Seq  int64
}

// Transaction exposes storage operations; the application chooses their order
// and the statuses, with all writes committed or rolled back together.
type Transaction interface {
	LockWorkflowsByStatus(context.Context, string, int) ([]Workflow, error)
	SetWorkflowStatuses(context.Context, []uuid.UUID, string, string) (int64, error)
	InsertSteps(context.Context, []StepWrite) error
	SetWorkflowStatus(context.Context, uuid.UUID, string, string) (bool, error)
	InsertStep(context.Context, StepWrite) error
	UpdateStep(context.Context, StepUpdate) (bool, error)
	FindPreviousStep(context.Context, uuid.UUID, int64, string, string) (StepSummary, bool, error)
}
