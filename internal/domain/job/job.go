package job

import (
	"errors"
	"uuid"
)

type ID = uuid.UUID
type Kind string
type Status string

const (
	StatusPending Status = "pending"
	StatusDone    Status = "done"

	KindSendEmail Kind = "Send email"
	KindGetPage   Kind = "Get page"
)

var (
	ErrKindIsNotValid   = errors.New("kind is not valid")
	ErrStatusIsNotValid = errors.New("status is not valid")
)

type Job struct {
	id     ID
	kind   Kind
	status Status
}

// New validates the job before constructing it.
func New(id ID, kind Kind, status Status) (*Job, error) {
	var errs []error

	switch kind {
	case KindSendEmail, KindGetPage:
	default:
		errs = append(errs, ErrKindIsNotValid)
	}

	switch status {
	case StatusPending, StatusDone:
	default:
		errs = append(errs, ErrStatusIsNotValid)
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &Job{id: id, kind: kind, status: status}, nil
}

func (j *Job) ID() ID {
	return j.id
}

func (j *Job) Kind() Kind {
	return j.kind
}

func (j *Job) Status() Status {
	return j.status
}
