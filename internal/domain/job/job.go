package job

import (
	"errors"
	"uuid"
)

type ID = uuid.UUID
type Name string
type Status string

const (
	StatusPending Status = "pending"
	StatusDone    Status = "done"

	NameSendEmail Name = "Send email"
	NameGetPage   Name = "Get page"
)

var (
	ErrNameIsNotValid   = errors.New("name is not valid")
	ErrStatusIsNotValid = errors.New("status is not valid")
)

type Job struct {
	id     ID
	name   Name
	status Status
}

// New validates the job before constructing it.
func New(id ID, name Name, status Status) (*Job, error) {
	var errs []error

	switch name {
	case NameSendEmail, NameGetPage:
	default:
		errs = append(errs, ErrNameIsNotValid)
	}

	switch status {
	case StatusPending, StatusDone:
	default:
		errs = append(errs, ErrStatusIsNotValid)
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &Job{id: id, name: name, status: status}, nil
}

func (j *Job) ID() ID {
	return j.id
}

func (j *Job) Name() Name {
	return j.name
}

func (j *Job) Status() Status {
	return j.status
}

// UpdateStatus returns a validated copy and leaves the original job unchanged.
func (j *Job) UpdateStatus(s Status) (*Job, error) {
	return New(j.id, j.name, s)
}
