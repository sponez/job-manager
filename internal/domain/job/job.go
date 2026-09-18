package job

import (
	"errors"
	"uuid"
)

type Id = uuid.UUID
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
	id     Id
	name   Name
	status Status
}

func NewJob(id Id, name Name, status Status) *Job {
	return &Job{id, name, status}
}

func TryNewJob(id uuid.UUID, name string, status string) (*Job, error) {
	var jName Name
	var jStatus Status
	var errs []error

	switch {
	case name == string(NameSendEmail):
		jName = NameSendEmail
	case name == string(NameGetPage):
		jName = NameGetPage
	default:
		errs = append(errs, ErrNameIsNotValid)
	}

	switch {
	case status == string(StatusPending):
		jStatus = StatusPending
	case status == string(StatusDone):
		jStatus = StatusDone
	default:
		errs = append(errs, ErrStatusIsNotValid)
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &Job{id, jName, jStatus}, nil
}

func (j *Job) Id() Id {
	return j.id
}

func (j *Job) Name() Name {
	return j.name
}

func (j *Job) Status() Status {
	return j.status
}

func (j *Job) UpdateStatus(s Status) *Job {
	return &Job{j.id, j.name, s}
}
