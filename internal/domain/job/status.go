package job

import "slices"

type Status string

const (
	StatusPending    Status = "pending"
	StatusInProgress Status = "in progress"
	StatusDone       Status = "done"
	StatusError      Status = "error"
)

var allStatuses = []Status{
	StatusPending,
	StatusInProgress,
	StatusDone,
	StatusError,
}

func (s *Status) Valid() bool {
	return slices.Contains(allStatuses, *s)
}
