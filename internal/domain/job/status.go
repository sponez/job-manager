package job

import "slices"

type Status string

const (
	StatusPending Status = "pending"
	StatusDone    Status = "done"
)

var allStatuses = []Status{
	StatusPending,
	StatusDone,
}

func (s *Status) Valid() bool {
	return slices.Contains(allStatuses, *s)
}
