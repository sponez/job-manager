package job

type Status string

const (
	StatusPending Status = "pending"
	StatusDone    Status = "done"
)

type Job struct {
	ID     int
	Name   string
	Status Status
}
