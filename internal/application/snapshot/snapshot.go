package snapshot

import (
	"context"
	"uuid"
)

// Snapshot is the content produced by the fetch step and stored by the save step.
type Snapshot struct {
	SourceURL   string
	ContentType string
	Body        []byte
}

type Fetcher interface {
	Fetch(ctx context.Context, url string) (Snapshot, error)
}

type Store interface {
	Save(ctx context.Context, workflowID uuid.UUID, value Snapshot) error
	Delete(ctx context.Context, workflowID uuid.UUID) error
}
