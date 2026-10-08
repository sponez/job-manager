-- +goose Up
ALTER TABLE workflows ADD COLUMN payload JSONB;

CREATE TABLE snapshots (
    workflow_id UUID PRIMARY KEY REFERENCES workflows(id) ON DELETE CASCADE,
    source_url TEXT NOT NULL,
    content_type TEXT NOT NULL,
    body BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TABLE jobs;

-- +goose Down
DROP TABLE snapshots;

ALTER TABLE workflows DROP COLUMN payload;

CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL
);
