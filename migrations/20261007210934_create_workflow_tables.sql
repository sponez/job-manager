-- +goose Up
CREATE TABLE workflows (
    id UUID PRIMARY KEY,

    type TEXT NOT NULL,
    definition_version INTEGER NOT NULL,

    status TEXT NOT NULL,

    idempotency_key TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE workflow_steps (
    id UUID PRIMARY KEY,

    workflow_id UUID NOT NULL
        REFERENCES workflows(id)
        ON DELETE CASCADE,

    seq BIGINT NOT NULL,

    type TEXT NOT NULL,

    -- forward / compensation
    kind TEXT NOT NULL,

    -- only for compensation steps
    compensates_step_id UUID
        REFERENCES workflow_steps(id),

    status TEXT NOT NULL,

    payload JSONB,
    result JSONB,
    last_error JSONB,

    -- pending: don't execute before this time
    -- running: lease expires at this time
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,

    -- Also acts as fencing token for stale workers.
    version BIGINT NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (workflow_id, seq)
);

-- +goose Down
DROP TABLE workflow_steps;
DROP TABLE workflows;
