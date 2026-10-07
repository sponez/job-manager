-- +goose Up
CREATE INDEX idx_workflow_steps_available
    ON workflow_steps (available_at, id)
    WHERE available_at IS NOT NULL;

CREATE INDEX idx_workflow_steps_workflow_id
    ON workflow_steps (workflow_id);

CREATE UNIQUE INDEX idx_workflows_idempotency
    ON workflows (type, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- +goose Down
DROP INDEX idx_workflow_steps_available;
DROP INDEX idx_workflow_steps_workflow_id;
DROP INDEX idx_workflows_idempotency;
