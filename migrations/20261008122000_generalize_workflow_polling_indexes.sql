-- +goose Up
DROP INDEX idx_workflow_steps_available;
DROP INDEX idx_workflows_pending;

CREATE INDEX idx_workflow_steps_status_available
    ON workflow_steps (status, available_at, id);

CREATE INDEX idx_workflows_status_created
    ON workflows (status, created_at, id);

-- +goose Down
DROP INDEX idx_workflows_status_created;
DROP INDEX idx_workflow_steps_status_available;

CREATE INDEX idx_workflows_pending
    ON workflows (created_at, id)
    WHERE status = 'pending';

CREATE INDEX idx_workflow_steps_available
    ON workflow_steps (available_at, id)
    WHERE status IN ('pending', 'running');
