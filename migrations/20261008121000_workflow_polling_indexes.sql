-- +goose Up
DROP INDEX idx_workflow_steps_available;

CREATE INDEX idx_workflow_steps_available
    ON workflow_steps (available_at, id)
    WHERE status IN ('pending', 'running');

CREATE INDEX idx_workflows_pending
    ON workflows (created_at, id)
    WHERE status = 'pending';

CREATE UNIQUE INDEX idx_workflow_compensations_target
    ON workflow_steps (compensates_step_id)
    WHERE kind = 'compensation';

-- +goose Down
DROP INDEX idx_workflow_compensations_target;
DROP INDEX idx_workflows_pending;
DROP INDEX idx_workflow_steps_available;

CREATE INDEX idx_workflow_steps_available
    ON workflow_steps (available_at, id)
    WHERE available_at IS NOT NULL;
