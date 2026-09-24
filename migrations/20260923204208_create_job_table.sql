-- +goose Up
CREATE TABLE IF NOT EXISTS job (
    id UUID NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL
);

-- +goose Down
DROP TABLE job;
