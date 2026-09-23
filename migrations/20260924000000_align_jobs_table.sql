-- +goose Up
ALTER TABLE job RENAME TO jobs;
ALTER TABLE jobs ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);

-- +goose Down
ALTER TABLE jobs DROP CONSTRAINT jobs_pkey;
ALTER TABLE jobs RENAME TO job;
