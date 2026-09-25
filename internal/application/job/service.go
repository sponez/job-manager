package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
	"uuid"

	"github.com/sponez/job-manager/internal/domain/job"
)

type JobService struct {
	jobRepository JobRepository
}

func New(jobRepository JobRepository) *JobService {
	return &JobService{jobRepository: jobRepository}
}

func (js *JobService) CreateJob(ctx context.Context, kind string) (*job.Job, error) {
	j, err := job.New(uuid.New(), job.Kind(kind), job.StatusPending)
	if err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}

	if err := js.jobRepository.CreateJob(ctx, j); err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}

	return j, nil
}

func (js *JobService) GetJob(ctx context.Context, id job.ID) (*job.Job, error) {
	j, err := js.jobRepository.GetJob(ctx, id)

	if err != nil {
		return nil, fmt.Errorf("get job %s: %w", id, err)
	}

	return j, nil
}

func (js *JobService) CompleteJob(ctx context.Context, id job.ID) error {
	err := js.jobRepository.UpdateStatusByID(ctx, id, job.StatusDone)

	if err != nil {
		return fmt.Errorf("complete job %s: %w", id, err)
	}

	return nil
}

// ListJobs preserves partial results when some records cannot be read.
func (js *JobService) ListJobs(ctx context.Context) ([]*job.Job, error) {
	jobs, err := js.jobRepository.ListJobs(ctx)

	if err != nil {
		return jobs, fmt.Errorf("list jobs: %w", err)
	}

	return jobs, nil
}

func (js *JobService) DeleteJob(ctx context.Context, id job.ID) error {
	if err := js.jobRepository.DeleteJob(ctx, id); err != nil {
		return fmt.Errorf("delete job: %w", err)
	}

	return nil
}

func (js *JobService) ProcessJob(ctx context.Context, id job.ID) {
	logErr := func(ctx context.Context, err error) {
		slog.ErrorContext(ctx, "failed to process job", "job_id", id, "error", err)
	}

	j, err := js.jobRepository.GetJob(ctx, id)
	if err != nil {
		logErr(ctx, err)
		return
	}

	if err := js.jobRepository.UpdateStatusByID(ctx, id, job.StatusInProgress); err != nil {
		logErr(ctx, err)
		return
	}

	switch j.Kind() {
	case job.KindGetPage:
		js.execJob(ctx, id, execGetPage)
	case job.KindSendEmail:
		js.execJob(ctx, id, execSendEmail)
	default:
		logErr(ctx, fmt.Errorf("unknown job kind"))
	}
}

func execGetPage(ctx context.Context) error {
	if err := waitForWork(ctx, 2*time.Second); err != nil {
		return err
	}

	failed := (rand.IntN(100) == 99)
	if failed {
		return errors.New("failed to get page")
	}

	return nil
}

func execSendEmail(ctx context.Context) error {
	if err := waitForWork(ctx, 15*time.Second); err != nil {
		return err
	}

	failed := (rand.IntN(10) == 9)
	if failed {
		return errors.New("failed to send email")
	}

	return nil
}

func waitForWork(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func (js *JobService) execJob(ctx context.Context, id job.ID, executor func(context.Context) error) {
	status := job.StatusDone
	if err := executor(ctx); err != nil {
		status = job.StatusError
		slog.ErrorContext(ctx, "failed to process job", "job_id", id, "error", err)
	}

	// Persist the outcome even when execution ended because its context expired.
	resultCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := js.jobRepository.UpdateStatusByID(resultCtx, id, status); err != nil {
		slog.ErrorContext(resultCtx, "failed to save job outcome", "job_id", id, "status", status, "error", err)
	}
}
