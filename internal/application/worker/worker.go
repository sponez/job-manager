package worker

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
)

var (
	ErrIsNotStarted   = errors.New("pool is not started")
	ErrAlreadyStarted = errors.New("pool is already started")
	ErrClosed         = errors.New("pool is closed")
	ErrQueueIsFull    = errors.New("queue is full")
	ErrNilTask        = errors.New("task is nil")
)

// Task receives the pool's context, not the context used to enqueue it.
// Tasks must cooperate with cancellation and handle their own execution errors.
type Task func(context.Context)

// WorkerPool runs tasks with bounded concurrency. Use New to construct it.
// Its methods are safe to call concurrently. A pool cannot be restarted or copied.
type WorkerPool struct {
	wg sync.WaitGroup
	mx sync.Mutex

	started     bool
	closed      bool
	ctx         context.Context
	workerCount int
	queue       chan Task
}

// New requires a positive worker count and a nonnegative queue size.
// With an unbuffered queue, Push succeeds only if a worker is ready to receive.
func New(workerCount, queueSize int) (*WorkerPool, error) {
	if workerCount <= 0 {
		return nil, errors.New("worker count must be positive")
	}
	if queueSize < 0 {
		return nil, errors.New("queue size must not be negative")
	}
	return &WorkerPool{
		workerCount: workerCount,
		queue:       make(chan Task, queueSize),
	}, nil
}

// Shutdown stops admission and waits for every worker. Repeated calls also wait.
// The queue is drained unless the Start context is canceled. Shutdown does not
// cancel running tasks and cannot return until they finish. A task must not call
// Shutdown on its own pool, since it would wait for itself.
func (wp *WorkerPool) Shutdown() {
	wp.mx.Lock()
	if !wp.closed {
		wp.closed = true
		close(wp.queue)
	}
	wp.mx.Unlock()

	wp.wg.Wait()
}

// Push enqueues without waiting for space. Success means accepted, not completed.
// ctx controls admission only; its later cancellation does not cancel the task.
func (wp *WorkerPool) Push(ctx context.Context, task Task) error {
	wp.mx.Lock()
	defer wp.mx.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if wp.closed {
		return ErrClosed
	}
	if !wp.started {
		return ErrIsNotStarted
	}
	if err := wp.ctx.Err(); err != nil {
		return err
	}
	if task == nil {
		return ErrNilTask
	}

	// Keep the send and close under the same mutex to prevent send-on-closed panic.
	select {
	case wp.queue <- task:
		return nil
	default:
		return ErrQueueIsFull
	}
}

// Start launches workers once. Cancellation stops workers and may leave queued
// tasks unexecuted; running tasks receive the cancellation through ctx.
func (wp *WorkerPool) Start(ctx context.Context) error {
	wp.mx.Lock()
	defer wp.mx.Unlock()

	if wp.closed {
		return ErrClosed
	}
	if wp.started {
		return ErrAlreadyStarted
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	wp.ctx = ctx
	wp.started = true
	// Register every worker before Shutdown can start waiting.
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Go(func() { wp.run(ctx) })
	}
	return nil
}

func (wp *WorkerPool) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-wp.queue:
			if !ok {
				return
			}
			// A ready queue can win the select even after cancellation.
			if ctx.Err() != nil {
				return
			}
			execTaskWithRecover(ctx, task)
		}
	}
}

func execTaskWithRecover(ctx context.Context, task Task) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "worker task panicked", "panic", r, "stack", string(debug.Stack()))
		}
	}()

	task(ctx)
}
