package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

func newTestPool(t *testing.T, workers, queueSize int) (*WorkerPool, context.Context, context.CancelFunc) {
	t.Helper()
	pool, err := New(workers, queueSize)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		pool.Shutdown()
	})
	return pool, ctx, cancel
}

func TestNewValidation(t *testing.T) {
	for _, tt := range []struct {
		name    string
		workers int
		queue   int
		valid   bool
	}{
		{"zero workers", 0, 1, false},
		{"negative workers", -1, 1, false},
		{"negative queue", 1, -1, false},
		{"unbuffered queue", 1, 0, true},
		{"buffered queue", 2, 10, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := New(tt.workers, tt.queue)
			if (err == nil) != tt.valid || (pool != nil) != tt.valid {
				t.Fatalf("New(%d, %d) = %v, %v", tt.workers, tt.queue, pool, err)
			}
		})
	}
}

func TestLifecycle(t *testing.T) {
	pool, ctx, _ := newTestPool(t, 1, 1)
	task := func(context.Context) {}
	if err := pool.Push(ctx, task); !errors.Is(err, ErrIsNotStarted) {
		t.Fatalf("Push before Start = %v", err)
	}
	if err := pool.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.Start(ctx); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("repeated Start = %v", err)
	}
	if err := pool.Push(ctx, nil); !errors.Is(err, ErrNilTask) {
		t.Fatalf("Push(nil) = %v", err)
	}
	pool.Shutdown()
	pool.Shutdown()
	pool.Shutdown()
	if err := pool.Push(ctx, task); !errors.Is(err, ErrClosed) {
		t.Fatalf("Push after Shutdown = %v", err)
	}
	if err := pool.Start(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after Shutdown = %v", err)
	}
}

func TestShutdownBeforeStart(t *testing.T) {
	pool, ctx, _ := newTestPool(t, 1, 1)
	pool.Shutdown()
	if err := pool.Start(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after Shutdown = %v", err)
	}
}

func TestCanceledStartDoesNotConsumeStart(t *testing.T) {
	pool, ctx, _ := newTestPool(t, 1, 1)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := pool.Start(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start with canceled context = %v", err)
	}
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start after rejected attempt = %v", err)
	}
}

func TestConcurrencyLimitQueueCapacityAndDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workers, queueSize = 2, 3
		pool, ctx, _ := newTestPool(t, workers, queueSize)
		if err := pool.Start(ctx); err != nil {
			t.Fatal(err)
		}
		release := make(chan struct{})
		var running, completed atomic.Int32
		task := func(ctx context.Context) {
			running.Add(1)
			select {
			case <-release:
			case <-ctx.Done():
			}
			running.Add(-1)
			completed.Add(1)
		}
		for i := 0; i < workers; i++ {
			if err := pool.Push(ctx, task); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if got := running.Load(); got != workers {
			t.Fatalf("running tasks = %d, want %d", got, workers)
		}
		for i := 0; i < queueSize; i++ {
			if err := pool.Push(ctx, task); err != nil {
				t.Fatal(err)
			}
		}
		if err := pool.Push(ctx, task); !errors.Is(err, ErrQueueIsFull) {
			t.Fatalf("Push into full queue = %v", err)
		}
		synctest.Wait()
		if got := running.Load(); got != workers {
			t.Fatalf("running tasks after filling queue = %d", got)
		}

		// Every simultaneous Shutdown must wait for the running tasks.
		var stopped atomic.Int32
		for i := 0; i < 3; i++ {
			go func() {
				pool.Shutdown()
				stopped.Add(1)
			}()
		}
		synctest.Wait()
		if got := stopped.Load(); got != 0 {
			t.Fatalf("%d Shutdown calls returned before tasks completed", got)
		}
		if err := pool.Push(ctx, task); !errors.Is(err, ErrClosed) {
			t.Fatalf("Push during Shutdown = %v", err)
		}
		close(release)
		synctest.Wait()
		if got := stopped.Load(); got != 3 {
			t.Errorf("completed Shutdown calls = %d, want 3", got)
		}
		if got := completed.Load(); got != workers+queueSize {
			t.Errorf("completed tasks = %d, want %d", got, workers+queueSize)
		}
	})
}

func TestCancellationStopsWorkersAndAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool, ctx, cancel := newTestPool(t, 1, 1)
		if err := pool.Start(ctx); err != nil {
			t.Fatal(err)
		}
		var taskContext context.Context
		if err := pool.Push(context.Background(), func(ctx context.Context) {
			taskContext = ctx
			<-ctx.Done()
		}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		var queuedRan bool
		if err := pool.Push(context.Background(), func(context.Context) { queuedRan = true }); err != nil {
			t.Fatal(err)
		}
		cancel()
		synctest.Wait()
		if taskContext != ctx {
			t.Error("task did not receive the pool context")
		}
		if queuedRan {
			t.Error("queued task ran after cancellation")
		}
		if err := pool.Push(context.Background(), func(context.Context) {}); !errors.Is(err, context.Canceled) {
			t.Fatalf("Push into canceled pool = %v", err)
		}
		pool.Shutdown()
	})
}

func TestAdmissionContextDoesNotCancelAcceptedTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool, ctx, _ := newTestPool(t, 1, 1)
		if err := pool.Start(ctx); err != nil {
			t.Fatal(err)
		}
		release := make(chan struct{})
		var taskErr error
		admissionCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := pool.Push(admissionCtx, func(ctx context.Context) {
			select {
			case <-release:
			case <-ctx.Done():
			}
			taskErr = ctx.Err()
		}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		cancel()
		if err := pool.Push(admissionCtx, func(context.Context) {}); !errors.Is(err, context.Canceled) {
			t.Fatalf("Push with canceled admission context = %v", err)
		}
		close(release)
		pool.Shutdown()
		if taskErr != nil {
			t.Errorf("accepted task was canceled: %v", taskErr)
		}
	})
}

func TestWorkerSurvivesTaskPanic(t *testing.T) {
	pool, ctx, _ := newTestPool(t, 1, 2)
	if err := pool.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.Push(ctx, func(context.Context) { panic("broken task") }); err != nil {
		t.Fatal(err)
	}
	var ran bool
	if err := pool.Push(ctx, func(context.Context) { ran = true }); err != nil {
		t.Fatal(err)
	}
	pool.Shutdown()
	if !ran {
		t.Fatal("worker did not execute the task following a panic")
	}
}

func TestUnbufferedQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool, ctx, _ := newTestPool(t, 1, 0)
		if err := pool.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait() // The worker is waiting to receive.
		var ran bool
		if err := pool.Push(ctx, func(context.Context) { ran = true }); err != nil {
			t.Fatal(err)
		}
		pool.Shutdown()
		if !ran {
			t.Fatal("task was not executed")
		}
	})
}

func TestConcurrentLifecycle(t *testing.T) {
	pool, ctx, _ := newTestPool(t, 4, 32)
	var calls sync.WaitGroup
	var accepted, executed atomic.Int32
	begin := make(chan struct{})
	for i := 0; i < 48; i++ {
		calls.Go(func() {
			<-begin
			switch i % 3 {
			case 0:
				err := pool.Start(ctx)
				if err != nil && !errors.Is(err, ErrAlreadyStarted) && !errors.Is(err, ErrClosed) {
					t.Errorf("Start = %v", err)
				}
			case 1:
				for j := 0; j < 32; j++ {
					err := pool.Push(ctx, func(context.Context) { executed.Add(1) })
					if err == nil {
						accepted.Add(1)
					} else if !errors.Is(err, ErrIsNotStarted) && !errors.Is(err, ErrClosed) && !errors.Is(err, ErrQueueIsFull) {
						t.Errorf("Push = %v", err)
					}
				}
			case 2:
				pool.Shutdown()
			}
		})
	}
	close(begin)
	calls.Wait()
	pool.Shutdown()
	if accepted.Load() != executed.Load() {
		t.Errorf("accepted %d tasks, executed %d", accepted.Load(), executed.Load())
	}
}
