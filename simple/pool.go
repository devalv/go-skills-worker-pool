// Package simple provides an in-process queue with bounded concurrency and typed results.
package simple

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrInvalidConfig = errors.New("invalid worker count, queue capacity, retention or task timeout")
	ErrNilHandler    = errors.New("handler must not be nil")
	ErrQueueFull     = errors.New("queue is full")
	ErrClosed        = errors.New("pool is closed")
	ErrNotFound      = errors.New("task not found")
	ErrPanic         = errors.New("handler panicked")
)

// ID identifies a task within a single pool instance.
type ID uint64

type Status string

const (
	Queued    Status = "queued"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Canceled  Status = "canceled"
)

type Config struct {
	Workers       int
	QueueCapacity int
	// Retention is measured from completion. Zero keeps results until the pool is discarded.
	Retention time.Duration
	// TaskTimeout starts when a worker picks up a task. Zero disables the timeout.
	TaskTimeout time.Duration
}

// Handler completes the logical operation, including any external API polling.
// It must honor ctx for timeouts and forced shutdown to take effect promptly.
type Handler[R any] func(ctx context.Context) (R, error)

// Task is a snapshot. Result is final only for a terminal status.
// Reference values are not deep-copied: callers must synchronize access to shared data.
type Task[R any] struct {
	ID         ID
	Status     Status
	Result     R
	Err        error
	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time
}

type job[R any] struct {
	id      ID
	handler Handler[R]
}

// Pool must be constructed with New and must not be copied after use.
type Pool[R any] struct {
	mu     sync.Mutex
	config Config
	queue  chan job[R]
	tasks  map[ID]Task[R]
	nextID ID
	closed bool
	ctx    context.Context //nolint:containedctx // Owns the pool lifetime, independently of caller requests.
	cancel context.CancelFunc
	done   chan struct{}
}

// New starts workers and, when retention is positive, background result cleanup.
func New[R any](config Config) (*Pool[R], error) {
	if config.Workers < 1 || config.QueueCapacity < 1 || config.Retention < 0 || config.TaskTimeout < 0 {
		return nil, ErrInvalidConfig
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool[R]{
		config: config,
		queue:  make(chan job[R], config.QueueCapacity),
		tasks:  make(map[ID]Task[R]), ctx: ctx, cancel: cancel,
		done: make(chan struct{}),
	}

	var workers sync.WaitGroup
	for range config.Workers {
		workers.Go(p.work)
	}
	go func() {
		workers.Wait()
		cancel()
		close(p.done)
	}()
	if config.Retention > 0 {
		go p.cleanupLoop()
	}

	return p, nil
}

// Submit queues a function for execution by a worker and immediately returns its ID.
// A nil handler, full queue or closed pool returns an error and a zero ID.
// Data captured by the function must not be mutated concurrently without synchronization.
func (p *Pool[R]) Submit(handler Handler[R]) (ID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, ErrClosed
	}
	if handler == nil {
		return 0, ErrNilHandler
	}

	id := p.nextID + 1
	select {
	case p.queue <- job[R]{id: id, handler: handler}:
		p.nextID = id
		p.tasks[id] = Task[R]{ID: id, Status: Queued, CreatedAt: time.Now()}

		return id, nil
	default:
		return 0, ErrQueueFull
	}
}

// Get returns a snapshot or ErrNotFound for unknown or expired tasks.
// It remains available after shutdown; expiration is still enforced on reads.
func (p *Pool[R]) Get(id ID) (Task[R], error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	task, ok := p.tasks[id]
	if !ok || p.expired(task, time.Now()) {
		delete(p.tasks, id)

		return Task[R]{}, ErrNotFound
	}

	return task, nil
}

// Shutdown stops admission and drains accepted work. Concurrent calls are safe.
// If ctx expires, running handlers are canceled and queued handlers are skipped.
// It returns without waiting for handlers that ignore cancellation. A later call
// can wait for them. Do not call Shutdown from a handler of this pool.
func (p *Pool[R]) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()

	select {
	case <-p.done:
		return nil
	default:
	}
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		p.cancel()

		return fmt.Errorf("shutdown: %w", ctx.Err())
	}
}

func (p *Pool[R]) work() {
	for job := range p.queue {
		p.execute(job)
	}
}

func (p *Pool[R]) execute(job job[R]) {
	p.mu.Lock()
	task := p.tasks[job.id]
	task.Status = Running
	task.StartedAt = time.Now()
	p.tasks[job.id] = task
	p.mu.Unlock()

	ctx := p.ctx
	if p.config.TaskTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.config.TaskTimeout)
		defer cancel()
	}

	result, err := p.invoke(ctx, job.handler)
	task.Result = result
	task.Err = err
	task.FinishedAt = time.Now()
	task.Status = Succeeded
	if err != nil {
		task.Status = Failed
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			task.Status = Canceled
		}
	}

	p.mu.Lock()
	p.tasks[job.id] = task
	p.mu.Unlock()
}

func (p *Pool[R]) invoke(ctx context.Context, handler Handler[R]) (result R, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("%w: %v", ErrPanic, value)
		}
	}()
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("task canceled before execution: %w", err)
	}

	return handler(ctx)
}

func (p *Pool[R]) expired(task Task[R], now time.Time) bool {
	return p.config.Retention > 0 && !task.FinishedAt.IsZero() &&
		!now.Before(task.FinishedAt.Add(p.config.Retention))
}

func (p *Pool[R]) cleanupLoop() {
	interval := min(p.config.Retention, time.Minute)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			p.mu.Lock()
			for id, task := range p.tasks {
				if p.expired(task, now) {
					delete(p.tasks, id)
				}
			}
			p.mu.Unlock()
		case <-p.done:
			return
		}
	}
}
