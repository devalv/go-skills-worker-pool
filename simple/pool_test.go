package simple

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestNewValidation(t *testing.T) {
	for _, config := range []Config{
		{}, {Workers: -1, QueueCapacity: 1}, {Workers: 1},
		{Workers: 1, QueueCapacity: -1},
		{Workers: 1, QueueCapacity: 1, Retention: -1},
		{Workers: 1, QueueCapacity: 1, TaskTimeout: -1},
	} {
		if _, err := New[string](config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("New(%+v): %v", config, err)
		}
	}
}

func TestSubmitNilHandler(t *testing.T) {
	pool, err := New[int](Config{Workers: 1, QueueCapacity: 1})
	must(t, err)
	if id, err := pool.Submit(nil); id != 0 || !errors.Is(err, ErrNilHandler) {
		t.Fatalf("nil handler: id=%d, err=%v", id, err)
	}
	must(t, pool.Shutdown(context.Background()))
}

func TestDifferentTasksInOnePool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type VM struct{ Name string }
		pool, err := New[any](Config{Workers: 1, QueueCapacity: 2})
		must(t, err)
		release := make(chan struct{})
		first, err := pool.Submit(func(context.Context) (any, error) {
			<-release
			return VM{Name: "demo"}, nil
		})
		must(t, err)
		synctest.Wait()
		deleted := false
		second, err := pool.Submit(func(context.Context) (any, error) {
			deleted = true
			return true, nil
		})
		must(t, err)
		synctest.Wait()
		if deleted {
			t.Fatal("queued function ran before a worker became available")
		}
		close(release)
		must(t, pool.Shutdown(context.Background()))
		created, err := pool.Get(first)
		must(t, err)
		vm, ok := created.Result.(VM)
		if created.Status != Succeeded || !ok || vm.Name != "demo" {
			t.Fatalf("create result: %+v", created)
		}
		removed, err := pool.Get(second)
		must(t, err)
		if removed.Status != Succeeded || removed.Result != true || !deleted {
			t.Fatalf("delete result: %+v", removed)
		}
	})
}

func TestQueueLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		pool, err := New[int](Config{Workers: 1, QueueCapacity: 1})
		handler := func(_ context.Context, input int) (int, error) {
			<-release
			return input * 2, nil
		}
		must(t, err)
		first, err := pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, 3) })
		must(t, err)
		synctest.Wait()
		task, err := pool.Get(first)
		must(t, err)
		if task.Status != Running || task.StartedAt.IsZero() {
			t.Fatalf("expected running, got %+v", task)
		}
		second, err := pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, 4) })
		must(t, err)
		task, err = pool.Get(second)
		must(t, err)
		if task.Status != Queued || !task.StartedAt.IsZero() {
			t.Fatalf("expected queued, got %+v", task)
		}
		if id, err := pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, 5) }); id != 0 || !errors.Is(err, ErrQueueFull) {
			t.Fatalf("full queue: id=%d, err=%v", id, err)
		}
		close(release)
		must(t, pool.Shutdown(context.Background()))
		for id, want := range map[ID]int{first: 6, second: 8} {
			task, err = pool.Get(id)
			must(t, err)
			if task.Status != Succeeded || task.Result != want || task.Err != nil ||
				task.FinishedAt.Before(task.StartedAt) || task.StartedAt.Before(task.CreatedAt) {
				t.Fatalf("unexpected result: %+v", task)
			}
		}
		if _, err := pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, 6) }); !errors.Is(err, ErrClosed) {
			t.Fatalf("submit after shutdown: %v", err)
		}
		if _, err := pool.Get(999); !errors.Is(err, ErrNotFound) {
			t.Fatalf("unknown ID: %v", err)
		}
		must(t, pool.Shutdown(context.Background()))
	})
}

func TestHandlerFailureAndPanic(t *testing.T) {
	expected := errors.New("external API failed")
	pool, err := New[int](Config{Workers: 1, QueueCapacity: 3})
	handler := func(_ context.Context, input int) (int, error) {
		switch input {
		case 0:
			return 10, expected
		case 1:
			panic("broken handler")
		default:
			return input, nil
		}
	}
	must(t, err)
	ids := make([]ID, 3)
	for i := range ids {
		ids[i], err = pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, i) })
		must(t, err)
	}
	must(t, pool.Shutdown(context.Background()))
	for i, want := range []error{expected, ErrPanic, nil} {
		task, err := pool.Get(ids[i])
		must(t, err)
		if !errors.Is(task.Err, want) {
			t.Fatalf("task %d: %v", i, task.Err)
		}
		if (i < 2 && task.Status != Failed) || (i == 2 && task.Status != Succeeded) {
			t.Fatalf("task %d: status=%s", i, task.Status)
		}
	}
}

func TestRetentionStartsAfterCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		pool, err := New[int](Config{Workers: 1, QueueCapacity: 1, Retention: time.Second})
		handler := func(_ context.Context, input int) (int, error) { <-release; return input, nil }
		must(t, err)
		first, err := pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, 1) })
		must(t, err)
		synctest.Wait()
		second, err := pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, 2) })
		must(t, err)
		time.Sleep(2 * time.Second)
		for _, id := range []ID{first, second} {
			_, err = pool.Get(id)
			must(t, err)
		}
		close(release)
		synctest.Wait()
		_, err = pool.Get(first)
		must(t, err)
		time.Sleep(2 * time.Second)
		synctest.Wait()
		pool.mu.Lock()
		remaining := len(pool.tasks)
		pool.mu.Unlock()
		if remaining != 0 {
			t.Fatalf("background cleanup left %d tasks", remaining)
		}
		must(t, pool.Shutdown(context.Background()))
	})
}

func TestExpirationAfterShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, retention := range []time.Duration{0, time.Second} {
			pool, err := New[int](Config{Workers: 1, QueueCapacity: 1, Retention: retention})
			handler := func(_ context.Context, input int) (int, error) { return input, nil }
			must(t, err)
			id, err := pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, 1) })
			must(t, err)
			must(t, pool.Shutdown(context.Background()))
			time.Sleep(time.Second)
			_, err = pool.Get(id)
			if retention == 0 {
				must(t, err)
			} else if !errors.Is(err, ErrNotFound) {
				t.Fatalf("expired task: %v", err)
			}
		}
	})
}

func TestTaskTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool, err := New[int](Config{Workers: 1, QueueCapacity: 1, TaskTimeout: time.Second})
		handler := func(ctx context.Context, _ int) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}
		must(t, err)
		id, err := pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, 0) })
		must(t, err)
		must(t, pool.Shutdown(context.Background()))
		task, err := pool.Get(id)
		must(t, err)
		if task.Status != Canceled || !errors.Is(task.Err, context.DeadlineExceeded) {
			t.Fatalf("timeout: %+v", task)
		}
	})
}

func TestForcedShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var called atomic.Int32
		pool, err := New[int](Config{Workers: 1, QueueCapacity: 1})
		handler := func(ctx context.Context, _ int) (int, error) {
			called.Add(1)
			<-ctx.Done()
			return 0, ctx.Err()
		}
		must(t, err)
		first, err := pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, 1) })
		must(t, err)
		synctest.Wait()
		second, err := pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, 2) })
		must(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := pool.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown: %v", err)
		}
		must(t, pool.Shutdown(context.Background()))
		if called.Load() != 1 {
			t.Fatal("queued handler executed after forced shutdown")
		}
		for _, id := range []ID{first, second} {
			task, err := pool.Get(id)
			must(t, err)
			if task.Status != Canceled || !errors.Is(task.Err, context.Canceled) {
				t.Fatalf("canceled task: %+v", task)
			}
		}
	})
}

func TestConcurrencyAndIsolation(t *testing.T) {
	const workers, count = 4, 100
	var active, peak atomic.Int32
	release := make(chan struct{})
	pool, err := New[int](Config{Workers: workers, QueueCapacity: count})
	handler := func(ctx context.Context, input int) (int, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); current > old; old = peak.Load() {
			if peak.CompareAndSwap(old, current) {
				break
			}
		}
		select {
		case <-release:
			return input * 2, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	must(t, err)
	var producers sync.WaitGroup
	ids := make(chan ID, count)
	for i := range count {
		producers.Go(func() {
			id, err := pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, i) })
			if err != nil {
				t.Errorf("submit: %v", err)
				return
			}
			ids <- id
			if _, err := pool.Get(id); err != nil {
				t.Errorf("get: %v", err)
			}
		})
	}
	producers.Wait()
	close(ids)
	close(release)
	var shutdowns sync.WaitGroup
	for range 4 {
		shutdowns.Go(func() {
			if err := pool.Shutdown(context.Background()); err != nil {
				t.Errorf("shutdown: %v", err)
			}
		})
	}
	shutdowns.Wait()
	if peak.Load() > workers || peak.Load() < 1 {
		t.Fatalf("peak concurrency: %d", peak.Load())
	}
	seen := make(map[ID]bool)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate ID: %d", id)
		}
		seen[id] = true
		task, err := pool.Get(id)
		must(t, err)
		if task.Status != Succeeded {
			t.Fatalf("unfinished task: %+v", task)
		}
	}
	if len(seen) != count {
		t.Fatalf("accepted %d tasks", len(seen))
	}
	other, err := New[int](Config{Workers: 1, QueueCapacity: 1})
	must(t, err)
	if _, err := other.Get(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("shared state: %v", err)
	}
	must(t, other.Shutdown(context.Background()))
}

func TestWorkersRunInParallel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		pool, err := New[int](Config{Workers: 3, QueueCapacity: 4})
		handler := func(_ context.Context, input int) (int, error) { <-release; return input, nil }
		must(t, err)
		ids := make([]ID, 4)
		for i := range ids {
			ids[i], err = pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, i) })
			must(t, err)
		}
		synctest.Wait()
		running, queued := 0, 0
		for _, id := range ids {
			task, err := pool.Get(id)
			must(t, err)
			if task.Status == Running {
				running++
			}
			if task.Status == Queued {
				queued++
			}
		}
		if running != 3 || queued != 1 {
			t.Fatalf("running=%d, queued=%d", running, queued)
		}
		close(release)
		must(t, pool.Shutdown(context.Background()))
	})
}

func TestShutdownDoesNotWaitForUncooperativeHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		pool, err := New[int](Config{Workers: 1, QueueCapacity: 1})
		handler := func(_ context.Context, input int) (int, error) { <-release; return input, nil }
		must(t, err)
		id, err := pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, 42) })
		must(t, err)
		synctest.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := pool.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown: %v", err)
		}
		task, err := pool.Get(id)
		must(t, err)
		if task.Status != Running {
			t.Fatalf("premature completion: %+v", task)
		}
		close(release)
		must(t, pool.Shutdown(context.Background()))
		task, err = pool.Get(id)
		must(t, err)
		if task.Status != Succeeded || task.Result != 42 {
			t.Fatalf("handler result lost: %+v", task)
		}
	})
}

func TestSubmitRacesWithShutdown(t *testing.T) {
	pool, err := New[int](Config{Workers: 4, QueueCapacity: 100})
	handler := func(_ context.Context, input int) (int, error) { return input, nil }
	must(t, err)
	start := make(chan struct{})
	accepted := make(chan ID, 100)
	var group sync.WaitGroup
	for i := range 100 {
		group.Go(func() {
			<-start
			id, err := pool.Submit(func(ctx context.Context) (int, error) { return handler(ctx, i) })
			if err == nil {
				accepted <- id
			} else if !errors.Is(err, ErrClosed) {
				t.Errorf("submit: %v", err)
			}
		})
	}
	group.Go(func() {
		<-start
		if err := pool.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	close(start)
	group.Wait()
	close(accepted)
	for id := range accepted {
		task, err := pool.Get(id)
		must(t, err)
		if task.Status != Succeeded {
			t.Fatalf("accepted task lost: %+v", task)
		}
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
