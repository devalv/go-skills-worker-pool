package main

import (
	"context"
	"fmt"
	"time"

	"github.com/devalv/go-skills-worker-pool/simple"
)

type CreateVM struct {
	Name string
}

type VM struct {
	ID string
}

func main() {
	if err := run(); err != nil {
		fmt.Printf("error: %v\n", err)
	}
}

func run() error {
	pool, err := simple.New[any](simple.Config{
		Workers:       2,
		QueueCapacity: 16,
		Retention:     time.Hour,
		TaskTimeout:   time.Minute,
	})
	if err != nil {
		return fmt.Errorf("create pool: %w", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := pool.Shutdown(ctx); err != nil {
			fmt.Printf("shutdown: %v\n", err)
		}
	}()

	id, err := pool.Submit(func(ctx context.Context) (any, error) {
		return createVM(ctx, CreateVM{Name: "demo"})
	})
	if err != nil {
		return fmt.Errorf("submit: %w", err)
	}
	fmt.Printf("accepted task %d\n", id)
	checkID, err := pool.Submit(func(ctx context.Context) (any, error) {
		return checkService(ctx)
	})
	if err != nil {
		return fmt.Errorf("submit service check: %w", err)
	}
	fmt.Printf("accepted service check %d\n", checkID)

	// The caller may return the ID immediately or poll for completion.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		task, err := pool.Get(id)
		if err != nil {
			return fmt.Errorf("get task: %w", err)
		}
		switch task.Status {
		case simple.Succeeded:
			vm, ok := task.Result.(VM)
			if !ok {
				return fmt.Errorf("unexpected VM result: %T", task.Result)
			}
			check, err := pool.Get(checkID)
			if err != nil {
				return fmt.Errorf("get service check: %w", err)
			}
			switch check.Status {
			case simple.Succeeded:
				fmt.Printf("created VM: %s; service available: %v\n", vm.ID, check.Result)

				return nil
			case simple.Failed, simple.Canceled:
				return fmt.Errorf("check service: %w", check.Err)
			case simple.Queued, simple.Running:
				continue
			}
		case simple.Failed, simple.Canceled:
			return fmt.Errorf("create VM: %w", task.Err)
		case simple.Queued, simple.Running:
			continue
		}
	}

	return nil
}

func checkService(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("service check interrupted: %w", err)
	}

	return true, nil
}

// Replace this simulation with an API call. If the API returns an operation ID,
// poll that operation here until the VM is ready, honoring ctx throughout.
func createVM(ctx context.Context, input CreateVM) (VM, error) {
	timer := time.NewTimer(25 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return VM{ID: "vm-" + input.Name}, nil
	case <-ctx.Done():
		return VM{}, fmt.Errorf("create VM interrupted: %w", ctx.Err())
	}
}
