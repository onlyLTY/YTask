package ytask

import (
	"context"
	"fmt"
	"time"
)

// ExampleTask is a simple example implementation of the Task interface
type ExampleTask struct {
	BaseTask
	// Data holds the task's data
	Data string
	// Duration is how long the task will take to execute
	Duration time.Duration
	// ShouldFail indicates if the task should fail
	ShouldFail bool
}

// NewExampleTask creates a new example task
func NewExampleTask(id string, priority int, data string, duration time.Duration, shouldFail bool) *ExampleTask {
	baseTask := NewBaseTask(id, "example", priority)
	return &ExampleTask{
		BaseTask:   *baseTask,
		Data:       data,
		Duration:   duration,
		ShouldFail: shouldFail,
	}
}

// Execute runs the example task
func (t *ExampleTask) Execute(ctx context.Context) error {
	// Check if context is cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Continue execution
	}

	// Simulate work by sleeping
	timer := time.NewTimer(t.Duration)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		// Work completed
	}

	// Return error if task is configured to fail
	if t.ShouldFail {
		return fmt.Errorf("task %s failed as configured", t.ID)
	}

	return nil
}

// IsDuplicate checks if this task is a duplicate of another task
// For example tasks, we consider them duplicates if they have the same ID and Data
func (t *ExampleTask) IsDuplicate(other Task) bool {
	// First check if IDs match
	if t.ID != other.GetID() {
		return false
	}

	// Then check if the other task is also an ExampleTask
	otherTask, ok := other.(*ExampleTask)
	if !ok {
		return false
	}

	// Finally, check if the Data field matches
	return t.Data == otherTask.Data
}

// ExampleMiddleware is a simple middleware that logs task execution
func ExampleMiddleware(next func(ctx context.Context, task Task) error) func(ctx context.Context, task Task) error {
	return func(ctx context.Context, task Task) error {
		fmt.Printf("Starting task %s (type: %s, priority: %d)\n",
			task.GetID(), task.GetType(), task.GetPriority())

		startTime := time.Now()
		err := next(ctx, task)
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("Task %s failed after %v: %v\n", task.GetID(), duration, err)
		} else {
			fmt.Printf("Task %s completed successfully in %v\n", task.GetID(), duration)
		}

		return err
	}
}

// ExampleFailureHandler is a simple failure handler that retries tasks with exponential backoff
func ExampleFailureHandler(task Task, err error) (time.Time, bool) {
	failureCount := task.GetFailureCount()

	// Maximum number of retries
	if failureCount > 3 {
		fmt.Printf("Task %s has failed %d times, giving up\n", task.GetID(), failureCount)
		return time.Time{}, false
	}

	// Calculate backoff duration (exponential)
	backoff := time.Duration(1<<uint(failureCount)) * time.Second
	nextTime := time.Now().Add(backoff)

	fmt.Printf("Task %s failed with error: %v. Retry #%d scheduled in %v\n",
		task.GetID(), err, failureCount, backoff)

	return nextTime, true
}
