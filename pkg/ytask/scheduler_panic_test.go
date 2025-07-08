package ytask

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestSchedulerPanicHandlingWithErrorPanic tests that the scheduler properly handles panics
// when a task panics with an error value
func TestSchedulerPanicHandlingWithErrorPanic(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Register a task type
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Create a task that panics with an error
	panicErr := errors.New("intentional error panic for testing")
	panicTask := NewTestTask("panic-task", "test", 0, func(ctx context.Context) error {
		panic(panicErr)
	})

	// Add the task
	scheduler.AddTask(panicTask)

	// Start the scheduler
	scheduler.Start()

	// Wait for task to be processed
	time.Sleep(200 * time.Millisecond)

	// Stop the scheduler
	scheduler.Stop()

	// Check stats
	stats := scheduler.GetStats()

	// The task should have failed and not been retried
	if stats["test"].Failed != 1 {
		t.Errorf("Expected 1 failed task, got %d", stats["test"].Failed)
	}

	if stats["test"].Queued != 0 {
		t.Errorf("Expected 0 queued tasks, got %d", stats["test"].Queued)
	}

	if stats["test"].Completed != 0 {
		t.Errorf("Expected 0 completed tasks, got %d", stats["test"].Completed)
	}
}

// TestSchedulerPanicHandlingWithNonStringPanic tests that the scheduler properly handles panics
// when a task panics with a non-string, non-error value
func TestSchedulerPanicHandlingWithNonStringPanic(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Register a task type
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Create a task that panics with an integer
	panicTask := NewTestTask("panic-task", "test", 0, func(ctx context.Context) error {
		panic(42) // Panic with an integer
	})

	// Add the task
	scheduler.AddTask(panicTask)

	// Start the scheduler
	scheduler.Start()

	// Wait for task to be processed
	time.Sleep(200 * time.Millisecond)

	// Stop the scheduler
	scheduler.Stop()

	// Check stats
	stats := scheduler.GetStats()

	// The task should have failed and not been retried
	if stats["test"].Failed != 1 {
		t.Errorf("Expected 1 failed task, got %d", stats["test"].Failed)
	}

	if stats["test"].Queued != 0 {
		t.Errorf("Expected 0 queued tasks, got %d", stats["test"].Queued)
	}

	if stats["test"].Completed != 0 {
		t.Errorf("Expected 0 completed tasks, got %d", stats["test"].Completed)
	}
}

// TestSchedulerPanicHandling tests that the scheduler properly handles panics in tasks
func TestSchedulerPanicHandling(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Register a task type
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Create a task that panics
	panicTask := NewTestTask("panic-task", "test", 0, func(ctx context.Context) error {
		panic("intentional panic for testing")
	})

	// Add the task
	scheduler.AddTask(panicTask)

	// Start the scheduler
	scheduler.Start()

	// Wait for task to be processed
	time.Sleep(200 * time.Millisecond)

	// Stop the scheduler
	scheduler.Stop()

	// Check stats
	stats := scheduler.GetStats()

	// The task should have failed and not been retried
	if stats["test"].Failed != 1 {
		t.Errorf("Expected 1 failed task, got %d", stats["test"].Failed)
	}

	if stats["test"].Queued != 0 {
		t.Errorf("Expected 0 queued tasks, got %d", stats["test"].Queued)
	}

	if stats["test"].Completed != 0 {
		t.Errorf("Expected 0 completed tasks, got %d", stats["test"].Completed)
	}
}

// TestSchedulerPanicHandlingWithFailureHandler tests that the scheduler properly handles panics
// when a failure handler is provided
func TestSchedulerPanicHandlingWithFailureHandler(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Track retry attempts
	retryAttempted := false

	// Create a failure handler that records retry attempts
	failureHandler := func(task Task, err error) (time.Time, bool) {
		retryAttempted = true
		return time.Time{}, false // No retry
	}

	// Register a task type with the failure handler
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
		FailureHandler:   failureHandler,
	})

	// Create a task that panics
	panicTask := NewTestTask("panic-task", "test", 0, func(ctx context.Context) error {
		panic("intentional panic for testing")
	})

	// Add the task
	scheduler.AddTask(panicTask)

	// Start the scheduler
	scheduler.Start()

	// Wait for task to be processed
	time.Sleep(200 * time.Millisecond)

	// Stop the scheduler
	scheduler.Stop()

	// Check stats
	stats := scheduler.GetStats()

	// The task should have failed
	if stats["test"].Failed != 1 {
		t.Errorf("Expected 1 failed task, got %d", stats["test"].Failed)
	}

	// The failure handler should have been called
	if !retryAttempted {
		t.Errorf("Expected failure handler to be called")
	}
}

// TestSchedulerPanicHandlingWithRetry tests that the scheduler properly handles panics
// when a failure handler requests a retry
func TestSchedulerPanicHandlingWithRetry(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Track retry attempts
	retryCount := 0

	// Create a failure handler that retries the task
	failureHandler := func(task Task, err error) (time.Time, bool) {
		retryCount++
		return time.Now(), true // Retry immediately
	}

	// Register a task type with the failure handler
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
		FailureHandler:   failureHandler,
	})

	// Create a task that panics
	panicCount := 0
	panicTask := NewTestTask("panic-task", "test", 0, func(ctx context.Context) error {
		panicCount++
		if panicCount == 1 {
			// Only panic on the first attempt
			panic("intentional panic for testing")
		}
		return nil
	})

	// Add the task
	scheduler.AddTask(panicTask)

	// Start the scheduler
	scheduler.Start()

	// Wait for task to be processed and retried
	time.Sleep(500 * time.Millisecond)

	// Stop the scheduler
	scheduler.Stop()

	// Check stats
	stats := scheduler.GetStats()

	// The task should have failed once and then completed
	if stats["test"].Failed != 1 {
		t.Errorf("Expected 1 failed task, got %d", stats["test"].Failed)
	}

	if stats["test"].Completed != 1 {
		t.Errorf("Expected 1 completed task, got %d", stats["test"].Completed)
	}

	// The failure handler should have been called once
	if retryCount != 1 {
		t.Errorf("Expected failure handler to be called once, got %d", retryCount)
	}

	// The task should have panicked once and then succeeded
	if panicCount != 2 {
		t.Errorf("Expected task to be executed twice, got %d", panicCount)
	}
}
