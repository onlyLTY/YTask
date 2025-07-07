package ytask

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestSchedulerErrorHandlingPaths tests various error handling paths in the scheduler
func TestSchedulerErrorHandlingPaths(t *testing.T) {
	// Test immediate retry with failure handler returning a future time
	t.Run("FutureRetryTime", func(t *testing.T) {
		scheduler := NewScheduler(5)

		// Create a failure handler that schedules retry for the future
		failureHandler := func(task Task, err error) (time.Time, bool) {
			// Use a time far enough in the future to avoid timing issues
			return time.Now().Add(1 * time.Hour), true // Retry in the future
		}

		// Register a task type with the failure handler
		scheduler.RegisterTaskType("test", TaskTypeConfig{
			MaxConcurrency:   2,
			PriorityLevels:   1,
			PriorityMode:     PriorityModeStrict,
			FilterDuplicates: false,
			FailureHandler:   failureHandler,
		})

		// Create a task that always fails
		attemptCount := 0
		task := NewTestTask("failing-task", "test", 0, func(ctx context.Context) error {
			attemptCount++
			return errors.New("task failed")
		})

		// Add the task
		scheduler.AddTask(task)

		// Start the scheduler
		scheduler.Start()

		// Wait for task to be processed
		time.Sleep(200 * time.Millisecond)

		// Stop the scheduler
		scheduler.Stop()

		// Check stats
		stats := scheduler.GetStats()

		// The task should have failed once and been re-queued
		if stats["test"].Failed != 1 {
			t.Errorf("Expected 1 failed task, got %d", stats["test"].Failed)
		}

		if stats["test"].Queued != 1 {
			t.Errorf("Expected 1 queued task (for future retry), got %d", stats["test"].Queued)
		}

		if attemptCount != 1 {
			t.Errorf("Expected 1 attempt, got %d", attemptCount)
		}
	})

	// Test failure handler returning no retry
	t.Run("NoRetry", func(t *testing.T) {
		scheduler := NewScheduler(5)

		// Create a failure handler that doesn't retry
		failureHandler := func(task Task, err error) (time.Time, bool) {
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

		// Create a task that always fails
		attemptCount := 0
		task := NewTestTask("failing-task", "test", 0, func(ctx context.Context) error {
			attemptCount++
			return errors.New("task failed")
		})

		// Add the task
		scheduler.AddTask(task)

		// Start the scheduler
		scheduler.Start()

		// Wait for task to be processed
		time.Sleep(200 * time.Millisecond)

		// Stop the scheduler
		scheduler.Stop()

		// Check stats
		stats := scheduler.GetStats()

		// The task should have failed once and not been re-queued
		if stats["test"].Failed != 1 {
			t.Errorf("Expected 1 failed task, got %d", stats["test"].Failed)
		}

		if stats["test"].Queued != 0 {
			t.Errorf("Expected 0 queued tasks, got %d", stats["test"].Queued)
		}

		if attemptCount != 1 {
			t.Errorf("Expected 1 attempt, got %d", attemptCount)
		}
	})

	// Test task with no failure handler
	t.Run("NoFailureHandler", func(t *testing.T) {
		scheduler := NewScheduler(5)

		// Register a task type with no failure handler
		scheduler.RegisterTaskType("test", TaskTypeConfig{
			MaxConcurrency:   2,
			PriorityLevels:   1,
			PriorityMode:     PriorityModeStrict,
			FilterDuplicates: false,
			// No failure handler
		})

		// Create a task that always fails
		attemptCount := 0
		task := NewTestTask("failing-task", "test", 0, func(ctx context.Context) error {
			attemptCount++
			return errors.New("task failed")
		})

		// Add the task
		scheduler.AddTask(task)

		// Start the scheduler
		scheduler.Start()

		// Wait for task to be processed
		time.Sleep(200 * time.Millisecond)

		// Stop the scheduler
		scheduler.Stop()

		// Check stats
		stats := scheduler.GetStats()

		// The task should have failed once and not been re-queued
		if stats["test"].Failed != 1 {
			t.Errorf("Expected 1 failed task, got %d", stats["test"].Failed)
		}

		if stats["test"].Queued != 0 {
			t.Errorf("Expected 0 queued tasks, got %d", stats["test"].Queued)
		}

		if attemptCount != 1 {
			t.Errorf("Expected 1 attempt, got %d", attemptCount)
		}
	})
}
