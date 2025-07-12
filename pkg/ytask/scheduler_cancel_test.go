package ytask

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestCancelTaskFunction(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Register a task type
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   3,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: true,
	})

	// Add a task
	task := NewTestTask("task1", "test", 1, func(ctx context.Context) error {
		return nil
	})

	success := scheduler.AddTask(task)
	if !success {
		t.Errorf("Failed to add task")
	}

	// Check stats before cancellation
	stats := scheduler.GetStats()
	if stats["test"].Queued != 1 {
		t.Errorf("Expected 1 queued task, got %d", stats["test"].Queued)
	}
	if stats["test"].Cancelled != 0 {
		t.Errorf("Expected 0 cancelled tasks, got %d", stats["test"].Cancelled)
	}

	// Cancel the task
	cancelled := scheduler.CancelTask("task1")
	if !cancelled {
		t.Errorf("Failed to cancel task")
	}

	// Check stats after cancellation
	stats = scheduler.GetStats()
	if stats["test"].Queued != 0 {
		t.Errorf("Expected 0 queued tasks, got %d", stats["test"].Queued)
	}
	if stats["test"].Cancelled != 1 {
		t.Errorf("Expected 1 cancelled task, got %d", stats["test"].Cancelled)
	}

	// Try to cancel a non-existent task
	cancelled = scheduler.CancelTask("non-existent")
	if cancelled {
		t.Errorf("Unexpectedly cancelled a non-existent task")
	}
}

func TestCancelTasksByType(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Register task types
	scheduler.RegisterTaskType("type1", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   3,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	scheduler.RegisterTaskType("type2", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   3,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Add tasks of type1
	for i := 0; i < 5; i++ {
		task := NewTestTask(f("type1-task%d", i), "type1", i%3, func(ctx context.Context) error {
			return nil
		})
		scheduler.AddTask(task)
	}

	// Add tasks of type2
	for i := 0; i < 3; i++ {
		task := NewTestTask(f("type2-task%d", i), "type2", i%3, func(ctx context.Context) error {
			return nil
		})
		scheduler.AddTask(task)
	}

	// Check stats before cancellation
	stats := scheduler.GetStats()
	if stats["type1"].Queued != 5 {
		t.Errorf("Expected 5 queued tasks for type1, got %d", stats["type1"].Queued)
	}
	if stats["type2"].Queued != 3 {
		t.Errorf("Expected 3 queued tasks for type2, got %d", stats["type2"].Queued)
	}

	// Cancel tasks of type1
	cancelledCount := scheduler.CancelTasksByType("type1")
	if cancelledCount != 5 {
		t.Errorf("Expected to cancel 5 tasks, got %d", cancelledCount)
	}

	// Check stats after cancellation
	stats = scheduler.GetStats()
	if stats["type1"].Queued != 0 {
		t.Errorf("Expected 0 queued tasks for type1, got %d", stats["type1"].Queued)
	}
	if stats["type1"].Cancelled != 5 {
		t.Errorf("Expected 5 cancelled tasks for type1, got %d", stats["type1"].Cancelled)
	}
	if stats["type2"].Queued != 3 {
		t.Errorf("Expected 3 queued tasks for type2, got %d", stats["type2"].Queued)
	}
	if stats["type2"].Cancelled != 0 {
		t.Errorf("Expected 0 cancelled tasks for type2, got %d", stats["type2"].Cancelled)
	}

	// Try to cancel tasks of a non-existent type
	cancelledCount = scheduler.CancelTasksByType("non-existent")
	if cancelledCount != 0 {
		t.Errorf("Unexpectedly cancelled tasks of a non-existent type")
	}
}

func TestClearTasksByType(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Register task types
	scheduler.RegisterTaskType("type1", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   3,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	scheduler.RegisterTaskType("type2", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   3,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Add tasks of type1
	for i := 0; i < 5; i++ {
		task := NewTestTask(f("type1-task%d", i), "type1", i%3, func(ctx context.Context) error {
			return nil
		})
		scheduler.AddTask(task)
	}

	// Add tasks of type2
	for i := 0; i < 3; i++ {
		task := NewTestTask(f("type2-task%d", i), "type2", i%3, func(ctx context.Context) error {
			return nil
		})
		scheduler.AddTask(task)
	}

	// Manually update stats to simulate tasks in different states
	// This is to test that all counters are reset properly
	s := scheduler.stats["type1"]
	atomic.AddInt64(&s.Executing, 2)
	atomic.AddInt64(&s.Completed, 3)
	atomic.AddInt64(&s.Failed, 1)
	atomic.AddInt64(&s.Paused, 1)
	atomic.AddInt64(&s.Cancelled, 2)

	// Also update global stats
	atomic.AddInt64(&scheduler.globalStats.Executing, 2)
	atomic.AddInt64(&scheduler.globalStats.Completed, 3)
	atomic.AddInt64(&scheduler.globalStats.Failed, 1)
	atomic.AddInt64(&scheduler.globalStats.Paused, 1)
	atomic.AddInt64(&scheduler.globalStats.Cancelled, 2)

	// Check stats before clearing
	stats := scheduler.GetStats()
	if stats["type1"].Queued != 5 {
		t.Errorf("Expected 5 queued tasks for type1, got %d", stats["type1"].Queued)
	}
	if stats["type1"].Executing != 2 {
		t.Errorf("Expected 2 executing tasks for type1, got %d", stats["type1"].Executing)
	}
	if stats["type1"].Completed != 3 {
		t.Errorf("Expected 3 completed tasks for type1, got %d", stats["type1"].Completed)
	}
	if stats["type1"].Failed != 1 {
		t.Errorf("Expected 1 failed task for type1, got %d", stats["type1"].Failed)
	}
	if stats["type1"].Paused != 1 {
		t.Errorf("Expected 1 paused task for type1, got %d", stats["type1"].Paused)
	}
	if stats["type1"].Cancelled != 2 {
		t.Errorf("Expected 2 cancelled tasks for type1, got %d", stats["type1"].Cancelled)
	}

	// Check global stats before clearing
	globalStats := scheduler.GetGlobalStats()
	initialGlobalExecuting := globalStats.Executing
	initialGlobalCompleted := globalStats.Completed
	initialGlobalFailed := globalStats.Failed
	initialGlobalPaused := globalStats.Paused
	initialGlobalCancelled := globalStats.Cancelled
	initialGlobalQueued := globalStats.Queued

	// Clear tasks of type1
	clearedCount := scheduler.ClearTasksByType("type1")
	if clearedCount != 5 {
		t.Errorf("Expected to clear 5 tasks, got %d", clearedCount)
	}

	// Check stats after clearing - all counters should be reset to 0
	stats = scheduler.GetStats()
	if stats["type1"].Queued != 0 {
		t.Errorf("Expected 0 queued tasks for type1, got %d", stats["type1"].Queued)
	}
	if stats["type1"].Executing != 0 {
		t.Errorf("Expected 0 executing tasks for type1, got %d", stats["type1"].Executing)
	}
	if stats["type1"].Completed != 0 {
		t.Errorf("Expected 0 completed tasks for type1, got %d", stats["type1"].Completed)
	}
	if stats["type1"].Failed != 0 {
		t.Errorf("Expected 0 failed tasks for type1, got %d", stats["type1"].Failed)
	}
	if stats["type1"].Paused != 0 {
		t.Errorf("Expected 0 paused tasks for type1, got %d", stats["type1"].Paused)
	}
	if stats["type1"].Cancelled != 0 {
		t.Errorf("Expected 0 cancelled tasks for type1, got %d", stats["type1"].Cancelled)
	}

	// Check that type2 stats are not affected
	if stats["type2"].Queued != 3 {
		t.Errorf("Expected 3 queued tasks for type2, got %d", stats["type2"].Queued)
	}

	// Check global stats after clearing
	globalStats = scheduler.GetGlobalStats()
	if globalStats.Executing != initialGlobalExecuting-2 {
		t.Errorf("Expected global executing count to decrease by 2, got %d (was %d)",
			globalStats.Executing, initialGlobalExecuting)
	}
	if globalStats.Completed != initialGlobalCompleted-3 {
		t.Errorf("Expected global completed count to decrease by 3, got %d (was %d)",
			globalStats.Completed, initialGlobalCompleted)
	}
	if globalStats.Failed != initialGlobalFailed-1 {
		t.Errorf("Expected global failed count to decrease by 1, got %d (was %d)",
			globalStats.Failed, initialGlobalFailed)
	}
	if globalStats.Paused != initialGlobalPaused-1 {
		t.Errorf("Expected global paused count to decrease by 1, got %d (was %d)",
			globalStats.Paused, initialGlobalPaused)
	}
	if globalStats.Cancelled != initialGlobalCancelled-2 {
		t.Errorf("Expected global cancelled count to decrease by 2, got %d (was %d)",
			globalStats.Cancelled, initialGlobalCancelled)
	}
	if globalStats.Queued != initialGlobalQueued-5 {
		t.Errorf("Expected global queued count to decrease by 5, got %d (was %d)",
			globalStats.Queued, initialGlobalQueued)
	}

	// Try to clear tasks of a non-existent type
	clearedCount = scheduler.ClearTasksByType("non-existent")
	if clearedCount != 0 {
		t.Errorf("Unexpectedly cleared tasks of a non-existent type")
	}
}

// Test cancelling tasks across different priority levels
func TestCancelTasksByTypeWithPriorities(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Register a task type with multiple priority levels
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   3,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Add tasks with different priorities
	for i := 0; i < 3; i++ {
		for j := 0; j < 2; j++ {
			task := NewTestTask(f("task-p%d-%d", i, j), "test", i, func(ctx context.Context) error {
				return nil
			})
			scheduler.AddTask(task)
		}
	}

	// Check stats before cancellation
	stats := scheduler.GetStats()
	if stats["test"].Queued != 6 {
		t.Errorf("Expected 6 queued tasks, got %d", stats["test"].Queued)
	}

	// Cancel all tasks
	cancelledCount := scheduler.CancelTasksByType("test")
	if cancelledCount != 6 {
		t.Errorf("Expected to cancel 6 tasks, got %d", cancelledCount)
	}

	// Check stats after cancellation
	stats = scheduler.GetStats()
	if stats["test"].Queued != 0 {
		t.Errorf("Expected 0 queued tasks, got %d", stats["test"].Queued)
	}
	if stats["test"].Cancelled != 6 {
		t.Errorf("Expected 6 cancelled tasks, got %d", stats["test"].Cancelled)
	}
}

// Test clearing tasks across different priority levels
func TestClearTasksByTypeWithPriorities(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Register a task type with multiple priority levels
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   3,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Add tasks with different priorities
	for i := 0; i < 3; i++ {
		for j := 0; j < 2; j++ {
			task := NewTestTask(f("task-p%d-%d", i, j), "test", i, func(ctx context.Context) error {
				return nil
			})
			scheduler.AddTask(task)
		}
	}

	// Check stats before clearing
	stats := scheduler.GetStats()
	if stats["test"].Queued != 6 {
		t.Errorf("Expected 6 queued tasks, got %d", stats["test"].Queued)
	}

	// Clear all tasks
	clearedCount := scheduler.ClearTasksByType("test")
	if clearedCount != 6 {
		t.Errorf("Expected to clear 6 tasks, got %d", clearedCount)
	}

	// Check stats after clearing
	stats = scheduler.GetStats()
	if stats["test"].Queued != 0 {
		t.Errorf("Expected 0 queued tasks, got %d", stats["test"].Queued)
	}
	if stats["test"].Cancelled != 0 {
		t.Errorf("Expected 0 cancelled tasks, got %d", stats["test"].Cancelled)
	}
}

// Test clearing tasks while some are executing
func TestClearTasksByTypeWithExecutingTasks(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(2) // Limit concurrency to ensure some tasks remain queued

	// Register a task type
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   1, // Only one task can execute at a time
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Channel to signal when a task starts executing
	executing := make(chan struct{}, 1)
	taskCompleted := make(chan struct{}, 1)

	// Add tasks that will take some time to execute
	for i := 0; i < 5; i++ {
		task := NewTestTask(f("task-%d", i), "test", 0, func(ctx context.Context) error {
			// Signal that the task is executing
			executing <- struct{}{}

			// Wait a bit to simulate work
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-taskCompleted:
				return nil
			}
		})
		scheduler.AddTask(task)
	}

	// Start the scheduler
	scheduler.Start()
	defer scheduler.Stop()

	// Wait for at least one task to start executing
	<-executing

	// Check stats before clearing - should have 1 executing and 4 queued
	stats := scheduler.GetStats()
	if stats["test"].Executing != 1 {
		t.Errorf("Expected 1 executing task, got %d", stats["test"].Executing)
	}
	if stats["test"].Queued != 4 {
		t.Errorf("Expected 4 queued tasks, got %d", stats["test"].Queued)
	}

	// Clear all tasks
	clearedCount := scheduler.ClearTasksByType("test")

	// We should have cleared 4 queued tasks
	if clearedCount != 4 {
		t.Errorf("Expected to clear 4 tasks, got %d", clearedCount)
	}

	// Check stats after clearing - all counters should be reset to 0
	stats = scheduler.GetStats()
	if stats["test"].Queued != 0 {
		t.Errorf("Expected 0 queued tasks, got %d", stats["test"].Queued)
	}
	if stats["test"].Executing != 0 {
		t.Errorf("Expected 0 executing tasks, got %d", stats["test"].Executing)
	}
	if stats["test"].Completed != 0 {
		t.Errorf("Expected 0 completed tasks, got %d", stats["test"].Completed)
	}
	if stats["test"].Failed != 0 {
		t.Errorf("Expected 0 failed tasks, got %d", stats["test"].Failed)
	}
	if stats["test"].Paused != 0 {
		t.Errorf("Expected 0 paused tasks, got %d", stats["test"].Paused)
	}
	if stats["test"].Cancelled != 0 {
		t.Errorf("Expected 0 cancelled tasks, got %d", stats["test"].Cancelled)
	}

	// Allow the executing task to complete
	taskCompleted <- struct{}{}
}
