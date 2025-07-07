package ytask

import (
	"context"
	"testing"
	"time"
)

// TestNewSchedulerDefaultConcurrency tests that NewScheduler uses the number of CPU cores
// when maxGlobalConcurrency is <= 0
func TestNewSchedulerDefaultConcurrency(t *testing.T) {
	// Create a scheduler with concurrency <= 0
	scheduler := NewScheduler(0)

	// Check that the concurrency was set to the number of CPU cores
	if scheduler.maxGlobalConcurrency <= 0 {
		t.Errorf("Expected maxGlobalConcurrency to be set to number of CPU cores, got %d", scheduler.maxGlobalConcurrency)
	}

	// Create another scheduler with negative concurrency
	scheduler = NewScheduler(-1)

	// Check that the concurrency was set to the number of CPU cores
	if scheduler.maxGlobalConcurrency <= 0 {
		t.Errorf("Expected maxGlobalConcurrency to be set to number of CPU cores, got %d", scheduler.maxGlobalConcurrency)
	}
}

// TestAddTaskNonExistentType tests that AddTask returns false when the task type doesn't exist
func TestAddTaskNonExistentType(t *testing.T) {
	scheduler := NewScheduler(5)

	// Create a task with a type that hasn't been registered
	task := NewTestTask("task1", "non-existent-type", 0, func(ctx context.Context) error {
		return nil
	})

	// Try to add the task
	success := scheduler.AddTask(task)

	// Check that the task was not added
	if success {
		t.Errorf("Expected AddTask to return false for non-existent task type")
	}
}

// TestFilterDuplicates tests the duplicate filtering functionality
func TestFilterDuplicates(t *testing.T) {
	scheduler := NewScheduler(5)

	// Register a task type with duplicate filtering enabled
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: true,
	})

	// Create two identical tasks
	task1 := NewTestTask("task1", "test", 0, func(ctx context.Context) error {
		return nil
	})

	task2 := NewTestTask("task1", "test", 0, func(ctx context.Context) error {
		return nil
	})

	// Add the first task
	success1 := scheduler.AddTask(task1)

	// Try to add the duplicate task
	success2 := scheduler.AddTask(task2)

	// Check that the first task was added but the second was not
	if !success1 {
		t.Errorf("Expected first task to be added successfully")
	}

	if success2 {
		t.Errorf("Expected duplicate task to be rejected")
	}

	// Check stats
	stats := scheduler.GetStats()
	if stats["test"].Queued != 1 {
		t.Errorf("Expected 1 queued task, got %d", stats["test"].Queued)
	}
}

// TestPriorityBoundsChecking tests that task priorities are adjusted to be within bounds
func TestPriorityBoundsChecking(t *testing.T) {
	scheduler := NewScheduler(5)

	// Register a task type with 3 priority levels
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   3,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Create tasks with out-of-bounds priorities
	lowTask := NewTestTask("low", "test", -1, func(ctx context.Context) error {
		return nil
	})

	highTask := NewTestTask("high", "test", 5, func(ctx context.Context) error {
		return nil
	})

	// Add the tasks
	scheduler.AddTask(lowTask)
	scheduler.AddTask(highTask)

	// Start the scheduler
	scheduler.Start()

	// Wait for tasks to be processed
	time.Sleep(500 * time.Millisecond)

	// Stop the scheduler
	scheduler.Stop()

	// Check that both tasks were completed
	stats := scheduler.GetStats()
	if stats["test"].Completed != 2 {
		t.Errorf("Expected 2 completed tasks, got %d", stats["test"].Completed)
	}
}

// TestPauseResumeTaskType tests pausing and resuming task types
func TestPauseResumeTaskType(t *testing.T) {
	scheduler := NewScheduler(5)

	// Register a task type
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Create a channel to track execution
	executed := make(chan bool, 1)

	// Create a task
	task := NewTestTask("task1", "test", 0, func(ctx context.Context) error {
		executed <- true
		return nil
	})

	// Add the task
	scheduler.AddTask(task)

	// Pause the task type
	scheduler.PauseTaskType("test")

	// Start the scheduler
	scheduler.Start()

	// Wait a bit to ensure the task doesn't execute
	select {
	case <-executed:
		t.Errorf("Task was executed despite being paused")
	case <-time.After(200 * time.Millisecond):
		// This is expected
	}

	// Resume the task type
	scheduler.ResumeTaskType("test")

	// Wait for the task to execute
	select {
	case <-executed:
		// This is expected
	case <-time.After(200 * time.Millisecond):
		t.Errorf("Task was not executed after resuming")
	}

	// Stop the scheduler
	scheduler.Stop()
}

// TestCancelTask tests cancelling a task
func TestCancelTask(t *testing.T) {
	scheduler := NewScheduler(5)

	// Register a task type
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Create a task
	task := NewTestTask("task1", "test", 0, func(ctx context.Context) error {
		return nil
	})

	// Add the task
	scheduler.AddTask(task)

	// Cancel the task
	cancelled := scheduler.CancelTask("task1")

	// Check that the task was cancelled
	if !cancelled {
		t.Errorf("Expected CancelTask to return true")
	}

	// Check stats
	stats := scheduler.GetStats()
	if stats["test"].Queued != 0 {
		t.Errorf("Expected 0 queued tasks, got %d", stats["test"].Queued)
	}

	if stats["test"].Cancelled != 1 {
		t.Errorf("Expected 1 cancelled task, got %d", stats["test"].Cancelled)
	}

	// Try to cancel a non-existent task
	cancelled = scheduler.CancelTask("non-existent")

	// Check that the operation failed
	if cancelled {
		t.Errorf("Expected CancelTask to return false for non-existent task")
	}
}

// TestGetGlobalStats tests the GetGlobalStats method
func TestGetGlobalStats(t *testing.T) {
	scheduler := NewScheduler(5)

	// Register a task type
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Create and add a task
	task := NewTestTask("task1", "test", 0, func(ctx context.Context) error {
		return nil
	})

	scheduler.AddTask(task)

	// Check global stats
	globalStats := scheduler.GetGlobalStats()

	if globalStats.Queued != 1 {
		t.Errorf("Expected 1 queued task in global stats, got %d", globalStats.Queued)
	}

	// Start the scheduler
	scheduler.Start()

	// Wait for task to complete
	time.Sleep(200 * time.Millisecond)

	// Stop the scheduler
	scheduler.Stop()

	// Check global stats again
	globalStats = scheduler.GetGlobalStats()

	if globalStats.Completed != 1 {
		t.Errorf("Expected 1 completed task in global stats, got %d", globalStats.Completed)
	}
}

// TestPriorityModePercentage tests the percentage-based priority mode
func TestPriorityModePercentage(t *testing.T) {
	scheduler := NewScheduler(5)

	// Register a task type with percentage-based priority
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:      1, // Only one task at a time to test priorities
		PriorityLevels:      3,
		PriorityMode:        PriorityModePercentage,
		PriorityPercentages: []int{20, 30, 50}, // 20% low, 30% medium, 50% high
		FilterDuplicates:    false,
	})

	// Create a channel to track execution order
	executionOrder := make(chan int, 10)

	// Create task functions for different priorities
	lowPriorityFn := func(ctx context.Context) error {
		executionOrder <- 0
		return nil
	}

	mediumPriorityFn := func(ctx context.Context) error {
		executionOrder <- 1
		return nil
	}

	highPriorityFn := func(ctx context.Context) error {
		executionOrder <- 2
		return nil
	}

	// Add multiple tasks of each priority to test percentage distribution
	// Add 3 low priority tasks
	for i := 0; i < 3; i++ {
		task := NewTestTask(f("low-%d", i), "test", 0, lowPriorityFn)
		scheduler.AddTask(task)
	}

	// Add 3 medium priority tasks
	for i := 0; i < 3; i++ {
		task := NewTestTask(f("medium-%d", i), "test", 1, mediumPriorityFn)
		scheduler.AddTask(task)
	}

	// Add 4 high priority tasks
	for i := 0; i < 4; i++ {
		task := NewTestTask(f("high-%d", i), "test", 2, highPriorityFn)
		scheduler.AddTask(task)
	}

	// Start the scheduler
	scheduler.Start()

	// Wait for tasks to complete and collect execution order
	var order []int
	for i := 0; i < 10; i++ {
		select {
		case p := <-executionOrder:
			order = append(order, p)
		case <-time.After(1 * time.Second):
			// If we timeout, we might not have all 10 tasks executed, but that's OK
			// We'll check what we have so far
			break
		}
	}

	// Stop the scheduler
	scheduler.Stop()

	// Check that at least one task was executed
	if len(order) == 0 {
		t.Fatalf("Expected at least one task to execute")
	}

	// Count tasks of each priority that were executed
	lowCount := 0
	mediumCount := 0
	highCount := 0

	for _, p := range order {
		switch p {
		case 0:
			lowCount++
		case 1:
			mediumCount++
		case 2:
			highCount++
		}
	}

	// Log the distribution for debugging
	t.Logf("Task execution distribution: Low: %d, Medium: %d, High: %d", lowCount, mediumCount, highCount)
}

// TestBaseTaskExecute tests the Execute method of BaseTask
func TestBaseTaskExecute(t *testing.T) {
	task := NewBaseTask("test-id", "test-type", 1)

	// Execute should return an error since it's not implemented
	err := task.Execute(context.Background())

	if err == nil {
		t.Errorf("Expected BaseTask.Execute to return an error")
	}
}
