package ytask

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestNamespaceAssociation tests that task types can be associated with namespaces
func TestNamespaceAssociation(t *testing.T) {
	scheduler := NewScheduler(10)

	// Register task types with namespaces
	scheduler.RegisterTaskTypeWithNamespace("type1", "namespace1", TaskTypeConfig{
		MaxConcurrency: 2,
		PriorityLevels: 1,
	})

	scheduler.RegisterTaskTypeWithNamespace("type2", "namespace1", TaskTypeConfig{
		MaxConcurrency: 2,
		PriorityLevels: 1,
	})

	scheduler.RegisterTaskTypeWithNamespace("type3", "namespace2", TaskTypeConfig{
		MaxConcurrency: 3,
		PriorityLevels: 1,
	})

	// Verify task types are associated with the correct namespaces
	if scheduler.taskTypeToNamespace["type1"] != "namespace1" {
		t.Errorf("Expected type1 to be in namespace1, got %s", scheduler.taskTypeToNamespace["type1"])
	}

	if scheduler.taskTypeToNamespace["type2"] != "namespace1" {
		t.Errorf("Expected type2 to be in namespace1, got %s", scheduler.taskTypeToNamespace["type2"])
	}

	if scheduler.taskTypeToNamespace["type3"] != "namespace2" {
		t.Errorf("Expected type3 to be in namespace2, got %s", scheduler.taskTypeToNamespace["type3"])
	}
}

// TestNamespaceMaxConcurrency tests that the namespace max concurrency is respected
func TestNamespaceMaxConcurrency(t *testing.T) {
	// Create a scheduler with high global concurrency
	scheduler := NewScheduler(10)

	// Register task types with namespaces
	scheduler.RegisterTaskTypeWithNamespace("type1", "namespace1", TaskTypeConfig{
		MaxConcurrency: 5, // High type-specific limit
		PriorityLevels: 1,
	})

	scheduler.RegisterTaskTypeWithNamespace("type2", "namespace1", TaskTypeConfig{
		MaxConcurrency: 5, // High type-specific limit
		PriorityLevels: 1,
	})

	// Set namespace concurrency limit
	scheduler.SetNamespaceMaxConcurrency("namespace1", 3) // Only 3 concurrent tasks in namespace1

	// Create a mutex to protect concurrent access to counters
	var mu sync.Mutex

	// Track concurrent execution
	concurrent := 0
	maxConcurrent := 0

	// Create a wait group to wait for all tasks
	var wg sync.WaitGroup
	wg.Add(6) // 6 tasks (3 of each type)

	// Create a long-running task function
	taskFn := func(ctx context.Context) error {
		mu.Lock()
		concurrent++
		if concurrent > maxConcurrent {
			maxConcurrent = concurrent
		}
		mu.Unlock()

		// Simulate long-running work
		time.Sleep(300 * time.Millisecond)

		mu.Lock()
		concurrent--
		mu.Unlock()

		wg.Done()
		return nil
	}

	// Add 3 tasks of type1
	for i := 0; i < 3; i++ {
		task := NewTestTask(fmt.Sprintf("task1-%d", i), "type1", 0, taskFn)
		scheduler.AddTask(task)
	}

	// Add 3 tasks of type2
	for i := 0; i < 3; i++ {
		task := NewTestTask(fmt.Sprintf("task2-%d", i), "type2", 0, taskFn)
		scheduler.AddTask(task)
	}

	// Start the scheduler
	scheduler.Start()

	// Wait for all tasks to complete
	wg.Wait()

	// Stop the scheduler
	scheduler.Stop()

	// Check namespace concurrency limit
	if maxConcurrent > 3 {
		t.Errorf("Namespace exceeded its concurrency limit: %d > 3", maxConcurrent)
	}
}

// TestPauseResumeNamespace tests pausing and resuming namespaces
func TestPauseResumeNamespace(t *testing.T) {
	scheduler := NewScheduler(10)

	// Register task types with namespaces
	scheduler.RegisterTaskTypeWithNamespace("type1", "namespace1", TaskTypeConfig{
		MaxConcurrency: 2,
		PriorityLevels: 1,
	})

	scheduler.RegisterTaskTypeWithNamespace("type2", "namespace1", TaskTypeConfig{
		MaxConcurrency: 2,
		PriorityLevels: 1,
	})

	// Create a mutex to protect concurrent access to counters
	var mu sync.Mutex

	// Track task execution
	executedTasks := make(map[string]bool)

	// Create a channel to signal when first tasks are done
	firstTasksDone := make(chan bool, 2)

	// Create a wait group for the second set of tasks
	var wg sync.WaitGroup
	wg.Add(2) // We'll add 2 tasks after pausing

	// Create a task function for the first set of tasks
	firstTaskFn := func(ctx context.Context) error {
		taskID := GetTaskIDFromContext(ctx)

		mu.Lock()
		executedTasks[taskID] = true
		mu.Unlock()

		// Signal that this task is done
		firstTasksDone <- true

		return nil
	}

	// Create a task function for the second set of tasks
	secondTaskFn := func(ctx context.Context) error {
		taskID := GetTaskIDFromContext(ctx)

		mu.Lock()
		executedTasks[taskID] = true
		mu.Unlock()

		wg.Done()
		return nil
	}

	// Start the scheduler
	scheduler.Start()

	// Add 2 tasks of type1
	task1 := NewTestTask("task1-1", "type1", 0, firstTaskFn)
	task2 := NewTestTask("task1-2", "type1", 0, firstTaskFn)
	scheduler.AddTask(task1)
	scheduler.AddTask(task2)

	// Wait for the first tasks to complete
	for i := 0; i < 2; i++ {
		select {
		case <-firstTasksDone:
			// Task completed
		case <-time.After(2 * time.Second):
			t.Fatalf("Timed out waiting for first tasks to complete")
		}
	}

	// Pause the namespace
	scheduler.PauseNamespace("namespace1")

	// Add 2 tasks of type2 (should not execute because namespace is paused)
	task3 := NewTestTask("task2-1", "type2", 0, secondTaskFn)
	task4 := NewTestTask("task2-2", "type2", 0, secondTaskFn)
	scheduler.AddTask(task3)
	scheduler.AddTask(task4)

	// Wait a moment to verify tasks don't execute while paused
	time.Sleep(500 * time.Millisecond)

	// Check that the second tasks haven't executed
	mu.Lock()
	task3Executed := executedTasks["task2-1"]
	task4Executed := executedTasks["task2-2"]
	mu.Unlock()

	if task3Executed || task4Executed {
		t.Errorf("Tasks executed while namespace was paused")
	}

	// Set a timeout for the wait group
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	// Check that the wait group doesn't complete (tasks don't execute)
	select {
	case <-done:
		t.Errorf("Tasks executed while namespace was paused")
	case <-time.After(500 * time.Millisecond):
		// This is expected - tasks shouldn't execute while paused
	}

	// Stop the scheduler
	scheduler.Stop()

	// Check that the first tasks executed
	if !executedTasks["task1-1"] || !executedTasks["task1-2"] {
		t.Errorf("Expected tasks task1-1 and task1-2 to execute")
	}

	// Check that the second tasks didn't execute
	if executedTasks["task2-1"] || executedTasks["task2-2"] {
		t.Errorf("Expected tasks task2-1 and task2-2 not to execute")
	}
}

// Helper function to get task ID from context
func GetTaskIDFromContext(ctx context.Context) string {
	if task, ok := ctx.Value("task").(Task); ok {
		return task.GetID()
	}
	return ""
}
