package ytask

import (
	"context"
	"testing"
	"time"
)

func TestBaseTask(t *testing.T) {
	// Create a new base task
	task := NewBaseTask("test-id", "test-type", 2)

	// Test GetID
	if task.GetID() != "test-id" {
		t.Errorf("Expected ID to be 'test-id', got '%s'", task.GetID())
	}

	// Test GetType
	if task.GetType() != "test-type" {
		t.Errorf("Expected Type to be 'test-type', got '%s'", task.GetType())
	}

	// Test GetPriority
	if task.GetPriority() != 2 {
		t.Errorf("Expected Priority to be 2, got %d", task.GetPriority())
	}

	// Test GetStatus
	if task.GetStatus() != TaskStatusQueued {
		t.Errorf("Expected Status to be '%s', got '%s'", TaskStatusQueued, task.GetStatus())
	}

	// Test SetStatus
	task.SetStatus(TaskStatusExecuting)
	if task.GetStatus() != TaskStatusExecuting {
		t.Errorf("Expected Status to be '%s', got '%s'", TaskStatusExecuting, task.GetStatus())
	}

	// Test GetFailureCount
	if task.GetFailureCount() != 0 {
		t.Errorf("Expected FailureCount to be 0, got %d", task.GetFailureCount())
	}

	// Test IncrementFailureCount
	task.IncrementFailureCount()
	if task.GetFailureCount() != 1 {
		t.Errorf("Expected FailureCount to be 1, got %d", task.GetFailureCount())
	}

	// Test GetNextExecutionTime and SetNextExecutionTime
	nextTime := time.Now().Add(5 * time.Minute)
	task.SetNextExecutionTime(nextTime)
	if !task.GetNextExecutionTime().Equal(nextTime) {
		t.Errorf("Expected NextExecutionTime to be %v, got %v", nextTime, task.GetNextExecutionTime())
	}

	// Test IsDuplicate
	task2 := NewBaseTask("test-id", "test-type", 1)
	if !task.IsDuplicate(task2) {
		t.Errorf("Expected tasks with same ID to be duplicates")
	}

	task3 := NewBaseTask("different-id", "test-type", 1)
	if task.IsDuplicate(task3) {
		t.Errorf("Expected tasks with different IDs not to be duplicates")
	}
}

func TestExampleTask(t *testing.T) {
	// Create a new example task
	task := NewExampleTask("test-id", 2, "test-data", 100*time.Millisecond, false)

	// Test inheritance from BaseTask
	if task.GetID() != "test-id" {
		t.Errorf("Expected ID to be 'test-id', got '%s'", task.GetID())
	}

	if task.GetType() != "example" {
		t.Errorf("Expected Type to be 'example', got '%s'", task.GetType())
	}

	if task.GetPriority() != 2 {
		t.Errorf("Expected Priority to be 2, got %d", task.GetPriority())
	}

	// Test Execute with success
	ctx := context.Background()
	err := task.Execute(ctx)
	if err != nil {
		t.Errorf("Expected successful execution, got error: %v", err)
	}

	// Test Execute with failure
	failTask := NewExampleTask("fail-id", 2, "test-data", 100*time.Millisecond, true)
	err = failTask.Execute(ctx)
	if err == nil {
		t.Errorf("Expected task to fail, but it succeeded")
	}

	// Test Execute with cancelled context
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately
	err = task.Execute(cancelCtx)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got: %v", err)
	}

	// Test IsDuplicate
	task2 := NewExampleTask("test-id", 1, "test-data", 200*time.Millisecond, false)
	if !task.IsDuplicate(task2) {
		t.Errorf("Expected tasks with same ID and Data to be duplicates")
	}

	task3 := NewExampleTask("test-id", 1, "different-data", 200*time.Millisecond, false)
	if task.IsDuplicate(task3) {
		t.Errorf("Expected tasks with same ID but different Data not to be duplicates")
	}

	// Test IsDuplicate with different task type
	baseTask := NewBaseTask("test-id", "base", 1)
	if task.IsDuplicate(baseTask) {
		t.Errorf("Expected tasks of different types not to be duplicates")
	}
}

func TestExampleMiddleware(t *testing.T) {
	// Create a simple task execution function
	executed := false
	execFunc := func(ctx context.Context, task Task) error {
		executed = true
		return nil
	}

	// Apply middleware
	wrappedFunc := ExampleMiddleware(execFunc)

	// Create a task
	task := NewBaseTask("test-id", "test-type", 1)

	// Execute the wrapped function
	err := wrappedFunc(context.Background(), task)

	// Check that the original function was executed
	if !executed {
		t.Errorf("Expected middleware to call the next function")
	}

	// Check that no error was returned
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
}

func TestExampleFailureHandler(t *testing.T) {
	// Create a task
	task := NewBaseTask("test-id", "test-type", 1)

	// Test first failure
	nextTime, retry := ExampleFailureHandler(task, context.DeadlineExceeded)
	if !retry {
		t.Errorf("Expected handler to retry after first failure")
	}

	expectedBackoff := 1 * time.Second
	expectedTime := time.Now().Add(expectedBackoff)
	timeDiff := nextTime.Sub(expectedTime)
	if timeDiff < -100*time.Millisecond || timeDiff > 100*time.Millisecond {
		t.Errorf("Expected next execution time to be around %v, got %v", expectedTime, nextTime)
	}

	// Test multiple failures
	for i := 0; i < 3; i++ {
		task.IncrementFailureCount()
	}

	// Now failure count is 3, next retry should be the last one
	nextTime, retry = ExampleFailureHandler(task, context.DeadlineExceeded)
	if !retry {
		t.Errorf("Expected handler to retry after 3 failures")
	}

	// Increment one more time to reach the limit
	task.IncrementFailureCount()

	// Now failure count is 4, should not retry
	_, retry = ExampleFailureHandler(task, context.DeadlineExceeded)
	if retry {
		t.Errorf("Expected handler not to retry after 4 failures")
	}
}
