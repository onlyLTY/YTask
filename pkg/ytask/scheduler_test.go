package ytask

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

// TestTask is a simple task implementation for testing
type TestTask struct {
	BaseTask
	ExecuteFn func(ctx context.Context) error
}

// NewTestTask creates a new test task
func NewTestTask(id string, taskType string, priority int, executeFn func(ctx context.Context) error) *TestTask {
	baseTask := NewBaseTask(id, taskType, priority)
	return &TestTask{
		BaseTask:  *baseTask,
		ExecuteFn: executeFn,
	}
}

// Execute runs the test task
func (t *TestTask) Execute(ctx context.Context) error {
	if t.ExecuteFn != nil {
		return t.ExecuteFn(ctx)
	}
	return nil
}

func TestSchedulerBasic(t *testing.T) {
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

	// Check stats
	stats := scheduler.GetStats()
	if stats["test"].Queued != 1 {
		t.Errorf("Expected 1 queued task, got %d", stats["test"].Queued)
	}

	// Start the scheduler
	scheduler.Start()

	// Wait for task to complete
	time.Sleep(200 * time.Millisecond)

	// Check stats again
	stats = scheduler.GetStats()
	if stats["test"].Completed != 1 {
		t.Errorf("Expected 1 completed task, got %d", stats["test"].Completed)
	}

	// Stop the scheduler
	scheduler.Stop()
}

func TestSchedulerPriorities(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Register a task type with strict priorities
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   1, // Only one task at a time to test priorities
		PriorityLevels:   3,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Create a channel to track execution order
	executionOrder := make(chan int, 3)

	// Add tasks with different priorities
	lowPriorityTask := NewTestTask("low", "test", 0, func(ctx context.Context) error {
		executionOrder <- 0
		return nil
	})

	mediumPriorityTask := NewTestTask("medium", "test", 1, func(ctx context.Context) error {
		executionOrder <- 1
		return nil
	})

	highPriorityTask := NewTestTask("high", "test", 2, func(ctx context.Context) error {
		executionOrder <- 2
		return nil
	})

	// Add tasks in reverse priority order
	scheduler.AddTask(lowPriorityTask)
	scheduler.AddTask(mediumPriorityTask)
	scheduler.AddTask(highPriorityTask)

	// Start the scheduler
	scheduler.Start()

	// Wait for tasks to complete and collect execution order
	var order []int
	for i := 0; i < 3; i++ {
		select {
		case p := <-executionOrder:
			order = append(order, p)
		case <-time.After(1 * time.Second):
			t.Fatalf("Timed out waiting for tasks to execute")
		}
	}

	// Stop the scheduler
	scheduler.Stop()

	// Check that tasks were executed in priority order (highest first)
	if len(order) != 3 {
		t.Fatalf("Expected 3 tasks to execute, got %d", len(order))
	}

	if order[0] != 2 || order[1] != 1 || order[2] != 0 {
		t.Errorf("Tasks executed in wrong order. Expected [2,1,0], got %v", order)
	}
}

func TestSchedulerConcurrency(t *testing.T) {
	// Create a scheduler with max 3 concurrent tasks
	scheduler := NewScheduler(3)

	// Register two task types
	scheduler.RegisterTaskType("typeA", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	scheduler.RegisterTaskType("typeB", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Create a mutex to protect concurrent access to counters
	var mu sync.Mutex

	// Track concurrent execution
	concurrentA := 0
	maxConcurrentA := 0
	concurrentB := 0
	maxConcurrentB := 0
	totalConcurrent := 0
	maxTotalConcurrent := 0

	// Create a wait group to wait for all tasks
	var wg sync.WaitGroup
	wg.Add(8) // 4 tasks of each type

	// Create task function for type A
	taskAFn := func(ctx context.Context) error {
		mu.Lock()
		concurrentA++
		totalConcurrent++
		if concurrentA > maxConcurrentA {
			maxConcurrentA = concurrentA
		}
		if totalConcurrent > maxTotalConcurrent {
			maxTotalConcurrent = totalConcurrent
		}
		mu.Unlock()

		// Simulate work
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		concurrentA--
		totalConcurrent--
		mu.Unlock()

		wg.Done()
		return nil
	}

	// Create task function for type B
	taskBFn := func(ctx context.Context) error {
		mu.Lock()
		concurrentB++
		totalConcurrent++
		if concurrentB > maxConcurrentB {
			maxConcurrentB = concurrentB
		}
		if totalConcurrent > maxTotalConcurrent {
			maxTotalConcurrent = totalConcurrent
		}
		mu.Unlock()

		// Simulate work
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		concurrentB--
		totalConcurrent--
		mu.Unlock()

		wg.Done()
		return nil
	}

	// Add 4 tasks of each type
	for i := 0; i < 4; i++ {
		taskA := NewTestTask(f("taskA-%d", i), "typeA", 0, taskAFn)
		taskB := NewTestTask(f("taskB-%d", i), "typeB", 0, taskBFn)
		scheduler.AddTask(taskA)
		scheduler.AddTask(taskB)
	}

	// Start the scheduler
	scheduler.Start()

	// Wait for all tasks to complete
	wg.Wait()

	// Stop the scheduler
	scheduler.Stop()

	// Check concurrency limits
	if maxConcurrentA > 2 {
		t.Errorf("Type A exceeded its concurrency limit: %d > 2", maxConcurrentA)
	}

	if maxConcurrentB > 2 {
		t.Errorf("Type B exceeded its concurrency limit: %d > 2", maxConcurrentB)
	}

	if maxTotalConcurrent > 3 {
		t.Errorf("Total concurrency exceeded global limit: %d > 3", maxTotalConcurrent)
	}
}

func TestSchedulerFailureHandling(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Create a failure handler that retries once
	failureHandler := func(task Task, err error) (time.Time, bool) {
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

	// Create a task that fails on first attempt but succeeds on retry
	attemptCount := 0
	task := NewTestTask("failing-task", "test", 0, func(ctx context.Context) error {
		attemptCount++
		if attemptCount == 1 {
			return fmt.Errorf("first attempt failure")
		}
		return nil
	})

	// Add the task
	scheduler.AddTask(task)

	// Start the scheduler
	scheduler.Start()

	// Wait for task to complete (including retry)
	time.Sleep(300 * time.Millisecond)

	// Stop the scheduler
	scheduler.Stop()

	// Check stats
	stats := scheduler.GetStats()
	if stats["test"].Completed != 1 {
		t.Errorf("Expected 1 completed task, got %d", stats["test"].Completed)
	}

	if stats["test"].Failed != 1 {
		t.Errorf("Expected 1 failed attempt, got %d", stats["test"].Failed)
	}

	if attemptCount != 2 {
		t.Errorf("Expected 2 attempts, got %d", attemptCount)
	}
}

func TestSchedulerMiddleware(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Register a task type
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Create a middleware that tracks execution
	middlewareCalled := false
	middleware := func(next func(ctx context.Context, task Task) error) func(ctx context.Context, task Task) error {
		return func(ctx context.Context, task Task) error {
			middlewareCalled = true
			return next(ctx, task)
		}
	}

	// Add the middleware
	scheduler.Use(middleware)

	// Create a task
	task := NewTestTask("task1", "test", 0, func(ctx context.Context) error {
		return nil
	})

	// Add the task
	scheduler.AddTask(task)

	// Start the scheduler
	scheduler.Start()

	// Wait for task to complete
	time.Sleep(200 * time.Millisecond)

	// Stop the scheduler
	scheduler.Stop()

	// Check that middleware was called
	if !middlewareCalled {
		t.Errorf("Middleware was not called")
	}
}

// Helper function to format strings
func f(format string, args ...interface{}) string {
	return fmt.Sprintf(format, args...)
}

// TestPriorityModePercentageImplementation tests that the percentage-based priority mode
// correctly distributes tasks according to the specified percentages
func TestPriorityModePercentageImplementation(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(5)

	// Register a task type with percentage-based priority
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:      5, // Allow multiple concurrent tasks to speed up the test
		PriorityLevels:      3,
		PriorityMode:        PriorityModePercentage,
		PriorityPercentages: []int{20, 30, 50}, // 20% low, 30% medium, 50% high
		FilterDuplicates:    false,
	})

	// Create a mutex to protect concurrent access to counters
	var mu sync.Mutex

	// Track execution counts by priority
	executionCounts := make([]int, 3)

	// Create a wait group to wait for all tasks
	var wg sync.WaitGroup
	totalTasks := 100 // Run 100 tasks to get a statistically significant sample
	wg.Add(totalTasks)

	// Create task functions for different priorities
	createTaskFn := func(priority int) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			mu.Lock()
			executionCounts[priority]++
			mu.Unlock()
			wg.Done()
			return nil
		}
	}

	// Add tasks of each priority
	for i := 0; i < totalTasks; i++ {
		// Add an equal number of tasks for each priority to the queue
		lowTask := NewTestTask(f("low-%d", i), "test", 0, createTaskFn(0))
		mediumTask := NewTestTask(f("medium-%d", i), "test", 1, createTaskFn(1))
		highTask := NewTestTask(f("high-%d", i), "test", 2, createTaskFn(2))

		scheduler.AddTask(lowTask)
		scheduler.AddTask(mediumTask)
		scheduler.AddTask(highTask)

	}

	// Start the scheduler
	scheduler.Start()

	// Wait for all tasks to complete
	wg.Wait()

	// Stop the scheduler
	scheduler.Stop()

	// Calculate the actual percentages
	total := executionCounts[0] + executionCounts[1] + executionCounts[2]
	lowPercent := float64(executionCounts[0]) / float64(total) * 100
	mediumPercent := float64(executionCounts[1]) / float64(total) * 100
	highPercent := float64(executionCounts[2]) / float64(total) * 100

	// Log the results
	t.Logf("Task execution distribution: Low: %d (%.1f%%), Medium: %d (%.1f%%), High: %d (%.1f%%)",
		executionCounts[0], lowPercent,
		executionCounts[1], mediumPercent,
		executionCounts[2], highPercent)

	// Check that the distribution is roughly as expected
	// Allow for some statistical variation (±10%)
	if math.Abs(lowPercent-20) > 10 {
		t.Errorf("Expected low priority tasks to be around 20%%, got %.1f%%", lowPercent)
	}
	if math.Abs(mediumPercent-30) > 10 {
		t.Errorf("Expected medium priority tasks to be around 30%%, got %.1f%%", mediumPercent)
	}
	if math.Abs(highPercent-50) > 10 {
		t.Errorf("Expected high priority tasks to be around 50%%, got %.1f%%", highPercent)
	}
}

// TestGlobalConcurrencyLimit tests that the global concurrency limit is respected
// even when tasks are added after the scheduler has started processing tasks
func TestGlobalConcurrencyLimit(t *testing.T) {
	// Create a scheduler with max 2 concurrent tasks
	scheduler := NewScheduler(2)

	// Register a task type
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   5, // Allow up to 5 concurrent tasks of this type
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Create a mutex to protect concurrent access to counters
	var mu sync.Mutex

	// Track concurrent execution
	concurrent := 0
	maxConcurrent := 0

	// Create a wait group to wait for all tasks
	var wg sync.WaitGroup
	wg.Add(5) // 5 tasks

	// Create a task function that sleeps to ensure concurrency
	taskFn := func(ctx context.Context) error {
		mu.Lock()
		concurrent++
		if concurrent > maxConcurrent {
			maxConcurrent = concurrent
		}
		mu.Unlock()

		// Simulate work
		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		concurrent--
		mu.Unlock()

		wg.Done()
		return nil
	}

	// Add 2 tasks initially
	for i := 0; i < 2; i++ {
		task := NewTestTask(f("task-%d", i), "test", 0, taskFn)
		scheduler.AddTask(task)
	}

	// Start the scheduler
	scheduler.Start()

	// Wait a bit to let the scheduler start processing
	time.Sleep(50 * time.Millisecond)

	// Add 3 more tasks while the scheduler is running
	for i := 2; i < 5; i++ {
		task := NewTestTask(f("task-%d", i), "test", 0, taskFn)
		scheduler.AddTask(task)
	}

	// Wait for all tasks to complete
	wg.Wait()

	// Stop the scheduler
	scheduler.Stop()

	// Check that the global concurrency limit was respected
	if maxConcurrent > 2 {
		t.Errorf("Global concurrency limit was exceeded: %d > 2", maxConcurrent)
	}
}
