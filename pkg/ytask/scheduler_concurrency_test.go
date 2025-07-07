package ytask

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestGlobalConcurrencyLimitTaskRequeue tests that tasks are put back in the queue
// when the global concurrency limit is reached
func TestGlobalConcurrencyLimitTaskRequeue(t *testing.T) {
	// Create a scheduler with max 1 concurrent task
	scheduler := NewScheduler(1)

	// Register a task type
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   5, // Higher than global limit
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Create a mutex to protect concurrent access to counters
	var mu sync.Mutex

	// Track execution
	executedTasks := make(map[string]bool)

	// Create a channel to signal when the first task starts executing
	firstTaskStarted := make(chan struct{})

	// Create a channel to signal when to allow the first task to complete
	allowFirstTaskToComplete := make(chan struct{})

	// Create a wait group to wait for all tasks
	var wg sync.WaitGroup
	wg.Add(2) // 2 tasks

	// Create a task function for the first task that blocks until signaled
	task1Fn := func(ctx context.Context) error {
		mu.Lock()
		executedTasks["task1"] = true
		mu.Unlock()

		// Signal that the first task has started
		close(firstTaskStarted)

		// Wait until signaled to complete
		<-allowFirstTaskToComplete

		wg.Done()
		return nil
	}

	// Create a task function for the second task
	task2Fn := func(ctx context.Context) error {
		mu.Lock()
		executedTasks["task2"] = true
		mu.Unlock()

		wg.Done()
		return nil
	}

	// Create the tasks
	task1 := NewTestTask("task1", "test", 0, task1Fn)
	task2 := NewTestTask("task2", "test", 0, task2Fn)

	// Add the tasks
	scheduler.AddTask(task1)
	scheduler.AddTask(task2)

	// Start the scheduler
	scheduler.Start()

	// Wait for the first task to start
	<-firstTaskStarted

	// Force a call to processTasks to try to start the second task
	// This should trigger the code path where the task is put back in the queue
	scheduler.processTasks()

	// Allow the first task to complete
	close(allowFirstTaskToComplete)

	// Wait for all tasks to complete
	wg.Wait()

	// Stop the scheduler
	scheduler.Stop()

	// Check that both tasks were executed
	mu.Lock()
	if !executedTasks["task1"] {
		t.Errorf("Expected task1 to be executed")
	}
	if !executedTasks["task2"] {
		t.Errorf("Expected task2 to be executed")
	}
	mu.Unlock()
}

// TestGlobalConcurrencyLimitCheck tests that the global concurrency limit is checked
// before starting each task in the processTasks method
func TestGlobalConcurrencyLimitCheck(t *testing.T) {
	// Create a scheduler with max 2 concurrent tasks
	scheduler := NewScheduler(2)

	// Register two task types
	scheduler.RegisterTaskType("typeA", TaskTypeConfig{
		MaxConcurrency:   5, // Higher than global limit
		PriorityLevels:   1,
		PriorityMode:     PriorityModeStrict,
		FilterDuplicates: false,
	})

	scheduler.RegisterTaskType("typeB", TaskTypeConfig{
		MaxConcurrency:   5, // Higher than global limit
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

	// Create a long-running task function for type A
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

		// Simulate long-running work
		time.Sleep(300 * time.Millisecond)

		mu.Lock()
		concurrentA--
		totalConcurrent--
		mu.Unlock()

		wg.Done()
		return nil
	}

	// Create a long-running task function for type B
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

		// Simulate long-running work
		time.Sleep(300 * time.Millisecond)

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
		t.Errorf("Type A exceeded global concurrency limit: %d > 2", maxConcurrentA)
	}

	if maxConcurrentB > 2 {
		t.Errorf("Type B exceeded global concurrency limit: %d > 2", maxConcurrentB)
	}

	if maxTotalConcurrent > 2 {
		t.Errorf("Total concurrency exceeded global limit: %d > 2", maxTotalConcurrent)
	}
}

// TestTaskTypeMaxConcurrency tests that the task type max concurrency is respected
func TestTaskTypeMaxConcurrency(t *testing.T) {
	// Create a scheduler with high global concurrency
	scheduler := NewScheduler(10)

	// Register a task type with low max concurrency
	scheduler.RegisterTaskType("test", TaskTypeConfig{
		MaxConcurrency:   2, // Only 2 concurrent tasks
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

	// Add 5 tasks
	for i := 0; i < 5; i++ {
		task := NewTestTask(f("task-%d", i), "test", 0, taskFn)
		scheduler.AddTask(task)
	}

	// Start the scheduler
	scheduler.Start()

	// Wait for all tasks to complete
	wg.Wait()

	// Stop the scheduler
	scheduler.Stop()

	// Check concurrency limit
	if maxConcurrent > 2 {
		t.Errorf("Task type exceeded its concurrency limit: %d > 2", maxConcurrent)
	}
}
