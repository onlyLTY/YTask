package main

import (
	"context"
	"fmt"
	"time"

	"github.com/onlyLTY/YTask/pkg/ytask"
)

// EmailTask is a custom task type for sending emails
type EmailTask struct {
	ytask.BaseTask
	To      string
	Subject string
	Body    string
}

// NewEmailTask creates a new email task
func NewEmailTask(id string, priority int, to, subject, body string) *EmailTask {
	baseTask := ytask.NewBaseTask(id, "email", priority)
	return &EmailTask{
		BaseTask: *baseTask,
		To:       to,
		Subject:  subject,
		Body:     body,
	}
}

// Execute runs the email task
func (t *EmailTask) Execute(ctx context.Context) error {
	// In a real application, this would send an actual email
	fmt.Printf("Sending email to %s with subject '%s'\n", t.To, t.Subject)

	// Simulate work
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("Email sent to %s\n", t.To)
	return nil
}

// DataProcessingTask is a custom task type for processing data
type DataProcessingTask struct {
	ytask.BaseTask
	DataSize int // Size of data to process in MB
}

// NewDataProcessingTask creates a new data processing task
func NewDataProcessingTask(id string, priority int, dataSize int) *DataProcessingTask {
	baseTask := ytask.NewBaseTask(id, "data-processing", priority)
	return &DataProcessingTask{
		BaseTask: *baseTask,
		DataSize: dataSize,
	}
}

// Execute runs the data processing task
func (t *DataProcessingTask) Execute(ctx context.Context) error {
	fmt.Printf("Processing %d MB of data\n", t.DataSize)

	// Simulate work with potential for cancellation
	for i := 0; i < t.DataSize; i += 10 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			fmt.Printf("Processed %d/%d MB\n", i+10, t.DataSize)
		}
	}

	return nil
}

// LoggingMiddleware is a middleware that logs task execution
func LoggingMiddleware(next func(ctx context.Context, task ytask.Task) error) func(ctx context.Context, task ytask.Task) error {
	return func(ctx context.Context, task ytask.Task) error {
		fmt.Printf("[%s] Starting task %s (type: %s, priority: %d)\n",
			time.Now().Format(time.RFC3339), task.GetID(), task.GetType(), task.GetPriority())

		startTime := time.Now()
		err := next(ctx, task)
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("[%s] Task %s failed after %v: %v\n",
				time.Now().Format(time.RFC3339), task.GetID(), duration, err)
		} else {
			fmt.Printf("[%s] Task %s completed successfully in %v\n",
				time.Now().Format(time.RFC3339), task.GetID(), duration)
		}

		return err
	}
}

// EmailFailureHandler handles failures for email tasks
func EmailFailureHandler(task ytask.Task, err error) (time.Time, bool) {
	failureCount := task.GetFailureCount()

	// Maximum number of retries
	if failureCount > 3 {
		fmt.Printf("Email task %s has failed %d times, giving up\n", task.GetID(), failureCount)
		return time.Time{}, false
	}

	// Calculate backoff duration (exponential)
	backoff := time.Duration(1<<uint(failureCount)) * time.Second
	nextTime := time.Now().Add(backoff)

	fmt.Printf("Email task %s failed with error: %v. Retry #%d scheduled in %v\n",
		task.GetID(), err, failureCount, backoff)

	return nextTime, true
}

func main() {
	// Create a scheduler with max 5 concurrent tasks
	scheduler := ytask.NewScheduler(5)

	// Add logging middleware
	scheduler.Use(LoggingMiddleware)

	// Register email task type with 3 priority levels
	scheduler.RegisterTaskType("email", ytask.TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   3,
		PriorityMode:     ytask.PriorityModeStrict,
		FilterDuplicates: true,
		FailureHandler:   EmailFailureHandler,
	})

	// Register data processing task type with 5 priority levels
	scheduler.RegisterTaskType("data-processing", ytask.TaskTypeConfig{
		MaxConcurrency:      3,
		PriorityLevels:      5,
		PriorityMode:        ytask.PriorityModePercentage,
		PriorityPercentages: []int{10, 15, 20, 25, 30}, // Higher priority gets higher percentage
		FilterDuplicates:    false,
	})

	// Create a resource monitor
	monitor := ytask.NewResourceMonitor(scheduler, 80.0, 80.0)
	monitor.SetMinConcurrency(1)

	// Start the scheduler and resource monitor
	scheduler.Start()
	monitor.Start()

	// Add some email tasks
	for i := 0; i < 5; i++ {
		priority := i % 3 // 0, 1, 2
		task := NewEmailTask(
			fmt.Sprintf("email-%d", i),
			priority,
			fmt.Sprintf("user%d@example.com", i),
			fmt.Sprintf("Test Email %d", i),
			"This is a test email",
		)
		scheduler.AddTask(task)
	}

	// Add some data processing tasks
	for i := 0; i < 3; i++ {
		priority := i % 5 // 0, 1, 2, 3, 4
		task := NewDataProcessingTask(
			fmt.Sprintf("data-%d", i),
			priority,
			(i+1)*50, // 50, 100, 150 MB
		)
		scheduler.AddTask(task)
	}

	// Print initial stats
	fmt.Println("\nInitial task statistics:")
	printStats(scheduler)

	// Wait for tasks to start executing
	time.Sleep(1 * time.Second)

	// Print stats during execution
	fmt.Println("\nTask statistics during execution:")
	printStats(scheduler)

	// Cancel a specific task (if it's still in the queue)
	scheduler.CancelTask("email-4")

	// Pause email tasks
	fmt.Println("\nPausing email tasks...")
	scheduler.PauseTaskType("email")

	// Wait for remaining tasks to complete
	time.Sleep(10 * time.Second)

	// Resume email tasks
	fmt.Println("\nResuming email tasks...")
	scheduler.ResumeTaskType("email")

	// Wait for all tasks to complete
	time.Sleep(5 * time.Second)

	// Print final stats
	fmt.Println("\nFinal task statistics:")
	printStats(scheduler)

	// Stop the scheduler and resource monitor
	monitor.Stop()
	scheduler.Stop()

	fmt.Println("\nAll tasks completed. Scheduler stopped.")
}

// printStats prints the current task statistics
func printStats(scheduler *ytask.Scheduler) {
	stats := scheduler.GetStats()
	globalStats := scheduler.GetGlobalStats()

	fmt.Println("Global stats:")
	fmt.Printf("  Queued: %d, Executing: %d, Completed: %d, Failed: %d, Cancelled: %d\n",
		globalStats.Queued, globalStats.Executing, globalStats.Completed,
		globalStats.Failed, globalStats.Cancelled)

	fmt.Println("Stats by task type:")
	for taskType, typeStat := range stats {
		fmt.Printf("  %s: Queued: %d, Executing: %d, Completed: %d, Failed: %d, Cancelled: %d\n",
			taskType, typeStat.Queued, typeStat.Executing, typeStat.Completed,
			typeStat.Failed, typeStat.Cancelled)
	}
}
