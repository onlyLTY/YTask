package main

import (
	"context"
	"fmt"
	"reflect"
	"strings"
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

// SMSTask is a custom task type for sending SMS messages
type SMSTask struct {
	ytask.BaseTask
	To      string
	Message string
}

// NewSMSTask creates a new SMS task
func NewSMSTask(id string, priority int, to, message string) *SMSTask {
	baseTask := ytask.NewBaseTask(id, "sms", priority)
	return &SMSTask{
		BaseTask: *baseTask,
		To:       to,
		Message:  message,
	}
}

// Execute runs the SMS task
func (t *SMSTask) Execute(ctx context.Context) error {
	// In a real application, this would send an actual SMS
	fmt.Printf("Sending SMS to %s\n", t.To)

	// Simulate work
	time.Sleep(300 * time.Millisecond)

	fmt.Printf("SMS sent to %s\n", t.To)
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

// ImageProcessingTask is a custom task type for processing images
type ImageProcessingTask struct {
	ytask.BaseTask
	ImageCount int // Number of images to process
}

// NewImageProcessingTask creates a new image processing task
func NewImageProcessingTask(id string, priority int, imageCount int) *ImageProcessingTask {
	baseTask := ytask.NewBaseTask(id, "image-processing", priority)
	return &ImageProcessingTask{
		BaseTask:   *baseTask,
		ImageCount: imageCount,
	}
}

// Execute runs the image processing task
func (t *ImageProcessingTask) Execute(ctx context.Context) error {
	fmt.Printf("Processing %d images\n", t.ImageCount)

	// Simulate work with potential for cancellation
	for i := 0; i < t.ImageCount; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
			fmt.Printf("Processed image %d/%d\n", i+1, t.ImageCount)
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

	// Register email task type with 3 priority levels in "communication" namespace
	scheduler.RegisterTaskTypeWithNamespace("email", "communication", ytask.TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   3,
		PriorityMode:     ytask.PriorityModeStrict,
		FilterDuplicates: true,
		FailureHandler:   EmailFailureHandler,
	})

	// Register SMS task type in the same "communication" namespace
	scheduler.RegisterTaskTypeWithNamespace("sms", "communication", ytask.TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   3,
		PriorityMode:     ytask.PriorityModeStrict,
		FilterDuplicates: true,
	})

	// Register data processing task type with 5 priority levels in "processing" namespace
	scheduler.RegisterTaskTypeWithNamespace("data-processing", "processing", ytask.TaskTypeConfig{
		MaxConcurrency:      3,
		PriorityLevels:      5,
		PriorityMode:        ytask.PriorityModePercentage,
		PriorityPercentages: []int{10, 15, 20, 25, 30}, // Higher priority gets higher percentage
		FilterDuplicates:    false,
	})

	// Register image processing task type in the same "processing" namespace
	scheduler.RegisterTaskTypeWithNamespace("image-processing", "processing", ytask.TaskTypeConfig{
		MaxConcurrency:   2,
		PriorityLevels:   3,
		PriorityMode:     ytask.PriorityModeStrict,
		FilterDuplicates: false,
	})

	// Set namespace concurrency limits
	scheduler.SetNamespaceMaxConcurrency("communication", 3) // Max 3 concurrent tasks in communication namespace
	scheduler.SetNamespaceMaxConcurrency("processing", 4)    // Max 4 concurrent tasks in processing namespace

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

	// Add some SMS tasks (also in communication namespace)
	for i := 0; i < 3; i++ {
		priority := i % 3 // 0, 1, 2
		task := NewSMSTask(
			fmt.Sprintf("sms-%d", i),
			priority,
			fmt.Sprintf("+1555123456%d", i),
			fmt.Sprintf("Test SMS message %d", i),
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

	// Add some image processing tasks (also in processing namespace)
	for i := 0; i < 2; i++ {
		priority := i % 3 // 0, 1
		task := NewImageProcessingTask(
			fmt.Sprintf("image-%d", i),
			priority,
			(i+1)*3, // 3, 6 images
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

	// Wait for a moment
	time.Sleep(2 * time.Second)

	// Print stats after pausing email tasks
	fmt.Println("\nTask statistics after pausing email tasks:")
	printStats(scheduler)

	// Pause the entire communication namespace
	fmt.Println("\nPausing the entire communication namespace...")
	scheduler.PauseNamespace("communication")

	// Wait for a moment
	time.Sleep(2 * time.Second)

	// Print stats after pausing communication namespace
	fmt.Println("\nTask statistics after pausing communication namespace:")
	printStats(scheduler)

	// Wait for processing tasks to make progress
	time.Sleep(5 * time.Second)

	// Resume the communication namespace
	fmt.Println("\nResuming the communication namespace...")
	scheduler.ResumeNamespace("communication")

	// Wait for a moment
	time.Sleep(2 * time.Second)

	// Print stats after resuming communication namespace
	fmt.Println("\nTask statistics after resuming communication namespace:")
	printStats(scheduler)

	// Wait for all tasks to complete
	time.Sleep(10 * time.Second)

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

	// Get namespace information using reflection (since these are not exposed via public methods)
	// In a real application, you would add public methods to access this information
	schedulerValue := reflect.ValueOf(scheduler).Elem()
	taskTypeToNamespace := schedulerValue.FieldByName("taskTypeToNamespace").Interface().(map[string]string)
	namespaceMaxConcurrency := schedulerValue.FieldByName("namespaceMaxConcurrency").Interface().(map[string]int)
	namespaceActiveTasks := schedulerValue.FieldByName("namespaceActiveTasks").Interface().(map[string]int)
	pausedNamespaces := schedulerValue.FieldByName("pausedNamespaces").Interface().(map[string]bool)

	// Group task types by namespace
	namespaceToTaskTypes := make(map[string][]string)
	for taskType, namespace := range taskTypeToNamespace {
		namespaceToTaskTypes[namespace] = append(namespaceToTaskTypes[namespace], taskType)
	}

	// Print namespace stats
	fmt.Println("Stats by namespace:")
	for namespace, taskTypes := range namespaceToTaskTypes {
		maxConcurrency := namespaceMaxConcurrency[namespace]
		activeTasks := namespaceActiveTasks[namespace]
		paused := ""
		if pausedNamespaces[namespace] {
			paused = " (PAUSED)"
		}

		fmt.Printf("  Namespace: %s%s - Active: %d/%d\n",
			namespace, paused, activeTasks, maxConcurrency)

		// Print task types in this namespace
		fmt.Printf("    Task types: %s\n", strings.Join(taskTypes, ", "))
	}

	fmt.Println("Stats by task type:")
	for taskType, typeStat := range stats {
		namespace := taskTypeToNamespace[taskType]
		namespaceInfo := ""
		if namespace != "" {
			namespaceInfo = fmt.Sprintf(" (Namespace: %s)", namespace)
		}

		fmt.Printf("  %s%s: Queued: %d, Executing: %d, Completed: %d, Failed: %d, Cancelled: %d\n",
			taskType, namespaceInfo, typeStat.Queued, typeStat.Executing, typeStat.Completed,
			typeStat.Failed, typeStat.Cancelled)
	}
}
