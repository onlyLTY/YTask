# YTask

YTask is a powerful, flexible task scheduling system for Go applications with advanced features for priority management, concurrency control, and system resource monitoring.

## Features

- **Task Scheduling**: Schedule and execute tasks with different priorities and types
- **Priority Management**: Support for strict priority mode and percentage-based priority allocation
- **Concurrency Control**: Limit concurrent task execution globally and per task type
- **Resource Monitoring**: Automatically adjust concurrency based on CPU and memory usage
- **Middleware Support**: Add cross-cutting concerns like logging, metrics, or authentication
- **Failure Handling**: Configurable retry mechanisms with exponential backoff
- **Task Management**: Pause, resume, or cancel tasks or task types
- **Statistics Tracking**: Monitor task execution statistics globally and per task type

## Installation

```bash
go get github.com/onlyLTY/YTask
```

Requires Go 1.24 or later.

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/onlyLTY/YTask/pkg/ytask"
)

// Define a custom task by embedding BaseTask
type EmailTask struct {
    ytask.BaseTask
    To      string
    Subject string
    Body    string
}

// Create a constructor for your task
func NewEmailTask(id string, priority int, to, subject, body string) *EmailTask {
    baseTask := ytask.NewBaseTask(id, "email", priority)
    return &EmailTask{
        BaseTask: *baseTask,
        To:       to,
        Subject:  subject,
        Body:     body,
    }
}

// Implement the Execute method
func (t *EmailTask) Execute(ctx context.Context) error {
    fmt.Printf("Sending email to %s with subject '%s'\n", t.To, t.Subject)
    // Simulate work
    time.Sleep(500 * time.Millisecond)
    fmt.Printf("Email sent to %s\n", t.To)
    return nil
}

func main() {
    // Create a scheduler with max 5 concurrent tasks
    scheduler := ytask.NewScheduler(5)

    // Register email task type with configuration
    scheduler.RegisterTaskType("email", ytask.TaskTypeConfig{
        MaxConcurrency:   2,
        PriorityLevels:   3,
        PriorityMode:     ytask.PriorityModeStrict,
        FilterDuplicates: true,
    })

    // Start the scheduler
    scheduler.Start()

    // Add some tasks
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

    // Wait for tasks to complete
    time.Sleep(5 * time.Second)

    // Stop the scheduler
    scheduler.Stop()
}
```

## Advanced Usage

### Adding Middleware

```go
// Define a logging middleware
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

// Add middleware to the scheduler
scheduler.Use(LoggingMiddleware)
```

### Resource Monitoring

```go
// Create a resource monitor that adjusts concurrency based on system load
monitor := ytask.NewResourceMonitor(scheduler, 80.0, 80.0)
monitor.SetMinConcurrency(1)

// Start the resource monitor
monitor.Start()

// Don't forget to stop it when done
defer monitor.Stop()
```

### Failure Handling

```go
// Define a failure handler with exponential backoff
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

// Register task type with failure handler
scheduler.RegisterTaskType("email", ytask.TaskTypeConfig{
    MaxConcurrency:   2,
    PriorityLevels:   3,
    PriorityMode:     ytask.PriorityModeStrict,
    FilterDuplicates: true,
    FailureHandler:   EmailFailureHandler,
})
```

### Task Management

```go
// Cancel a specific task
scheduler.CancelTask("email-4")

// Pause all tasks of a specific type
scheduler.PauseTaskType("email")

// Resume tasks of a specific type
scheduler.ResumeTaskType("email")

// Get statistics
stats := scheduler.GetStats()
globalStats := scheduler.GetGlobalStats()
```

### Namespace Support

Namespaces allow you to group related task types and apply concurrency limits at the namespace level.

```go
// Register task types with namespaces
scheduler.RegisterTaskTypeWithNamespace("email", "communication", ytask.TaskTypeConfig{
    MaxConcurrency:   2,
    PriorityLevels:   3,
    PriorityMode:     ytask.PriorityModeStrict,
    FilterDuplicates: true,
})

scheduler.RegisterTaskTypeWithNamespace("sms", "communication", ytask.TaskTypeConfig{
    MaxConcurrency:   2,
    PriorityLevels:   3,
    PriorityMode:     ytask.PriorityModeStrict,
    FilterDuplicates: true,
})

// Set namespace concurrency limits
scheduler.SetNamespaceMaxConcurrency("communication", 3) // Max 3 concurrent tasks in communication namespace

// Pause all tasks in a namespace
scheduler.PauseNamespace("communication")

// Resume all tasks in a namespace
scheduler.ResumeNamespace("communication")
```

Using namespaces provides several benefits:
- Group related task types logically
- Apply concurrency limits at the namespace level
- Pause/resume all task types in a namespace at once
- Better organize your task processing pipeline

## API Reference

### Core Types

- `Task`: Interface that all tasks must implement
- `BaseTask`: Basic implementation of the Task interface
- `Scheduler`: Manages task execution and scheduling
- `ResourceMonitor`: Monitors system resources and adjusts concurrency

### Priority Modes

- `PriorityModeStrict`: Higher priority tasks always execute before lower priority tasks
- `PriorityModePercentage`: Allocate execution slots based on percentage per priority level

### Task Status

- `TaskStatusQueued`: Task is waiting to be executed
- `TaskStatusExecuting`: Task is currently executing
- `TaskStatusCompleted`: Task has completed successfully
- `TaskStatusFailed`: Task has failed
- `TaskStatusPaused`: Task is paused
- `TaskStatusCancelled`: Task has been cancelled

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.