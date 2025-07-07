package ytask

import (
	"context"
	"fmt"
	"time"
)

// TaskStatus represents the current status of a task
type TaskStatus string

const (
	TaskStatusQueued    TaskStatus = "queued"
	TaskStatusExecuting TaskStatus = "executing"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusPaused    TaskStatus = "paused"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// Task is the interface that all tasks must implement
type Task interface {
	// Execute runs the task and returns any error
	Execute(ctx context.Context) error

	// GetID returns the unique identifier of the task
	GetID() string

	// GetType returns the type of the task
	GetType() string

	// GetPriority returns the priority of the task
	GetPriority() int

	// GetStatus returns the current status of the task
	GetStatus() TaskStatus

	// SetStatus updates the status of the task
	SetStatus(status TaskStatus)

	// GetFailureCount returns the number of times this task has failed
	GetFailureCount() int

	// IncrementFailureCount increments the failure count
	IncrementFailureCount()

	// GetNextExecutionTime returns the next time this task should be executed
	GetNextExecutionTime() time.Time

	// SetNextExecutionTime sets the next time this task should be executed
	SetNextExecutionTime(time time.Time)

	// IsDuplicate checks if this task is a duplicate of another task
	IsDuplicate(other Task) bool
}

// BaseTask provides a basic implementation of the Task interface
type BaseTask struct {
	ID                string
	Type              string
	Priority          int
	Status            TaskStatus
	FailureCount      int
	NextExecutionTime time.Time
}

// Execute is a placeholder implementation that should be overridden by embedding structs
func (t *BaseTask) Execute(ctx context.Context) error {
	return fmt.Errorf("Execute not implemented for BaseTask, this method should be overridden")
}

// GetID returns the unique identifier of the task
func (t *BaseTask) GetID() string {
	return t.ID
}

// GetType returns the type of the task
func (t *BaseTask) GetType() string {
	return t.Type
}

// GetPriority returns the priority of the task
func (t *BaseTask) GetPriority() int {
	return t.Priority
}

// GetStatus returns the current status of the task
func (t *BaseTask) GetStatus() TaskStatus {
	return t.Status
}

// SetStatus updates the status of the task
func (t *BaseTask) SetStatus(status TaskStatus) {
	t.Status = status
}

// GetFailureCount returns the number of times this task has failed
func (t *BaseTask) GetFailureCount() int {
	return t.FailureCount
}

// IncrementFailureCount increments the failure count
func (t *BaseTask) IncrementFailureCount() {
	t.FailureCount++
}

// GetNextExecutionTime returns the next time this task should be executed
func (t *BaseTask) GetNextExecutionTime() time.Time {
	return t.NextExecutionTime
}

// SetNextExecutionTime sets the next time this task should be executed
func (t *BaseTask) SetNextExecutionTime(time time.Time) {
	t.NextExecutionTime = time
}

// IsDuplicate checks if this task is a duplicate of another task
// Default implementation checks if IDs are the same
func (t *BaseTask) IsDuplicate(other Task) bool {
	return t.ID == other.GetID()
}

// NewBaseTask creates a new BaseTask with the given parameters
func NewBaseTask(id, taskType string, priority int) *BaseTask {
	return &BaseTask{
		ID:                id,
		Type:              taskType,
		Priority:          priority,
		Status:            TaskStatusQueued,
		FailureCount:      0,
		NextExecutionTime: time.Now(),
	}
}
