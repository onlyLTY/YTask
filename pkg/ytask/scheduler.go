package ytask

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// FailureHandler is a function that handles task failures
type FailureHandler func(task Task, err error) (time.Time, bool)

// Middleware is a function that wraps task execution
type Middleware func(next func(ctx context.Context, task Task) error) func(ctx context.Context, task Task) error

// PriorityMode defines how tasks with different priorities are scheduled
type PriorityMode int

const (
	// PriorityModePercentage schedules tasks based on priority percentages
	PriorityModePercentage PriorityMode = iota
	// PriorityModeStrict schedules tasks strictly by priority (higher first)
	PriorityModeStrict
)

// TaskTypeConfig holds configuration for a specific task type
type TaskTypeConfig struct {
	// MaxConcurrency is the maximum number of concurrent tasks of this type
	MaxConcurrency int
	// PriorityLevels is the number of priority levels for this task type
	PriorityLevels int
	// PriorityMode determines how priorities are handled
	PriorityMode PriorityMode
	// PriorityPercentages defines the percentage of tasks to execute at each priority level
	// Only used when PriorityMode is PriorityModePercentage
	PriorityPercentages []int
	// FailureHandler is called when a task fails
	FailureHandler FailureHandler
	// FilterDuplicates determines whether to filter duplicate tasks
	FilterDuplicates bool
}

// TaskStats holds statistics for tasks
type TaskStats struct {
	Queued    int64
	Executing int64
	Completed int64
	Failed    int64
	Paused    int64
	Cancelled int64
}

// Scheduler manages task execution
type Scheduler struct {
	// taskQueues holds tasks grouped by type and priority
	taskQueues map[string][][]Task
	// taskConfigs holds configuration for each task type
	taskConfigs map[string]TaskTypeConfig
	// stats holds task statistics
	stats map[string]*TaskStats
	// globalStats holds global task statistics
	globalStats TaskStats
	// activeTasks tracks currently executing tasks by type
	activeTasks map[string]int
	// maxGlobalConcurrency is the maximum number of concurrent tasks across all types
	maxGlobalConcurrency int
	// currentGlobalConcurrency is the current number of concurrent tasks
	currentGlobalConcurrency int32
	// middlewares is a list of middleware functions to apply to task execution
	middlewares []Middleware
	// mutex protects the scheduler's state
	mutex sync.RWMutex
	// pausedTypes tracks which task types are paused
	pausedTypes map[string]bool
	// wg is used to wait for all tasks to complete
	wg sync.WaitGroup
	// ctx is the context for the scheduler
	ctx context.Context
	// cancel is the cancel function for the scheduler's context
	cancel context.CancelFunc

	// taskTypeToNamespace maps task types to namespaces
	taskTypeToNamespace map[string]string
	// namespaceMaxConcurrency holds the maximum concurrency for each namespace
	namespaceMaxConcurrency map[string]int
	// namespaceActiveTasks tracks currently executing tasks by namespace
	namespaceActiveTasks map[string]int
	// pausedNamespaces tracks which namespaces are paused
	pausedNamespaces map[string]bool
}

// NewScheduler creates a new task scheduler
func NewScheduler(maxGlobalConcurrency int) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())

	if maxGlobalConcurrency <= 0 {
		// Default to number of CPU cores if not specified
		maxGlobalConcurrency = runtime.NumCPU()
	}

	return &Scheduler{
		taskQueues:               make(map[string][][]Task),
		taskConfigs:              make(map[string]TaskTypeConfig),
		stats:                    make(map[string]*TaskStats),
		globalStats:              TaskStats{},
		activeTasks:              make(map[string]int),
		maxGlobalConcurrency:     maxGlobalConcurrency,
		currentGlobalConcurrency: 0,
		middlewares:              []Middleware{},
		pausedTypes:              make(map[string]bool),
		ctx:                      ctx,
		cancel:                   cancel,
		taskTypeToNamespace:      make(map[string]string),
		namespaceMaxConcurrency:  make(map[string]int),
		namespaceActiveTasks:     make(map[string]int),
		pausedNamespaces:         make(map[string]bool),
	}
}

// RegisterTaskType registers a new task type with the scheduler
func (s *Scheduler) RegisterTaskType(taskType string, config TaskTypeConfig) {
	s.RegisterTaskTypeWithNamespace(taskType, "", config)
}

// RegisterTaskTypeWithNamespace registers a new task type with the scheduler and associates it with a namespace.
// This allows for grouping related task types and applying concurrency limits at the namespace level.
// If namespace is empty, the task type is not associated with any namespace.
func (s *Scheduler) RegisterTaskTypeWithNamespace(taskType string, namespace string, config TaskTypeConfig) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.taskConfigs[taskType] = config

	// Initialize queues for each priority level
	priorityQueues := make([][]Task, config.PriorityLevels)
	for i := 0; i < config.PriorityLevels; i++ {
		priorityQueues[i] = make([]Task, 0)
	}
	s.taskQueues[taskType] = priorityQueues

	// Initialize stats for this task type
	s.stats[taskType] = &TaskStats{}

	// Initialize active tasks count
	s.activeTasks[taskType] = 0

	// Associate task type with namespace if provided
	if namespace != "" {
		s.taskTypeToNamespace[taskType] = namespace

		// Initialize namespace active tasks count if it doesn't exist
		if _, exists := s.namespaceActiveTasks[namespace]; !exists {
			s.namespaceActiveTasks[namespace] = 0
		}
	}
}

// AddTask adds a task to the scheduler
func (s *Scheduler) AddTask(task Task) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	taskType := task.GetType()
	config, exists := s.taskConfigs[taskType]
	if !exists {
		return false
	}

	// Check for duplicates if enabled
	if config.FilterDuplicates {
		for priority := 0; priority < config.PriorityLevels; priority++ {
			for _, existingTask := range s.taskQueues[taskType][priority] {
				if existingTask.IsDuplicate(task) {
					return false
				}
			}
		}
	}

	// Get task priority and ensure it's within bounds
	priority := task.GetPriority()
	if priority < 0 {
		priority = 0
	} else if priority >= config.PriorityLevels {
		priority = config.PriorityLevels - 1
	}

	// Add a task to the appropriate queue
	s.taskQueues[taskType][priority] = append(s.taskQueues[taskType][priority], task)

	// Update stats
	atomic.AddInt64(&s.stats[taskType].Queued, 1)
	atomic.AddInt64(&s.globalStats.Queued, 1)

	return true
}

// Use adds middleware to the scheduler
func (s *Scheduler) Use(middleware Middleware) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.middlewares = append(s.middlewares, middleware)
}

// Start starts the scheduler
func (s *Scheduler) Start() {
	go s.processTasksLoop()
}

// Stop stops the scheduler and waits for all tasks to complete
func (s *Scheduler) Stop() {
	s.cancel()
	s.wg.Wait()
}

// PauseTaskType pauses processing of a specific task type
func (s *Scheduler) PauseTaskType(taskType string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.pausedTypes[taskType] = true
}

// ResumeTaskType resumes processing of a specific task type
func (s *Scheduler) ResumeTaskType(taskType string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.pausedTypes, taskType)
}

// CancelTask cancels a specific task
func (s *Scheduler) CancelTask(taskID string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for taskType, queues := range s.taskQueues {
		for priority, queue := range queues {
			for i, task := range queue {
				if task.GetID() == taskID {
					// Remove task from queue
					s.taskQueues[taskType][priority] = append(queue[:i], queue[i+1:]...)

					// Update stats
					atomic.AddInt64(&s.stats[taskType].Queued, -1)
					atomic.AddInt64(&s.globalStats.Queued, -1)
					atomic.AddInt64(&s.stats[taskType].Cancelled, 1)
					atomic.AddInt64(&s.globalStats.Cancelled, 1)

					task.SetStatus(TaskStatusCancelled)
					return true
				}
			}
		}
	}

	return false
}

// CancelTasksByType cancels all tasks of a specific type
func (s *Scheduler) CancelTasksByType(taskType string) int {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check if the task type exists
	queues, exists := s.taskQueues[taskType]
	if !exists {
		return 0
	}

	cancelledCount := 0

	// Iterate through all priority queues for this task type
	for priority, queue := range queues {
		for _, task := range queue {
			// Update task status
			task.SetStatus(TaskStatusCancelled)
			cancelledCount++
		}

		// Update stats
		queueLength := int64(len(queue))
		atomic.AddInt64(&s.stats[taskType].Queued, -queueLength)
		atomic.AddInt64(&s.globalStats.Queued, -queueLength)
		atomic.AddInt64(&s.stats[taskType].Cancelled, queueLength)
		atomic.AddInt64(&s.globalStats.Cancelled, queueLength)

		// Clear the queue
		s.taskQueues[taskType][priority] = make([]Task, 0)
	}

	return cancelledCount
}

// ClearTasksByType removes all tasks of a specific type without marking them as cancelled
// and resets all counters for this task type to 0
func (s *Scheduler) ClearTasksByType(taskType string) int {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check if the task type exists
	queues, exists := s.taskQueues[taskType]
	if !exists {
		return 0
	}

	clearedCount := 0

	// Iterate through all priority queues for this task type
	for priority, queue := range queues {
		// Count cleared tasks
		clearedCount += len(queue)

		// Clear the queue
		s.taskQueues[taskType][priority] = make([]Task, 0)
	}

	// Get current stats for this task type
	taskStats := s.stats[taskType]

	// Update global stats by decrementing the current values
	atomic.AddInt64(&s.globalStats.Queued, -atomic.LoadInt64(&taskStats.Queued))
	atomic.AddInt64(&s.globalStats.Executing, -atomic.LoadInt64(&taskStats.Executing))
	atomic.AddInt64(&s.globalStats.Completed, -atomic.LoadInt64(&taskStats.Completed))
	atomic.AddInt64(&s.globalStats.Failed, -atomic.LoadInt64(&taskStats.Failed))
	atomic.AddInt64(&s.globalStats.Paused, -atomic.LoadInt64(&taskStats.Paused))
	atomic.AddInt64(&s.globalStats.Cancelled, -atomic.LoadInt64(&taskStats.Cancelled))

	// Reset all counters for this task type to 0
	atomic.StoreInt64(&taskStats.Queued, 0)
	atomic.StoreInt64(&taskStats.Executing, 0)
	atomic.StoreInt64(&taskStats.Completed, 0)
	atomic.StoreInt64(&taskStats.Failed, 0)
	atomic.StoreInt64(&taskStats.Paused, 0)
	atomic.StoreInt64(&taskStats.Cancelled, 0)

	return clearedCount
}

// GetStats returns statistics for all task types
func (s *Scheduler) GetStats() map[string]*TaskStats {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Create a copy to avoid race conditions
	statsCopy := make(map[string]*TaskStats)
	for taskType, stats := range s.stats {
		statsCopy[taskType] = &TaskStats{
			Queued:    atomic.LoadInt64(&stats.Queued),
			Executing: atomic.LoadInt64(&stats.Executing),
			Completed: atomic.LoadInt64(&stats.Completed),
			Failed:    atomic.LoadInt64(&stats.Failed),
			Paused:    atomic.LoadInt64(&stats.Paused),
			Cancelled: atomic.LoadInt64(&stats.Cancelled),
		}
	}

	return statsCopy
}

// GetGlobalStats returns global task statistics
func (s *Scheduler) GetGlobalStats() TaskStats {
	return TaskStats{
		Queued:    atomic.LoadInt64(&s.globalStats.Queued),
		Executing: atomic.LoadInt64(&s.globalStats.Executing),
		Completed: atomic.LoadInt64(&s.globalStats.Completed),
		Failed:    atomic.LoadInt64(&s.globalStats.Failed),
		Paused:    atomic.LoadInt64(&s.globalStats.Paused),
		Cancelled: atomic.LoadInt64(&s.globalStats.Cancelled),
	}
}

// SetMaxGlobalConcurrency updates the maximum global concurrency
func (s *Scheduler) SetMaxGlobalConcurrency(max int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.maxGlobalConcurrency = max
}

// SetNamespaceMaxConcurrency sets the maximum concurrency for a namespace.
// This limits the total number of tasks that can be executed concurrently across all task types in the namespace.
// If max <= 0, the concurrency limit for the namespace is removed.
func (s *Scheduler) SetNamespaceMaxConcurrency(namespace string, max int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if max <= 0 {
		// Remove the concurrency limit if max <= 0
		delete(s.namespaceMaxConcurrency, namespace)
	} else {
		s.namespaceMaxConcurrency[namespace] = max
	}
}

// PauseNamespace pauses processing of all task types in a namespace.
// This prevents any tasks in the namespace from being executed until ResumeNamespace is called.
// Tasks that are already executing will continue to run until completion.
func (s *Scheduler) PauseNamespace(namespace string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.pausedNamespaces[namespace] = true
}

// ResumeNamespace resumes processing of all task types in a namespace.
// This allows tasks in the namespace to be executed after they were paused with PauseNamespace.
// The scheduler will start processing tasks in the namespace on the next processing cycle.
func (s *Scheduler) ResumeNamespace(namespace string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.pausedNamespaces, namespace)
}

// processTasksLoop continuously processes tasks
func (s *Scheduler) processTasksLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.processTasks()
		}
	}
}

// processTasks processes tasks from all queues
func (s *Scheduler) processTasks() {
	s.mutex.Lock()

	// Create a list of task types to process
	taskTypes := make([]string, 0, len(s.taskQueues))
	for taskType := range s.taskQueues {
		// Skip paused task types
		if s.pausedTypes[taskType] {
			continue
		}

		// Skip task types in paused namespaces
		namespace, hasNamespace := s.taskTypeToNamespace[taskType]
		if hasNamespace && s.pausedNamespaces[namespace] {
			continue
		}

		taskTypes = append(taskTypes, taskType)
	}

	// Shuffle task types to ensure fairness
	sort.Strings(taskTypes)

	for _, taskType := range taskTypes {
		// Check if we can start more tasks based on global concurrency limit
		currentGlobal := atomic.LoadInt32(&s.currentGlobalConcurrency)
		if int(currentGlobal) >= s.maxGlobalConcurrency {
			break // Stop processing if we've reached the global limit
		}

		// Check namespace concurrency limit if applicable
		namespace, hasNamespace := s.taskTypeToNamespace[taskType]
		if hasNamespace {
			maxNamespaceConcurrency, hasLimit := s.namespaceMaxConcurrency[namespace]
			if hasLimit && s.namespaceActiveTasks[namespace] >= maxNamespaceConcurrency {
				continue // Skip this task type if namespace limit reached
			}
		}

		config := s.taskConfigs[taskType]

		// Check if we can start more tasks of this type
		if s.activeTasks[taskType] >= config.MaxConcurrency {
			continue
		}

		// Select tasks based on priority mode
		var selectedTasks []Task

		if config.PriorityMode == PriorityModeStrict {
			// In strict mode, process higher priority tasks first
			for priority := config.PriorityLevels - 1; priority >= 0; priority-- {
				if len(s.taskQueues[taskType][priority]) > 0 {
					// Check if the first task is ready to be executed
					task := s.taskQueues[taskType][priority][0]
					if !task.GetNextExecutionTime().After(time.Now()) {
						// Take the first task from this priority queue
						s.taskQueues[taskType][priority] = s.taskQueues[taskType][priority][1:]
						selectedTasks = append(selectedTasks, task)
						break
					}
				}
			}
		} else {
			// In percentage mode, distribute tasks according to percentages
			// Calculate total number of ready tasks
			totalReadyTasks := 0
			readyTasksByPriority := make([]int, config.PriorityLevels)

			for priority := 0; priority < config.PriorityLevels; priority++ {
				for _, task := range s.taskQueues[taskType][priority] {
					if !task.GetNextExecutionTime().After(time.Now()) {
						readyTasksByPriority[priority]++
						totalReadyTasks++
					}
				}
			}

			if totalReadyTasks > 0 {
				// First, calculate the sum of percentages for priority levels that have ready tasks
				// and store the original percentages for priorities with ready tasks
				totalPercentage := 0
				priorityPercentages := make(map[int]int)

				for priority := 0; priority < config.PriorityLevels; priority++ {
					// Skip priorities with no ready tasks
					if readyTasksByPriority[priority] == 0 {
						continue
					}

					// Get the percentage for this priority level
					var percentage int
					if priority < len(config.PriorityPercentages) {
						percentage = config.PriorityPercentages[priority]
					} else {
						// If percentage not specified, distribute evenly
						percentage = 100 / config.PriorityLevels
					}

					priorityPercentages[priority] = percentage
					totalPercentage += percentage
				}

				// Generate a random number between 0 and 100
				randomPercentage := rand.IntN(100)

				// Find the priority level based on the random percentage
				// using remapped percentages that sum to 100
				cumulativePercentage := 0
				selectedPriority := -1 // Default to -1 to detect if no priority was selected

				for priority := config.PriorityLevels - 1; priority >= 0; priority-- {
					// Skip priorities with no ready tasks
					if readyTasksByPriority[priority] == 0 {
						continue
					}

					// Remap the percentage to a proportion of 100 based on the total percentage
					var remappedPercentage int
					if totalPercentage > 0 {
						remappedPercentage = (priorityPercentages[priority] * 100) / totalPercentage
					} else {
						// If total percentage is 0 (shouldn't happen), distribute evenly
						remappedPercentage = 100 / len(priorityPercentages)
					}

					cumulativePercentage += remappedPercentage
					if randomPercentage < cumulativePercentage || cumulativePercentage >= 100 {
						selectedPriority = priority
						break
					}
				}

				// If we didn't select a priority (due to rounding errors), use the highest priority with ready tasks
				if selectedPriority == -1 {
					for priority := config.PriorityLevels - 1; priority >= 0; priority-- {
						if readyTasksByPriority[priority] > 0 {
							selectedPriority = priority
							break
						}
					}
				}

				// Take the first ready task from the selected priority queue
				for i, task := range s.taskQueues[taskType][selectedPriority] {
					if !task.GetNextExecutionTime().After(time.Now()) {
						// Remove the task from the queue
						s.taskQueues[taskType][selectedPriority] = append(
							s.taskQueues[taskType][selectedPriority][:i],
							s.taskQueues[taskType][selectedPriority][i+1:]...,
						)
						selectedTasks = append(selectedTasks, task)
						break
					}
				}
			}
		}

		// Start selected tasks
		for _, task := range selectedTasks {
			// Check global concurrency limit again before starting each task
			currentGlobal = atomic.LoadInt32(&s.currentGlobalConcurrency)
			if int(currentGlobal) >= s.maxGlobalConcurrency {
				// Put the task back in the queue
				priority := task.GetPriority()
				s.taskQueues[taskType][priority] = append([]Task{task}, s.taskQueues[taskType][priority]...)
				break // Stop processing tasks
			}

			// Update stats
			atomic.AddInt64(&s.stats[taskType].Queued, -1)
			atomic.AddInt64(&s.globalStats.Queued, -1)
			atomic.AddInt64(&s.stats[taskType].Executing, 1)
			atomic.AddInt64(&s.globalStats.Executing, 1)

			s.activeTasks[taskType]++
			atomic.AddInt32(&s.currentGlobalConcurrency, 1)

			// Update namespace active tasks count if applicable
			namespace, hasNamespace := s.taskTypeToNamespace[taskType]
			if hasNamespace {
				s.namespaceActiveTasks[namespace]++
			}

			task.SetStatus(TaskStatusExecuting)

			// Start task execution in a goroutine
			s.wg.Add(1)
			go s.executeTask(task, config)
		}
	}

	s.mutex.Unlock()
}

// executeTask executes a task with middleware and error handling
func (s *Scheduler) executeTask(task Task, config TaskTypeConfig) {
	defer s.wg.Done()
	defer atomic.AddInt32(&s.currentGlobalConcurrency, -1)

	taskType := task.GetType()

	// Create a context for this task
	ctx, cancel := context.WithCancel(s.ctx)
	// Add task to context so it can be retrieved by middleware or task functions
	ctx = context.WithValue(ctx, "task", task)
	defer cancel()

	// Build the execution chain with middleware
	execFunc := func(ctx context.Context, task Task) error {
		return task.Execute(ctx)
	}

	// Apply middleware in reverse order
	for i := len(s.middlewares) - 1; i >= 0; i-- {
		execFunc = s.middlewares[i](execFunc)
	}

	// Execute the task with panic recovery
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				// Convert panic to error
				switch v := r.(type) {
				case error:
					err = v
				case string:
					err = fmt.Errorf("panic: %s", v)
				default:
					err = fmt.Errorf("panic: %v", r)
				}
			}
		}()
		// Execute the task
		err = execFunc(ctx, task)
	}()

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Update active tasks count
	s.activeTasks[taskType]--

	// Update namespace active tasks count if applicable
	namespace, hasNamespace := s.taskTypeToNamespace[taskType]
	if hasNamespace {
		s.namespaceActiveTasks[namespace]--
	}

	// Update stats
	atomic.AddInt64(&s.stats[taskType].Executing, -1)
	atomic.AddInt64(&s.globalStats.Executing, -1)

	if err != nil {
		// Task failed
		atomic.AddInt64(&s.stats[taskType].Failed, 1)
		atomic.AddInt64(&s.globalStats.Failed, 1)

		task.SetStatus(TaskStatusFailed)
		task.IncrementFailureCount()

		// Call failure handler if provided
		if config.FailureHandler != nil {
			nextTime, retry := config.FailureHandler(task, err)
			if retry {
				// Always re-queue the task, even for immediate retries
				// This avoids recursive calls that can lead to stack overflow
				task.SetNextExecutionTime(nextTime)
				task.SetStatus(TaskStatusQueued)
				s.taskQueues[taskType][task.GetPriority()] = append(s.taskQueues[taskType][task.GetPriority()], task)
				atomic.AddInt64(&s.stats[taskType].Queued, 1)
				atomic.AddInt64(&s.globalStats.Queued, 1)
			}
		}
	} else {
		// Task completed successfully
		atomic.AddInt64(&s.stats[taskType].Completed, 1)
		atomic.AddInt64(&s.globalStats.Completed, 1)

		task.SetStatus(TaskStatusCompleted)
	}
}
