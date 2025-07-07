package ytask

import (
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"sync"
	"time"
)

// ResourceMonitor monitors system resources and adjusts concurrency limits
type ResourceMonitor struct {
	// scheduler is the task scheduler to adjust
	scheduler *Scheduler

	// maxCPUUsage is the maximum CPU usage percentage (0-100)
	maxCPUUsage float64

	// maxMemoryUsage is the maximum memory usage percentage (0-100)
	maxMemoryUsage float64

	// originalConcurrency is the original max global concurrency
	originalConcurrency int

	// minConcurrency is the minimum concurrency to allow
	minConcurrency int

	// mutex protects the monitor's state
	mutex sync.RWMutex

	// running indicates if the monitor is running
	running bool

	// stopCh is used to signal the monitor to stop
	stopCh chan struct{}
}

// NewResourceMonitor creates a new resource monitor
func NewResourceMonitor(scheduler *Scheduler, maxCPUUsage, maxMemoryUsage float64) *ResourceMonitor {
	return &ResourceMonitor{
		scheduler:           scheduler,
		maxCPUUsage:         maxCPUUsage,
		maxMemoryUsage:      maxMemoryUsage,
		originalConcurrency: scheduler.maxGlobalConcurrency,
		minConcurrency:      1,
		running:             false,
		stopCh:              make(chan struct{}),
	}
}

// Start starts the resource monitor
func (m *ResourceMonitor) Start() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.running {
		return
	}

	// Create a new stop channel if the monitor was previously stopped
	if m.stopCh == nil {
		m.stopCh = make(chan struct{})
	}

	m.running = true
	go m.monitorLoop()
}

// Stop stops the resource monitor
func (m *ResourceMonitor) Stop() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.running {
		return
	}

	m.running = false
	close(m.stopCh)
	m.stopCh = nil

	// Restore original concurrency
	m.scheduler.SetMaxGlobalConcurrency(m.originalConcurrency)
}

// SetMaxCPUUsage sets the maximum CPU usage percentage
func (m *ResourceMonitor) SetMaxCPUUsage(maxCPUUsage float64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.maxCPUUsage = maxCPUUsage
}

// SetMaxMemoryUsage sets the maximum memory usage percentage
func (m *ResourceMonitor) SetMaxMemoryUsage(maxMemoryUsage float64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.maxMemoryUsage = maxMemoryUsage
}

// SetMinConcurrency sets the minimum concurrency
func (m *ResourceMonitor) SetMinConcurrency(minConcurrency int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.minConcurrency = minConcurrency
}

// monitorLoop continuously monitors system resources
func (m *ResourceMonitor) monitorLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Previous CPU measurements for calculating usage
	var prevCPUTime time.Time
	var prevCPUUsage float64

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.adjustConcurrency(&prevCPUTime, &prevCPUUsage)
		}
	}
}

// adjustConcurrency adjusts the scheduler's concurrency based on resource usage
func (m *ResourceMonitor) adjustConcurrency(prevCPUTime *time.Time, prevCPUUsage *float64) {
	m.mutex.RLock()
	maxCPUUsage := m.maxCPUUsage
	maxMemoryUsage := m.maxMemoryUsage
	minConcurrency := m.minConcurrency
	originalConcurrency := m.originalConcurrency
	m.mutex.RUnlock()

	// Get current CPU usage
	cpuUsage := GetCPUUsage(prevCPUTime, prevCPUUsage)

	// Get current memory usage
	memUsage := GetMemoryUsage()

	// Calculate new concurrency based on resource usage
	cpuFactor := 1.0
	if cpuUsage > 0 {
		cpuFactor = maxCPUUsage / cpuUsage
	}

	memFactor := 1.0
	if memUsage > 0 {
		memFactor = maxMemoryUsage / memUsage
	}

	// Use the more restrictive factor
	factor := min(cpuFactor, memFactor)

	// Calculate new concurrency
	newConcurrency := int(float64(originalConcurrency) * factor)

	// Ensure minimum concurrency
	if newConcurrency < minConcurrency {
		newConcurrency = minConcurrency
	}

	// If factor is greater than 1, gradually increase concurrency
	if factor >= 1.0 {
		currentConcurrency := m.scheduler.maxGlobalConcurrency
		if newConcurrency > currentConcurrency {
			// Increase by at most 2 at a time to avoid oscillation
			newConcurrency = currentConcurrency + min(2, newConcurrency-currentConcurrency)
		}
	}

	// Update scheduler's concurrency if it changed
	if newConcurrency != m.scheduler.maxGlobalConcurrency {
		m.scheduler.SetMaxGlobalConcurrency(newConcurrency)
	}
}

// CPUUsageFunc is a function type for getting CPU usage
type CPUUsageFunc func(prevTime *time.Time, prevUsage *float64) float64

// MemoryUsageFunc is a function type for getting memory usage
type MemoryUsageFunc func() float64

// Default implementation of CPU usage calculation
var defaultGetCPUUsage CPUUsageFunc = func(prevTime *time.Time, prevUsage *float64) float64 {
	percentages, err := cpu.Percent(0, false)
	if err != nil || len(percentages) == 0 {
		return 0
	}
	return percentages[0]
}

// Default implementation of memory usage calculation
var defaultGetMemoryUsage MemoryUsageFunc = func() float64 {
	v, err := mem.VirtualMemory()
	if err != nil {
		return 0
	}
	return v.UsedPercent
}

// Current implementations that can be overridden for testing
var GetCPUUsage CPUUsageFunc = defaultGetCPUUsage
var GetMemoryUsage MemoryUsageFunc = defaultGetMemoryUsage
