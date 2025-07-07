package ytask

import (
	"strconv"
	"testing"
	"time"
)

func TestResourceMonitor(t *testing.T) {
	// Create a scheduler with initial concurrency of 10
	scheduler := NewScheduler(10)

	// Create a resource monitor with max CPU and memory usage of 80%
	monitor := NewResourceMonitor(scheduler, 80.0, 80.0)

	// Set minimum concurrency to 2
	monitor.SetMinConcurrency(2)

	// Start the monitor
	monitor.Start()

	// Verify that the monitor is running
	if !monitor.running {
		t.Errorf("Expected monitor to be running")
	}

	// Wait for the monitor to run a few cycles
	time.Sleep(3 * time.Second)

	// Stop the monitor
	monitor.Stop()

	// Verify that the monitor is stopped
	if monitor.running {
		t.Errorf("Expected monitor to be stopped")
	}

	// Verify that the scheduler's concurrency was restored
	if scheduler.maxGlobalConcurrency != 10 {
		t.Errorf("Expected scheduler concurrency to be restored to 10, got %d", scheduler.maxGlobalConcurrency)
	}
}

func TestResourceMonitorAdjustment(t *testing.T) {
	// Create a scheduler with initial concurrency of 10
	scheduler := NewScheduler(10)

	// Create a resource monitor with max CPU and memory usage of 80%
	monitor := NewResourceMonitor(scheduler, 80.0, 80.0)

	// Set minimum concurrency to 2
	monitor.SetMinConcurrency(2)

	// Manually call adjustConcurrency with simulated high CPU usage
	prevCPUTime := time.Now().Add(-1 * time.Second)
	prevCPUUsage := 50.0

	// Simulate CPU usage at 90% (above the 80% threshold)
	GetCPUUsage = func(prevTime *time.Time, prevUsage *float64) float64 {
		*prevTime = time.Now()
		*prevUsage = 90.0
		return 90.0
	}

	// Simulate memory usage at 70% (below the 80% threshold)
	GetMemoryUsage = func() float64 {
		return 70.0
	}

	// Call adjustConcurrency
	monitor.adjustConcurrency(&prevCPUTime, &prevCPUUsage)

	// Verify that concurrency was reduced due to high CPU usage
	// Expected: 10 * (80/90) ≈ 8.89, rounded to 8
	if scheduler.maxGlobalConcurrency > 9 {
		t.Errorf("Expected concurrency to be reduced below 10, got %d", scheduler.maxGlobalConcurrency)
	}

	// Now simulate both CPU and memory usage below thresholds
	GetCPUUsage = func(prevTime *time.Time, prevUsage *float64) float64 {
		*prevTime = time.Now()
		*prevUsage = 60.0
		return 60.0
	}

	GetMemoryUsage = func() float64 {
		return 60.0
	}

	// Reset scheduler concurrency
	scheduler.SetMaxGlobalConcurrency(5)

	// Call adjustConcurrency
	monitor.adjustConcurrency(&prevCPUTime, &prevCPUUsage)

	// Verify that concurrency was increased (but not all the way to 10 at once)
	if scheduler.maxGlobalConcurrency <= 5 {
		t.Errorf("Expected concurrency to be increased above 5, got %d", scheduler.maxGlobalConcurrency)
	}

	if scheduler.maxGlobalConcurrency > 7 { // Should increase by at most 2
		t.Errorf("Expected concurrency to increase by at most 2, got %d (from 5)", scheduler.maxGlobalConcurrency)
	}

	// Test minimum concurrency
	// Simulate extremely high resource usage
	GetCPUUsage = func(prevTime *time.Time, prevUsage *float64) float64 {
		*prevTime = time.Now()
		*prevUsage = 100.0
		return 100.0
	}

	GetMemoryUsage = func() float64 {
		return 100.0
	}

	// Call adjustConcurrency
	monitor.adjustConcurrency(&prevCPUTime, &prevCPUUsage)

	// Verify that concurrency was reduced but not below minimum
	if scheduler.maxGlobalConcurrency < 2 {
		t.Errorf("Expected concurrency to not go below minimum of 2, got %d", scheduler.maxGlobalConcurrency)
	}

	// Restore original functions
	GetCPUUsage = defaultGetCPUUsage
	GetMemoryUsage = defaultGetMemoryUsage
}

// Override the CPU and memory usage functions for testing
var (
	originalGetCPUUsage    = GetCPUUsage
	originalGetMemoryUsage = GetMemoryUsage
)

func init() {
	// Save original functions
	if originalGetCPUUsage == nil {
		originalGetCPUUsage = GetCPUUsage
	}
	if originalGetMemoryUsage == nil {
		originalGetMemoryUsage = GetMemoryUsage
	}
}

func TestMain(m *testing.M) {
	// Run tests
	code := m.Run()

	// Restore original functions
	GetCPUUsage = originalGetCPUUsage
	GetMemoryUsage = originalGetMemoryUsage

	// Exit with the test status code
	if code != 0 {
		panic("Tests failed code" + strconv.Itoa(code) + " (see above for details)")
	}
}
