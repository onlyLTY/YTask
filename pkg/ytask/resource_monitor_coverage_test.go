package ytask

import (
	"testing"
	"time"
)

// TestResourceMonitorStartWhenRunning tests that Start() does nothing when the monitor is already running
func TestResourceMonitorStartWhenRunning(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(10)

	// Create a resource monitor
	monitor := NewResourceMonitor(scheduler, 70.0, 70.0)

	// Start the monitor
	monitor.Start()

	// Verify that the monitor is running
	if !monitor.running {
		t.Errorf("Expected monitor to be running")
	}

	// Start the monitor again (should do nothing)
	monitor.Start()

	// Stop the monitor
	monitor.Stop()

	// Verify that the monitor is stopped
	if monitor.running {
		t.Errorf("Expected monitor to be stopped")
	}
}

// TestResourceMonitorSetThresholds tests the threshold setting methods
func TestResourceMonitorSetThresholds(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(10)

	// Create a resource monitor
	monitor := NewResourceMonitor(scheduler, 70.0, 70.0)

	// Set new thresholds
	monitor.SetMaxCPUUsage(80.0)
	monitor.SetMaxMemoryUsage(90.0)

	// Verify thresholds were set correctly by checking behavior
	// We can't directly access the private fields, so we'll test indirectly

	// Override GetCPUUsage to return a value just above the threshold
	originalGetCPUUsage := GetCPUUsage
	GetCPUUsage = func(prevTime *time.Time, prevUsage *float64) float64 {
		*prevTime = time.Now()
		*prevUsage = 85.0
		return 85.0
	}

	// Override GetMemoryUsage to return a value below the threshold
	originalGetMemoryUsage := GetMemoryUsage
	GetMemoryUsage = func() float64 {
		return 70.0
	}

	// Create variables for adjustConcurrency
	prevCPUTime := time.Now().Add(-1 * time.Second)
	prevCPUUsage := 50.0

	// Call adjustConcurrency
	monitor.adjustConcurrency(&prevCPUTime, &prevCPUUsage)

	// Verify that concurrency was reduced due to CPU usage above threshold
	if scheduler.maxGlobalConcurrency >= 10 {
		t.Errorf("Expected concurrency to be reduced below 10, got %d", scheduler.maxGlobalConcurrency)
	}

	// Restore original functions
	GetCPUUsage = originalGetCPUUsage
	GetMemoryUsage = originalGetMemoryUsage
}

// TestResourceMonitorErrorHandling tests error handling in the resource monitor
func TestResourceMonitorErrorHandling(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(10)

	// Create a resource monitor
	monitor := NewResourceMonitor(scheduler, 70.0, 70.0)

	// Override GetCPUUsage to simulate an error
	originalGetCPUUsage := GetCPUUsage
	GetCPUUsage = func(prevTime *time.Time, prevUsage *float64) float64 {
		return -1.0 // Indicate an error
	}

	// Start the monitor
	monitor.Start()

	// Wait a bit for the monitor to run
	time.Sleep(100 * time.Millisecond)

	// Stop the monitor
	monitor.Stop()

	// Restore original function
	GetCPUUsage = originalGetCPUUsage

	// Override GetMemoryUsage to simulate an error
	originalGetMemoryUsage := GetMemoryUsage
	GetMemoryUsage = func() float64 {
		return -1.0 // Indicate an error
	}

	// Start the monitor again
	monitor.Start()

	// Wait a bit for the monitor to run
	time.Sleep(100 * time.Millisecond)

	// Stop the monitor
	monitor.Stop()

	// Restore original function
	GetMemoryUsage = originalGetMemoryUsage
}

// TestResourceMonitorAdjustmentEdgeCases tests edge cases in the adjustConcurrency method
func TestResourceMonitorAdjustmentEdgeCases(t *testing.T) {
	// Create a scheduler
	scheduler := NewScheduler(10)

	// Create a resource monitor
	monitor := NewResourceMonitor(scheduler, 80.0, 80.0)

	// Set minimum concurrency
	monitor.SetMinConcurrency(2)

	// Test with invalid CPU usage
	prevCPUTime := time.Now().Add(-1 * time.Second)
	prevCPUUsage := 50.0

	// Override GetCPUUsage to return invalid value
	originalGetCPUUsage := GetCPUUsage
	GetCPUUsage = func(prevTime *time.Time, prevUsage *float64) float64 {
		*prevTime = time.Now()
		*prevUsage = -1.0
		return -1.0
	}

	// Call adjustConcurrency
	monitor.adjustConcurrency(&prevCPUTime, &prevCPUUsage)

	// Verify that concurrency wasn't changed due to invalid CPU usage
	if scheduler.maxGlobalConcurrency != 10 {
		t.Errorf("Expected concurrency to remain at 10, got %d", scheduler.maxGlobalConcurrency)
	}

	// Restore original function
	GetCPUUsage = originalGetCPUUsage

	// Test with invalid memory usage
	originalGetMemoryUsage := GetMemoryUsage
	GetMemoryUsage = func() float64 {
		return -1.0
	}

	// Call adjustConcurrency
	monitor.adjustConcurrency(&prevCPUTime, &prevCPUUsage)

	// Verify that concurrency wasn't changed due to invalid memory usage
	if scheduler.maxGlobalConcurrency != 10 {
		t.Errorf("Expected concurrency to remain at 10, got %d", scheduler.maxGlobalConcurrency)
	}

	// Restore original function
	GetMemoryUsage = originalGetMemoryUsage

	// Test with zero CPU usage (to test division by zero protection)
	GetCPUUsage = func(prevTime *time.Time, prevUsage *float64) float64 {
		*prevTime = time.Now()
		*prevUsage = 0.0
		return 0.0
	}

	// Call adjustConcurrency
	monitor.adjustConcurrency(&prevCPUTime, &prevCPUUsage)

	// Verify that concurrency wasn't reduced to zero
	if scheduler.maxGlobalConcurrency < 1 {
		t.Errorf("Expected concurrency to not be reduced to zero, got %d", scheduler.maxGlobalConcurrency)
	}

	// Restore original functions
	GetCPUUsage = originalGetCPUUsage
	GetMemoryUsage = originalGetMemoryUsage
}
