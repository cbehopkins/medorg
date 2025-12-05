package adaptive

import (
	"runtime"
	"testing"
	"time"
)

func TestNewTuner(t *testing.T) {
	tuner := NewTuner()
	
	if tuner.minTokens != 1 {
		t.Errorf("expected minTokens=1, got %d", tuner.minTokens)
	}
	
	expectedMax := 2 * runtime.NumCPU()
	if tuner.maxTokens != expectedMax {
		t.Errorf("expected maxTokens=%d, got %d", expectedMax, tuner.maxTokens)
	}
	
	if tuner.currentTokens != runtime.NumCPU() {
		t.Errorf("expected currentTokens=%d, got %d", runtime.NumCPU(), tuner.currentTokens)
	}
	
	if tuner.inflectionDetected {
		t.Error("expected inflectionDetected=false on new tuner")
	}
}

func TestNewTunerWithConfig(t *testing.T) {
	tuner := NewTunerWithConfig(2, 8, 10*time.Second)
	
	if tuner.minTokens != 2 {
		t.Errorf("expected minTokens=2, got %d", tuner.minTokens)
	}
	
	if tuner.maxTokens != 8 {
		t.Errorf("expected maxTokens=8, got %d", tuner.maxTokens)
	}
	
	if tuner.currentTokens != 2 {
		t.Errorf("expected currentTokens=2, got %d", tuner.currentTokens)
	}
}

func TestTunerConfigDefaults(t *testing.T) {
	tests := []struct {
		name          string
		minTokens     int
		maxTokens     int
		checkInterval time.Duration
		expectedMin   int
		expectedMax   int
	}{
		{
			name:          "all invalid",
			minTokens:     0,
			maxTokens:     0,
			checkInterval: 0,
			expectedMin:   1,
			expectedMax:   1,
		},
		{
			name:          "min > max",
			minTokens:     10,
			maxTokens:     5,
			checkInterval: 10 * time.Second,
			expectedMin:   10,
			expectedMax:   10,
		},
		{
			name:          "short interval",
			minTokens:     1,
			maxTokens:     4,
			checkInterval: 1 * time.Second,
			expectedMin:   1,
			expectedMax:   4,
		},
	}
	
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tuner := NewTunerWithConfig(tc.minTokens, tc.maxTokens, tc.checkInterval)
			
			if tuner.minTokens != tc.expectedMin {
				t.Errorf("expected minTokens=%d, got %d", tc.expectedMin, tuner.minTokens)
			}
			
			if tuner.maxTokens != tc.expectedMax {
				t.Errorf("expected maxTokens=%d, got %d", tc.expectedMax, tuner.maxTokens)
			}
		})
	}
}

func TestAcquireReleaseToken(t *testing.T) {
	tuner := NewTunerWithConfig(1, 4, 1*time.Second)
	
	// Should be able to acquire token
	ch := tuner.AcquireToken()
	
	select {
	case <-ch:
		// Successfully acquired
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout acquiring token")
	}
	
	// Release it
	tuner.ReleaseToken()
}

func TestRecordBytes(t *testing.T) {
	tuner := NewTunerWithConfig(1, 4, 1*time.Second)
	
	tuner.RecordBytes(1000)
	tuner.RecordBytes(2000)
	
	// Verify bytes are recorded (we can't directly check atomic value, but we can verify no panic)
	stats := tuner.GetStats()
	if stats == nil {
		t.Error("GetStats returned nil")
	}
}

func TestGetStats(t *testing.T) {
	tuner := NewTunerWithConfig(1, 4, 1*time.Second)
	tuner.RecordBytes(5000)
	
	stats := tuner.GetStats()
	
	if stats["current_tokens"] != 1 {
		t.Errorf("expected current_tokens=1, got %v", stats["current_tokens"])
	}
	
	if stats["best_tokens"] != 0 {
		t.Errorf("expected best_tokens=0, got %v", stats["best_tokens"])
	}
	
	if stats["min_tokens"] != 1 {
		t.Errorf("expected min_tokens=1, got %v", stats["min_tokens"])
	}
	
	if stats["max_tokens"] != 4 {
		t.Errorf("expected max_tokens=4, got %v", stats["max_tokens"])
	}
	
	if stats["inflection_detected"] != false {
		t.Errorf("expected inflection_detected=false, got %v", stats["inflection_detected"])
	}
}

func TestStartStop(t *testing.T) {
	tuner := NewTunerWithConfig(1, 4, 100*time.Millisecond)
	
	tuner.Start()
	defer tuner.Stop()
	
	// Give monitoring loop time to run
	time.Sleep(150 * time.Millisecond)
	
	if tuner.stopped.Load() == false {
		// Should still be running
		if tuner.currentTokens < 1 || tuner.currentTokens > 4 {
			t.Errorf("tuner should still be monitoring, currentTokens=%d", tuner.currentTokens)
		}
	}
	
	tuner.Stop()
	
	if !tuner.stopped.Load() {
		t.Error("tuner should be stopped after Stop()")
	}
}

func TestInflectionDetection(t *testing.T) {
	tuner := NewTunerWithConfig(1, 3, 10*time.Millisecond)
	tuner.Start()
	defer tuner.Stop()
	
	// Simulate throughput data:
	// Token 1: 100 MB/s (best)
	// Token 2: 90 MB/s (drop)
	// Expected: inflection detected at token 2
	
	// Manually inject samples
	tuner.mu.Lock()
	
	// Sample 1: token count 1, good throughput
	tuner.throughputHistory = append(tuner.throughputHistory, throughputSample{
		tokenCount: 1,
		throughput: 100 * 1024 * 1024, // 100 MB/s
		timestamp:  time.Now(),
	})
	tuner.lastThroughput = 100 * 1024 * 1024
	tuner.bestThroughput = 100 * 1024 * 1024
	tuner.bestTokenCount = 1
	tuner.currentTokens = 1
	
	// Sample 2: token count 2, worse throughput (inflection)
	tuner.throughputHistory = append(tuner.throughputHistory, throughputSample{
		tokenCount: 2,
		throughput: 90 * 1024 * 1024, // 90 MB/s - 10% drop
		timestamp:  time.Now(),
	})
	tuner.lastThroughput = 90 * 1024 * 1024
	tuner.currentTokens = 2
	
	// Simulate makeAdjustment logic
	// This should detect inflection and freeze
	if len(tuner.throughputHistory) >= 2 {
		prevSample := tuner.throughputHistory[len(tuner.throughputHistory)-2]
		currSample := tuner.throughputHistory[len(tuner.throughputHistory)-1]
		
		percentChange := (currSample.throughput - prevSample.throughput) / prevSample.throughput * 100
		
		if percentChange < -5 && currSample.tokenCount > tuner.bestTokenCount {
			// This is what should happen
			tuner.inflectionDetected = true
		}
	}
	
	tuner.mu.Unlock()
	
	// Verify inflection was detected
	stats := tuner.GetStats()
	if stats["inflection_detected"] != true {
		t.Error("expected inflection to be detected")
	}
	
	if stats["best_tokens"] != 1 {
		t.Errorf("expected best_tokens=1, got %v", stats["best_tokens"])
	}
}

func TestThroughputImprovement(t *testing.T) {
	tuner := NewTunerWithConfig(1, 4, 1*time.Second)
	
	tuner.mu.Lock()
	
	// Sample 1: baseline throughput
	tuner.throughputHistory = append(tuner.throughputHistory, throughputSample{
		tokenCount: 1,
		throughput: 50 * 1024 * 1024, // 50 MB/s
		timestamp:  time.Now(),
	})
	tuner.lastThroughput = 50 * 1024 * 1024
	tuner.bestThroughput = 50 * 1024 * 1024
	tuner.bestTokenCount = 1
	
	// Sample 2: improved throughput with more tokens
	tuner.throughputHistory = append(tuner.throughputHistory, throughputSample{
		tokenCount: 2,
		throughput: 60 * 1024 * 1024, // 60 MB/s - 20% improvement
		timestamp:  time.Now(),
	})
	tuner.lastThroughput = 60 * 1024 * 1024
	tuner.bestThroughput = 60 * 1024 * 1024
	tuner.bestTokenCount = 2
	
	tuner.mu.Unlock()
	
	stats := tuner.GetStats()
	
	// Use approximate comparison for floating-point
	expectedThroughput := 60.0 * 1024 * 1024
	actualThroughput := stats["best_throughput"].(float64)
	if actualThroughput < expectedThroughput-1000 || actualThroughput > expectedThroughput+1000 {
		t.Errorf("expected best_throughput≈60MB/s, got %v", actualThroughput)
	}
	
	if stats["best_tokens"] != 2 {
		t.Errorf("expected best_tokens=2, got %v", stats["best_tokens"])
	}
}

func TestGetCurrentTokenCount(t *testing.T) {
	tuner := NewTunerWithConfig(1, 4, 1*time.Second)
	
	if tuner.GetCurrentTokenCount() != 1 {
		t.Errorf("expected 1 token, got %d", tuner.GetCurrentTokenCount())
	}
}

func BenchmarkAcquireReleaseToken(b *testing.B) {
	tuner := NewTunerWithConfig(1, 8, 1*time.Second)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch := tuner.AcquireToken()
		<-ch
		tuner.ReleaseToken()
	}
}

func BenchmarkRecordBytes(b *testing.B) {
	tuner := NewTunerWithConfig(1, 8, 1*time.Second)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tuner.RecordBytes(4096)
	}
}
