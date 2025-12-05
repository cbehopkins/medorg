package adaptive

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Tuner adaptively adjusts concurrency tokens based on throughput performance.
// It monitors bits/second during operations and adjusts the token count to find
// optimal performance, detecting inflection points where more tokens reduce throughput.
type Tuner struct {
	mu sync.Mutex

	// Configuration
	minTokens      int           // Minimum tokens (default: 1)
	maxTokens      int           // Maximum tokens (default: 2 * CPU count)
	checkInterval  time.Duration // How often to check and adjust (default: 30s)
	stabilizeDelay time.Duration // How long to let system stabilize before adjusting (default: 5s)

	// Current state
	currentTokens int
	tokenChan     chan struct{}

	// Throughput tracking
	bytesProcessed   atomic.Int64  // Total bytes processed in current interval
	lastCheckTime    time.Time     // Last time we checked/adjusted
	lastThroughput   float64       // Bytes per second from last interval
	bestThroughput   float64       // Highest throughput seen
	bestTokenCount   int           // Token count that gave best throughput
	throughputHistory []throughputSample

	// State machine
	lastAction  tokenAction      // Last adjustment we made
	inflectionDetected bool       // Whether we've found the sweet spot

	// Control
	stopCh chan struct{}
	doneCh chan struct{}
	stopped atomic.Bool
}

type tokenAction int

const (
	actionNone tokenAction = iota
	actionAdded
	actionRemoved
)

type throughputSample struct {
	tokenCount  int
	throughput  float64
	timestamp   time.Time
}

// NewTuner creates a new adaptive tuner with sensible defaults
func NewTuner() *Tuner {
	cpuCount := runtime.NumCPU()
	return &Tuner{
		minTokens:     1,
		maxTokens:     2 * cpuCount,
		checkInterval: 30 * time.Second,
		stabilizeDelay: 5 * time.Second,
		currentTokens: cpuCount, // Start with CPU count
		tokenChan:     make(chan struct{}, cpuCount),
		lastCheckTime: time.Now(),
		lastAction:    actionNone,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		throughputHistory: make([]throughputSample, 0, 100),
	}
}

// NewTunerWithConfig creates a tuner with custom configuration
func NewTunerWithConfig(minTokens, maxTokens int, checkInterval time.Duration) *Tuner {
	if minTokens < 1 {
		minTokens = 1
	}
	if maxTokens < minTokens {
		maxTokens = minTokens
	}
	if checkInterval < 5*time.Second {
		checkInterval = 5 * time.Second
	}

	t := &Tuner{
		minTokens:      minTokens,
		maxTokens:      maxTokens,
		checkInterval:  checkInterval,
		stabilizeDelay: 5 * time.Second,
		currentTokens:  minTokens,
		tokenChan:      make(chan struct{}, maxTokens),
		lastCheckTime:  time.Now(),
		lastAction:     actionNone,
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
		throughputHistory: make([]throughputSample, 0, 100),
	}

	// Fill initial tokens
	for i := 0; i < t.currentTokens; i++ {
		t.tokenChan <- struct{}{}
	}

	return t
}

// Start begins the monitoring and adjustment goroutine
func (t *Tuner) Start() {
	go t.monitoringLoop()
}

// Stop gracefully stops the tuner
func (t *Tuner) Stop() {
	if t.stopped.CompareAndSwap(false, true) {
		close(t.stopCh)
		<-t.doneCh
	}
}

// AcquireToken gets a token, blocking until one is available
func (t *Tuner) AcquireToken() <-chan struct{} {
	ch := make(chan struct{}, 1)
	go func() {
		<-t.tokenChan
		ch <- struct{}{}
	}()
	return ch
}

// ReleaseToken returns a token to the pool
func (t *Tuner) ReleaseToken() {
	select {
	case t.tokenChan <- struct{}{}:
	default:
		// Channel full, shouldn't happen but handle gracefully
	}
}

// RecordBytes records bytes processed (called by consumers during file processing)
func (t *Tuner) RecordBytes(bytes int64) {
	t.bytesProcessed.Add(bytes)
}

// GetCurrentTokenCount returns the current number of tokens
func (t *Tuner) GetCurrentTokenCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.currentTokens
}

// monitoringLoop periodically checks throughput and adjusts tokens
func (t *Tuner) monitoringLoop() {
	defer close(t.doneCh)

	ticker := time.NewTicker(t.checkInterval)
	defer ticker.Stop()

	// Timer to wait for stabilization before adjusting
	stabilizeTimer := time.NewTimer(t.stabilizeDelay)
	defer stabilizeTimer.Stop()
	stabilizeTimer.Stop() // Don't start immediately

	stabilizeArmed := false

	for {
		select {
		case <-t.stopCh:
			return

		case <-stabilizeTimer.C:
			// System has stabilized, check throughput and potentially adjust
			t.checkAndAdjust()
			// Prepare for next stabilization period
			stabilizeArmed = false
			stabilizeTimer.Reset(t.stabilizeDelay)

		case <-ticker.C:
			// Arm the stabilization timer if not already armed
			if !stabilizeArmed {
				stabilizeTimer.Reset(t.stabilizeDelay)
				stabilizeArmed = true
			}
		}
	}
}

// checkAndAdjust evaluates current throughput and makes token adjustments
func (t *Tuner) checkAndAdjust() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(t.lastCheckTime).Seconds()
	if elapsed < 1 {
		return // Too soon to measure
	}

	// Calculate throughput
	bytesProcessed := t.bytesProcessed.Swap(0)
	currentThroughput := float64(bytesProcessed) / elapsed

	// Record sample
	t.throughputHistory = append(t.throughputHistory, throughputSample{
		tokenCount: t.currentTokens,
		throughput: currentThroughput,
		timestamp:  now,
	})

	// Update best throughput if this is better
	if currentThroughput > t.bestThroughput {
		t.bestThroughput = currentThroughput
		t.bestTokenCount = t.currentTokens
		t.inflectionDetected = false // Reset inflection detection
	}

	t.lastThroughput = currentThroughput
	t.lastCheckTime = now

	// Make adjustment decision
	t.makeAdjustment()
}

// makeAdjustment determines if and how to adjust the token count
func (t *Tuner) makeAdjustment() {
	if t.inflectionDetected {
		// Already found sweet spot, don't adjust
		return
	}

	if len(t.throughputHistory) < 2 {
		// Need at least 2 samples to compare
		t.tryIncreaseTokens()
		return
	}

	// Compare last throughput with previous
	prevSample := t.throughputHistory[len(t.throughputHistory)-2]
	currSample := t.throughputHistory[len(t.throughputHistory)-1]

	// Calculate throughput change percentage
	percentChange := (currSample.throughput - prevSample.throughput) / prevSample.throughput * 100

	if percentChange > 5 {
		// Throughput improved, continue in same direction
		if t.lastAction == actionAdded || t.lastAction == actionNone {
			t.tryIncreaseTokens()
		} else {
			// Was removing, now improving - this is odd, try adding instead
			t.tryIncreaseTokens()
		}
	} else if percentChange < -5 {
		// Throughput decreased - inflection detected
		if currSample.tokenCount > t.bestTokenCount {
			// We added a token and it made things worse
			fmt.Printf("[Tuner] Inflection detected: throughput dropped %.1f%% with %d tokens (best was %d tokens @ %.2f MB/s)\n",
				percentChange, currSample.tokenCount, t.bestTokenCount, t.bestThroughput/(1024*1024))
			t.freezeAtBestTokenCount()
		} else {
			// Unexpected, but be conservative
			t.freezeAtBestTokenCount()
		}
	} else {
		// Throughput relatively stable, try adding more
		t.tryIncreaseTokens()
	}
}

// tryIncreaseTokens attempts to add a token to the pool
func (t *Tuner) tryIncreaseTokens() {
	if t.currentTokens >= t.maxTokens {
		fmt.Printf("[Tuner] Reached maximum tokens (%d), stopping increases\n", t.maxTokens)
		t.inflectionDetected = true
		return
	}

	t.currentTokens++
	t.tokenChan <- struct{}{} // Add token to pool
	fmt.Printf("[Tuner] Increased tokens to %d (throughput: %.2f MB/s)\n",
		t.currentTokens, t.lastThroughput/(1024*1024))
	t.lastAction = actionAdded
}

// tryDecreaseTokens removes a token from the pool
func (t *Tuner) tryDecreaseTokens() {
	if t.currentTokens <= t.minTokens {
		fmt.Printf("[Tuner] Reached minimum tokens (%d), stopping decreases\n", t.minTokens)
		t.inflectionDetected = true
		return
	}

	// Drain one token from the pool
	select {
	case <-t.tokenChan:
		t.currentTokens--
		fmt.Printf("[Tuner] Decreased tokens to %d (throughput: %.2f MB/s)\n",
			t.currentTokens, t.lastThroughput/(1024*1024))
		t.lastAction = actionRemoved
	default:
		// Token not immediately available, skip this adjustment
	}
}

// freezeAtBestTokenCount locks in the best token count found
func (t *Tuner) freezeAtBestTokenCount() {
	if t.currentTokens == t.bestTokenCount {
		// Already at best, just mark as detected
		t.inflectionDetected = true
		return
	}

	// Adjust to best count
	delta := t.bestTokenCount - t.currentTokens
	if delta > 0 {
		for i := 0; i < delta; i++ {
			t.tokenChan <- struct{}{}
		}
	} else if delta < 0 {
		for i := 0; i < -delta; i++ {
			select {
			case <-t.tokenChan:
			default:
				// Channel empty, can't drain more
				return
			}
		}
	}

	t.currentTokens = t.bestTokenCount
	t.inflectionDetected = true
	fmt.Printf("[Tuner] FROZEN at %d tokens (best throughput: %.2f MB/s)\n",
		t.bestTokenCount, t.bestThroughput/(1024*1024))
}

// GetStats returns diagnostic information about tuning performance
func (t *Tuner) GetStats() map[string]interface{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	return map[string]interface{}{
		"current_tokens":    t.currentTokens,
		"best_tokens":       t.bestTokenCount,
		"best_throughput":   t.bestThroughput,
		"current_throughput": t.lastThroughput,
		"inflection_detected": t.inflectionDetected,
		"samples_collected":  len(t.throughputHistory),
		"min_tokens":        t.minTokens,
		"max_tokens":        t.maxTokens,
	}
}
