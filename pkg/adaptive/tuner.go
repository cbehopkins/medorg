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
	currentTokens  int
	lastTokenCount int // Token count from previous measurement
	tokenChan      chan struct{}

	// Throughput tracking
	bytesProcessed    atomic.Int64 // Total bytes processed in current interval
	lastCheckTime     time.Time    // Last time we checked/adjusted
	lastThroughput    float64      // Bytes per second from last interval
	bestThroughput    float64      // Highest throughput seen
	bestTokenCount    int          // Token count that gave best throughput
	throughputHistory []throughputSample

	// State machine
	lastAction         tokenAction // Last adjustment we made
	inflectionDetected bool        // Whether we've found the sweet spot

	// Control
	stopCh  chan struct{}
	doneCh  chan struct{}
	stopped atomic.Bool
}

type tokenAction int

const (
	actionNone tokenAction = iota
	actionAdded
	actionRemoved
)

type throughputSample struct {
	tokenCount int
	throughput float64
	timestamp  time.Time
}

// NewTuner creates a new adaptive tuner with sensible defaults
func NewTuner() *Tuner {
	cpuCount := runtime.NumCPU()
	t := &Tuner{
		minTokens:         1,
		maxTokens:         2 * cpuCount,
		checkInterval:     30 * time.Second,
		stabilizeDelay:    5 * time.Second,
		currentTokens:     cpuCount, // Start with CPU count
		lastTokenCount:    cpuCount, // Initialize for exploration detection
		tokenChan:         make(chan struct{}, cpuCount),
		lastCheckTime:     time.Now(),
		lastAction:        actionNone,
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
		throughputHistory: make([]throughputSample, 0, 100),
	}

	// Fill initial tokens
	for i := 0; i < t.currentTokens; i++ {
		t.tokenChan <- struct{}{}
	}

	return t
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
		minTokens:         minTokens,
		maxTokens:         maxTokens,
		checkInterval:     checkInterval,
		stabilizeDelay:    5 * time.Second,
		currentTokens:     minTokens,
		lastTokenCount:    minTokens, // Initialize for exploration detection
		tokenChan:         make(chan struct{}, maxTokens),
		lastCheckTime:     time.Now(),
		lastAction:        actionNone,
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
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

	for {
		select {
		case <-t.stopCh:
			return

		case <-ticker.C:
			// Check and potentially adjust based on measured throughput
			t.checkAndAdjust()
		}
	}
}

// checkAndAdjust evaluates current throughput and makes token adjustments
func (t *Tuner) checkAndAdjust() {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(t.lastCheckTime).Seconds()

	// Need reasonable time window to measure throughput
	if elapsed < 1.0 {
		return // Too soon to measure
	}

	// Calculate throughput (bytes per second)
	bytesProcessed := t.bytesProcessed.Swap(0)
	currentThroughput := float64(bytesProcessed) / elapsed

	// Skip adjustment if no work was done
	if bytesProcessed == 0 {
		// No bytes processed - might still be scanning or idle
		fmt.Printf("[Tuner] No bytes processed in %.1f seconds (tokens: %d)\n", elapsed, t.currentTokens)
		t.lastCheckTime = now
		return
	}

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

	// Print status update
	fmt.Printf("[Tuner] Checked throughput: %.2f MB/s (%.1f GB processed, tokens: %d)\n",
		currentThroughput/(1024*1024), float64(bytesProcessed)/(1024*1024*1024), t.currentTokens)

	// Make adjustment decision
	t.makeAdjustment()
}

// AdjustmentDecision represents what action should be taken
type AdjustmentDecision struct {
	Action        string // "increase", "decrease", "freeze", "hold"
	NewTokenCount int
	Reason        string
}

// makeDecisionWithHysteresisExploration is the core tuning logic - pure function for testing
// Uses a two-phase approach:
// 1. EXPLORATION: If we just increased tokens and throughput improved >1%, keep exploring
// 2. PLATEAU: Once throughput plateaus (diminishing returns), use hysteresis to avoid saturation
//
// Parameters:
//   - currentTokens: current token count
//   - minTokens, maxTokens: bounds
//   - bestThroughput: best throughput seen across all token counts
//   - bestTokenCount: token count that achieved bestThroughput
//   - lastTokenCount: token count from previous measurement
//   - inflectionDetected: if true, stop tuning
//   - throughputHistory: recent samples for comparison
func makeDecisionWithHysteresisExploration(currentTokens, minTokens, maxTokens, bestTokenCount, lastTokenCount int,
	bestThroughput float64, inflectionDetected bool, throughputHistory []throughputSample,
) AdjustmentDecision {
	const hysteresisMargin = 0.02     // 2% - hysteresis margin for plateau detection
	const explorationThreshold = 0.01 // 1% - minimum improvement to continue exploration

	if inflectionDetected {
		return AdjustmentDecision{
			Action:        "hold",
			NewTokenCount: currentTokens,
			Reason:        "inflection already detected",
		}
	}

	if len(throughputHistory) < 2 {
		if currentTokens >= maxTokens {
			return AdjustmentDecision{
				Action:        "hold",
				NewTokenCount: currentTokens,
				Reason:        "max tokens reached",
			}
		}
		return AdjustmentDecision{
			Action:        "increase",
			NewTokenCount: currentTokens + 1,
			Reason:        "need more samples to compare",
		}
	}

	prevSample := throughputHistory[len(throughputHistory)-2]
	currSample := throughputHistory[len(throughputHistory)-1]

	// Calculate throughput change percentage
	percentChange := (currSample.throughput - prevSample.throughput) / prevSample.throughput * 100

	// Check if we just increased tokens
	justIncreasedTokens := currSample.tokenCount > lastTokenCount

	// Decision logic:
	// 1. If we just added a token and throughput improved >1%, keep exploring
	// 2. If throughput declined >1%, we hit inflection - freeze
	// 3. Otherwise, apply hysteresis (need 2% improvement over best)

	if justIncreasedTokens && percentChange > explorationThreshold {
		// We added a token and it helped - keep exploring
		if currentTokens >= maxTokens {
			return AdjustmentDecision{
				Action:        "hold",
				NewTokenCount: currentTokens,
				Reason:        fmt.Sprintf("added token improved %.1f%% but at max tokens", percentChange),
			}
		}
		return AdjustmentDecision{
			Action:        "increase",
			NewTokenCount: currentTokens + 1,
			Reason:        fmt.Sprintf("explored new token count: %.1f%% improvement, trying more", percentChange),
		}
	} else if percentChange < -1 {
		// Throughput declined - likely inflection
		if currSample.tokenCount > bestTokenCount {
			return AdjustmentDecision{
				Action:        "freeze",
				NewTokenCount: bestTokenCount,
				Reason:        fmt.Sprintf("inflection: throughput dropped %.1f%%, best was %d tokens at %.2f MB/s", percentChange, bestTokenCount, bestThroughput),
			}
		}
		return AdjustmentDecision{
			Action:        "freeze",
			NewTokenCount: bestTokenCount,
			Reason:        fmt.Sprintf("throughput declined %.1f%%, freezing at best", percentChange),
		}
	}

	// We're in plateau - apply hysteresis
	percentAboveBest := (currSample.throughput - bestThroughput) / bestThroughput * 100

	if percentAboveBest > hysteresisMargin {
		// Found improvement over best
		if currentTokens >= maxTokens {
			return AdjustmentDecision{
				Action:        "hold",
				NewTokenCount: currentTokens,
				Reason:        fmt.Sprintf("%.1f%% above best but at max tokens", percentAboveBest),
			}
		}
		return AdjustmentDecision{
			Action:        "increase",
			NewTokenCount: currentTokens + 1,
			Reason:        fmt.Sprintf("plateau shows %.1f%% improvement above best, exploring further", percentAboveBest),
		}
	}

	return AdjustmentDecision{
		Action:        "hold",
		NewTokenCount: currentTokens,
		Reason:        fmt.Sprintf("stable plateau (%.1f%% above best, recent: %.1f%%)", percentAboveBest, percentChange),
	}
}

// makeDecisionOriginal is the original core tuning logic (without exploration) - for backward compatibility
func makeDecisionOriginal(currentTokens, minTokens, maxTokens, bestTokenCount int,
	inflectionDetected bool, throughputHistory []throughputSample,
) AdjustmentDecision {
	if inflectionDetected {
		return AdjustmentDecision{
			Action:        "hold",
			NewTokenCount: currentTokens,
			Reason:        "inflection already detected",
		}
	}

	if len(throughputHistory) < 2 {
		if currentTokens >= maxTokens {
			return AdjustmentDecision{
				Action:        "hold",
				NewTokenCount: currentTokens,
				Reason:        "max tokens reached",
			}
		}
		return AdjustmentDecision{
			Action:        "increase",
			NewTokenCount: currentTokens + 1,
			Reason:        "need more samples to compare",
		}
	}

	// Compare last throughput with previous
	prevSample := throughputHistory[len(throughputHistory)-2]
	currSample := throughputHistory[len(throughputHistory)-1]

	// Calculate throughput change percentage
	percentChange := (currSample.throughput - prevSample.throughput) / prevSample.throughput * 100

	if percentChange > 1 {
		// Throughput improved
		if currentTokens >= maxTokens {
			return AdjustmentDecision{
				Action:        "hold",
				NewTokenCount: currentTokens,
				Reason:        fmt.Sprintf("throughput improved %.1f%% but at max tokens", percentChange),
			}
		}
		return AdjustmentDecision{
			Action:        "increase",
			NewTokenCount: currentTokens + 1,
			Reason:        fmt.Sprintf("throughput improved %.1f%%", percentChange),
		}
	} else if percentChange < -1 {
		// Throughput decreased - inflection detected
		if currSample.tokenCount > bestTokenCount {
			return AdjustmentDecision{
				Action:        "freeze",
				NewTokenCount: bestTokenCount,
				Reason:        fmt.Sprintf("inflection detected: throughput dropped %.1f%%, best was %d tokens", percentChange, bestTokenCount),
			}
		}
		return AdjustmentDecision{
			Action:        "freeze",
			NewTokenCount: bestTokenCount,
			Reason:        fmt.Sprintf("throughput declined %.1f%%, freezing", percentChange),
		}
	}

	return AdjustmentDecision{
		Action:        "hold",
		NewTokenCount: currentTokens,
		Reason:        fmt.Sprintf("throughput stable (%.1f%% change)", percentChange),
	}
}

// makeDecision is backward compatibility wrapper - calls the original logic without exploration
func makeDecision(currentTokens, minTokens, maxTokens, bestTokenCount int,
	inflectionDetected bool, throughputHistory []throughputSample,
) AdjustmentDecision {
	return makeDecisionOriginal(currentTokens, minTokens, maxTokens, bestTokenCount, inflectionDetected, throughputHistory)
}

// makeDecisionWithHysteresis is an alias for the exploration-aware version (for test compatibility)
func makeDecisionWithHysteresis(currentTokens, minTokens, maxTokens, bestTokenCount, lastTokenCount int,
	bestThroughput float64, inflectionDetected bool, throughputHistory []throughputSample,
) AdjustmentDecision {
	return makeDecisionWithHysteresisExploration(currentTokens, minTokens, maxTokens, bestTokenCount, lastTokenCount,
		bestThroughput, inflectionDetected, throughputHistory)
}

// makeAdjustment determines if and how to adjust the token count
func (t *Tuner) makeAdjustment() {
	decision := makeDecisionWithHysteresisExploration(t.currentTokens, t.minTokens, t.maxTokens,
		t.bestTokenCount, t.lastTokenCount, t.bestThroughput, t.inflectionDetected, t.throughputHistory)

	// Track token count for next decision cycle
	t.lastTokenCount = t.currentTokens

	switch decision.Action {
	case "increase":
		t.tryIncreaseTokens()
	case "decrease":
		t.tryDecreaseTokens()
	case "freeze":
		t.freezeAtBestTokenCount()
		fmt.Printf("[Tuner] %s\n", decision.Reason)
	case "hold":
		// No action needed
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
		"current_tokens":      t.currentTokens,
		"best_tokens":         t.bestTokenCount,
		"best_throughput":     t.bestThroughput,
		"current_throughput":  t.lastThroughput,
		"inflection_detected": t.inflectionDetected,
		"samples_collected":   len(t.throughputHistory),
		"min_tokens":          t.minTokens,
		"max_tokens":          t.maxTokens,
		"bytes_processed":     t.bytesProcessed.Load(),
	}
}
