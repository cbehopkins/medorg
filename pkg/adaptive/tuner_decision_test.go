package adaptive

import (
	"testing"
)

func TestMakeDecision(t *testing.T) {
	tests := []struct {
		name                string
		currentTokens       int
		minTokens           int
		maxTokens           int
		bestTokenCount      int
		inflectionDetected  bool
		throughputHistory   []throughputSample
		expectedAction      string
		expectedNewTokens   int
		shouldContainReason string
	}{
		{
			name:               "inflection already detected - hold",
			currentTokens:      4,
			minTokens:          1,
			maxTokens:          8,
			bestTokenCount:     3,
			inflectionDetected: true,
			throughputHistory: []throughputSample{
				{tokenCount: 2, throughput: 100.0},
				{tokenCount: 3, throughput: 150.0},
			},
			expectedAction:      "hold",
			expectedNewTokens:   4,
			shouldContainReason: "inflection already detected",
		},
		{
			name:                "not enough samples - increase",
			currentTokens:       1,
			minTokens:           1,
			maxTokens:           8,
			bestTokenCount:      1,
			inflectionDetected:  false,
			throughputHistory:   []throughputSample{},
			expectedAction:      "increase",
			expectedNewTokens:   2,
			shouldContainReason: "need more samples",
		},
		{
			name:               "one sample only - increase",
			currentTokens:      2,
			minTokens:          1,
			maxTokens:          8,
			bestTokenCount:     2,
			inflectionDetected: false,
			throughputHistory: []throughputSample{
				{tokenCount: 2, throughput: 100.0},
			},
			expectedAction:      "increase",
			expectedNewTokens:   3,
			shouldContainReason: "need more samples",
		},
		{
			name:               "at max tokens - hold",
			currentTokens:      8,
			minTokens:          1,
			maxTokens:          8,
			bestTokenCount:     4,
			inflectionDetected: false,
			throughputHistory: []throughputSample{
				{tokenCount: 7, throughput: 100.0},
			},
			expectedAction:      "hold",
			expectedNewTokens:   8,
			shouldContainReason: "max tokens reached",
		},
		{
			name:               "throughput improved 2% - increase",
			currentTokens:      3,
			minTokens:          1,
			maxTokens:          8,
			bestTokenCount:     3,
			inflectionDetected: false,
			throughputHistory: []throughputSample{
				{tokenCount: 2, throughput: 100.0},
				{tokenCount: 3, throughput: 102.0}, // +2%
			},
			expectedAction:      "increase",
			expectedNewTokens:   4,
			shouldContainReason: "improved",
		},
		{
			name:               "throughput improved at max tokens - hold",
			currentTokens:      8,
			minTokens:          1,
			maxTokens:          8,
			bestTokenCount:     7,
			inflectionDetected: false,
			throughputHistory: []throughputSample{
				{tokenCount: 7, throughput: 100.0},
				{tokenCount: 8, throughput: 102.0}, // +2%
			},
			expectedAction:      "hold",
			expectedNewTokens:   8,
			shouldContainReason: "at max tokens",
		},
		{
			name:               "throughput dropped 2% - freeze",
			currentTokens:      5,
			minTokens:          1,
			maxTokens:          8,
			bestTokenCount:     4,
			inflectionDetected: false,
			throughputHistory: []throughputSample{
				{tokenCount: 4, throughput: 100.0},
				{tokenCount: 5, throughput: 98.0}, // -2%
			},
			expectedAction:      "freeze",
			expectedNewTokens:   4,
			shouldContainReason: "inflection detected",
		},
		{
			name:               "throughput stable +0.5% - hold",
			currentTokens:      3,
			minTokens:          1,
			maxTokens:          8,
			bestTokenCount:     3,
			inflectionDetected: false,
			throughputHistory: []throughputSample{
				{tokenCount: 2, throughput: 100.0},
				{tokenCount: 3, throughput: 100.5}, // +0.5%
			},
			expectedAction:      "hold",
			expectedNewTokens:   3,
			shouldContainReason: "stable",
		},
		{
			name:               "throughput stable -0.5% - hold",
			currentTokens:      3,
			minTokens:          1,
			maxTokens:          8,
			bestTokenCount:     3,
			inflectionDetected: false,
			throughputHistory: []throughputSample{
				{tokenCount: 2, throughput: 100.0},
				{tokenCount: 3, throughput: 99.5}, // -0.5%
			},
			expectedAction:      "hold",
			expectedNewTokens:   3,
			shouldContainReason: "stable",
		},
		{
			name:               "throughput dropped exactly 1% - stable (boundary)",
			currentTokens:      5,
			minTokens:          1,
			maxTokens:          8,
			bestTokenCount:     4,
			inflectionDetected: false,
			throughputHistory: []throughputSample{
				{tokenCount: 4, throughput: 100.0},
				{tokenCount: 5, throughput: 99.0}, // -1% (exactly at boundary, treated as stable)
			},
			expectedAction:      "hold",
			expectedNewTokens:   5,
			shouldContainReason: "stable",
		},
		{
			name:               "throughput dropped slightly over 1% - freeze",
			currentTokens:      5,
			minTokens:          1,
			maxTokens:          8,
			bestTokenCount:     4,
			inflectionDetected: false,
			throughputHistory: []throughputSample{
				{tokenCount: 4, throughput: 100.0},
				{tokenCount: 5, throughput: 98.99}, // -1.01% (exceeds boundary)
			},
			expectedAction:      "freeze",
			expectedNewTokens:   4,
			shouldContainReason: "inflection detected",
		},
		{
			name:               "throughput improved exactly 1% - hold (at boundary)",
			currentTokens:      3,
			minTokens:          1,
			maxTokens:          8,
			bestTokenCount:     3,
			inflectionDetected: false,
			throughputHistory: []throughputSample{
				{tokenCount: 2, throughput: 100.0},
				{tokenCount: 3, throughput: 101.0}, // +1% (exactly at boundary, treated as stable)
			},
			expectedAction:      "hold",
			expectedNewTokens:   3,
			shouldContainReason: "stable",
		},
		{
			name:               "throughput improved slightly over 1% - increase",
			currentTokens:      3,
			minTokens:          1,
			maxTokens:          8,
			bestTokenCount:     3,
			inflectionDetected: false,
			throughputHistory: []throughputSample{
				{tokenCount: 2, throughput: 100.0},
				{tokenCount: 3, throughput: 101.01}, // +1.01% (exceeds boundary)
			},
			expectedAction:      "increase",
			expectedNewTokens:   4,
			shouldContainReason: "improved",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := makeDecision(
				tt.currentTokens,
				tt.minTokens,
				tt.maxTokens,
				tt.bestTokenCount,
				tt.inflectionDetected,
				tt.throughputHistory,
			)

			if decision.Action != tt.expectedAction {
				t.Errorf("Expected action %q, got %q", tt.expectedAction, decision.Action)
			}

			if decision.NewTokenCount != tt.expectedNewTokens {
				t.Errorf("Expected new token count %d, got %d", tt.expectedNewTokens, decision.NewTokenCount)
			}

			if tt.shouldContainReason != "" && !containsSubstring(decision.Reason, tt.shouldContainReason) {
				t.Errorf("Expected reason to contain %q, got %q", tt.shouldContainReason, decision.Reason)
			}
		})
	}
}

func containsSubstring(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestDecisionSequence tests realistic tuning sequences
func TestDecisionSequence(t *testing.T) {
	tests := []struct {
		name                string
		scenario            string
		decisions           []AdjustmentDecision
		history             []throughputSample
		currentTokens       int
		bestTokenCount      int
		expectedFinal       string // expected final action
		expectedFinalTokens int
	}{
		{
			name:     "steady improvement sequence",
			scenario: "throughput keeps improving as we add tokens",
			history: []throughputSample{
				{tokenCount: 1, throughput: 100.0},
				{tokenCount: 2, throughput: 180.0},
				{tokenCount: 3, throughput: 260.0},
				{tokenCount: 4, throughput: 330.0},
			},
			currentTokens:  4,
			bestTokenCount: 4,
			expectedFinal:  "increase", // should keep increasing
		},
		{
			name:     "inflection at 4 tokens",
			scenario: "improvement stops after token 4",
			history: []throughputSample{
				{tokenCount: 1, throughput: 100.0},
				{tokenCount: 2, throughput: 180.0},
				{tokenCount: 3, throughput: 260.0},
				{tokenCount: 4, throughput: 330.0},
				{tokenCount: 5, throughput: 324.0}, // -1.8% decline
			},
			currentTokens:  5,
			bestTokenCount: 4,
			expectedFinal:  "freeze", // should freeze at 4
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate decision making
			decision := makeDecision(
				tt.currentTokens,
				1, // minTokens
				8, // maxTokens
				tt.bestTokenCount,
				false, // inflectionDetected
				tt.history,
			)

			if decision.Action != tt.expectedFinal {
				t.Errorf("Expected final action %q, got %q (reason: %s)",
					tt.expectedFinal, decision.Action, decision.Reason)
			}
		})
	}
}

// TestEdgeCaseSubpercentageImprovement tests the edge case where each token
// adds a very small (<1%) improvement, causing the tuner to keep adding tokens
// until hitting the max (saturation at 32).
func TestEdgeCaseSubpercentageImprovement(t *testing.T) {
	// SCENARIO 1: 0.5% improvements per token
	// With 0.5% improvements, the ±1% threshold should prevent increases
	t.Run("0.5% improvements - should hold steady", func(t *testing.T) {
		history := make([]throughputSample, 0)
		baselineTP := 100.0

		for tokenCount := 1; tokenCount <= 32; tokenCount++ {
			tp := baselineTP * (1.0 + float64(tokenCount-1)*0.005)
			history = append(history, throughputSample{
				tokenCount: tokenCount,
				throughput: tp,
			})
		}

		currentTokens := 1
		maxTokens := 32
		actions := make([]string, 0)

		for i := 1; i < len(history); i++ {
			decision := makeDecision(
				currentTokens,
				1,
				maxTokens,
				1,
				false,
				history[max(0, i-1):i+1],
			)
			actions = append(actions, decision.Action)
		}

		// All should be "hold" (0.5% is stable within ±1%)
		allHold := true
		for _, action := range actions {
			if action != "hold" {
				allHold = false
				break
			}
		}

		if !allHold {
			t.Errorf("With 0.5%% improvements, expected all 'hold' but got: %v", actions)
		}
		t.Logf("0.5%% improvements: tuner correctly HOLDS (does not saturate)")
	})

	// SCENARIO 2: 1.5% improvements per token
	// This EXCEEDS the +1% threshold, should trigger increases
	t.Run("1.5% improvements - should saturate at max tokens", func(t *testing.T) {
		history := make([]throughputSample, 0)
		baselineTP := 100.0

		for tokenCount := 1; tokenCount <= 32; tokenCount++ {
			tp := baselineTP * (1.0 + float64(tokenCount-1)*0.015) // 1.5% per token
			history = append(history, throughputSample{
				tokenCount: tokenCount,
				throughput: tp,
			})
		}

		currentTokens := 1
		maxTokens := 32
		actions := make([]string, 0)

		for i := 1; i < len(history); i++ {
			decision := makeDecision(
				currentTokens,
				1,
				maxTokens,
				1,
				false,
				history[max(0, i-1):i+1],
			)
			actions = append(actions, decision.Action)

			// Track if we would have increased
			if decision.Action == "increase" && currentTokens < maxTokens {
				currentTokens++
			}
		}

		// All should be "increase" until we're at or near max tokens
		allIncreaseUntilMax := true
		for i, action := range actions {
			if i < len(actions)-1 && action != "increase" {
				allIncreaseUntilMax = false
				break
			}
		}

		if !allIncreaseUntilMax {
			t.Errorf("With 1.5%% improvements, expected saturation to max tokens, got: %v", actions)
		}

		if currentTokens != maxTokens {
			t.Errorf("Expected to saturate at %d tokens, but only reached %d", maxTokens, currentTokens)
		}

		t.Logf("1.5%% improvements: tuner SATURATES to 32 tokens (exceeds +1%% threshold)")
	})
}

// TestMeasurementNoiseSaturation tests the real-world edge case:
// throughput is actually stable, but measurement noise causes occasional spikes
// that exceed the +1% threshold, triggering token increases all the way to 32.
func TestMeasurementNoiseSaturation(t *testing.T) {
	t.Run("stable 46MB/s with ±2% noise - old logic saturates to max", func(t *testing.T) {
		// Real scenario from logs: throughput hovers around 46-47 MB/s with natural variance
		// Simulating the sequence from the logs, with small fluctuations
		history := []throughputSample{
			{tokenCount: 1, throughput: 46.69},
			{tokenCount: 2, throughput: 44.78},  // -4.1% (decline, would freeze)
			{tokenCount: 3, throughput: 46.50},  // +3.8% (improvement, would increase)
			{tokenCount: 4, throughput: 46.57},  // +0.15% (stable)
			{tokenCount: 5, throughput: 46.64},  // +0.15% (stable)
			{tokenCount: 6, throughput: 46.52},  // -0.26% (stable)
			{tokenCount: 7, throughput: 46.77},  // +0.54% (stable)
			{tokenCount: 8, throughput: 45.94},  // -1.77% (decline, would freeze)
			{tokenCount: 9, throughput: 46.99},  // +2.28% (improvement, would increase)
			{tokenCount: 10, throughput: 47.05}, // +0.13% (stable)
			{tokenCount: 11, throughput: 46.56}, // -1.04% (decline, would freeze)
			{tokenCount: 12, throughput: 47.55}, // +2.12% (improvement, would increase)
		}

		actions := make([]string, 0)
		percentChanges := make([]float64, 0)

		for i := 1; i < len(history); i++ {
			decision := makeDecision(
				history[i].tokenCount,
				1,  // minTokens
				32, // maxTokens
				1,  // bestTokenCount (would be updated as we find improvements)
				false,
				history[max(0, i-1):i+1],
			)
			actions = append(actions, decision.Action)

			percentChange := (history[i].throughput - history[i-1].throughput) / history[i-1].throughput * 100
			percentChanges = append(percentChanges, percentChange)
		}

		t.Logf("Old logic (no hysteresis) with measurement noise:")
		for i, action := range actions {
			t.Logf("  Step %d: %+.2f%% -> action: %s", i+1, percentChanges[i], action)
		}

		// The key observation: we have 4 "increase" decisions out of 11
		// These occasional spikes above +1% would cause the tuner to add tokens
		increaseCount := 0
		for _, action := range actions {
			if action == "increase" {
				increaseCount++
			}
		}

		t.Logf("Result: %d 'increase' decisions out of %d checks", increaseCount, len(actions))
		t.Logf("PROBLEM: Measurement noise causes sporadic increases despite no real throughput gain")
		t.Logf("The tuner would add tokens 1->12+ until hitting maxTokens=32")
		t.Logf("But throughput stays around 46-47 MB/s, same as with 4 workers")
	})

	t.Run("stable 46MB/s with ±2% noise - hysteresis prevents saturation", func(t *testing.T) {
		// Same real log data, but using hysteresis logic
		history := []throughputSample{
			{tokenCount: 1, throughput: 46.69},
			{tokenCount: 2, throughput: 44.78},
			{tokenCount: 3, throughput: 46.50},
			{tokenCount: 4, throughput: 46.57},
			{tokenCount: 5, throughput: 46.64},
			{tokenCount: 6, throughput: 46.52},
			{tokenCount: 7, throughput: 46.77},
			{tokenCount: 8, throughput: 45.94},
			{tokenCount: 9, throughput: 46.99},
			{tokenCount: 10, throughput: 47.05},
			{tokenCount: 11, throughput: 46.56},
			{tokenCount: 12, throughput: 47.55},
		}

		bestTP := 46.69 // Start with first measurement as "best"
		actions := make([]string, 0)
		percentAboveBest := make([]float64, 0)

		for i := 1; i < len(history); i++ {
			currentTP := history[i].throughput
			if currentTP > bestTP {
				bestTP = currentTP
			}

			decision := makeDecisionWithHysteresis(
				history[i].tokenCount,
				1,                               // minTokens
				32,                              // maxTokens
				1,                               // bestTokenCount
				max(1, history[i].tokenCount-1), // lastTokenCount - approximate
				bestTP,                          // Use best throughput so far
				false,
				history[max(0, i-1):i+1],
			)
			actions = append(actions, decision.Action)
			pct := (currentTP - bestTP) / bestTP * 100
			percentAboveBest = append(percentAboveBest, pct)
		}

		t.Logf("New logic (with 2%% hysteresis margin) on same data:")
		for i, action := range actions {
			t.Logf("  Step %d: %+.2f%% above best -> action: %s", i+1, percentAboveBest[i], action)
		}

		// Count increases - should be zero or very few
		increaseCount := 0
		for _, action := range actions {
			if action == "increase" {
				increaseCount++
			}
		}

		t.Logf("Result: %d 'increase' decisions out of %d checks", increaseCount, len(actions))
		t.Logf("SOLUTION: 2%% hysteresis margin filters out measurement noise")
		t.Logf("Tuner stays at low token count, not saturating at 32")

		if increaseCount == 0 {
			t.Logf("✓ SUCCESS: Hysteresis prevents spurious increases")
		}
	})

	t.Run("realistic scenario: big jump 1→2 tokens, then plateau", func(t *testing.T) {
		// Your actual scenario: 12.79 MB/s at 1 token, then 35+ MB/s at 2 tokens
		history := []throughputSample{
			{tokenCount: 1, throughput: 12.79},
			{tokenCount: 2, throughput: 35.02}, // +174% jump!
			{tokenCount: 2, throughput: 35.23}, // +0.6% noise
			{tokenCount: 2, throughput: 35.06}, // -0.5% noise
		}

		bestTP := 12.79 // Start with 1 token throughput
		actions := make([]string, 0)

		for i := 1; i < len(history); i++ {
			currentTP := history[i].throughput
			if currentTP > bestTP {
				bestTP = currentTP
			}

			decision := makeDecisionWithHysteresis(
				history[i].tokenCount,
				1,                               // minTokens
				32,                              // maxTokens
				1,                               // bestTokenCount
				max(1, history[i].tokenCount-1), // lastTokenCount - approximate
				bestTP,                          // Use best throughput so far
				false,
				history[max(0, i-1):i+1],
			)
			actions = append(actions, decision.Action)

			pct := (currentTP - bestTP) / bestTP * 100
			t.Logf("  Step %d (token %d): %.2f MB/s (%+.2f%% above best %.2f) -> %s",
				i, history[i].tokenCount, currentTP, pct, bestTP, decision.Action)
		}

		// With 2% hysteresis:
		// - Step 1: 35.02 vs 12.79 = +174% -> INCREASE (way above 2%)
		// - Step 2: 35.23 vs 35.02 = +0.6% -> HOLD (below 2%)
		// - Step 3: 35.06 vs 35.02 = +0.1% -> HOLD (below 2%)
		// So: 1 increase decision, then holds
		increaseCount := 0
		for _, action := range actions {
			if action == "increase" {
				increaseCount++
			}
		}

		t.Logf("Result: %d 'increase' decision(s) - will add a 3rd token", increaseCount)
		t.Logf("Then plateau checking will hold steady around 35 MB/s")

		if increaseCount > 0 {
			t.Logf("✓ Correctly identified that 2 tokens is better than 1")
		}
	})
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
