package producer

import (
	"context"
	"math"
	"time"
)

// retrier executes a function with exponential backoff retry.
type retrier struct {
	cfg RetryConfig
}

func newRetrier(cfg RetryConfig) *retrier {
	return &retrier{cfg: cfg}
}

// Do executes fn up to MaxAttempts times with exponential backoff between attempts.
// Returns nil if fn succeeds on any attempt.
// Returns the last error if all attempts are exhausted.
// Respects context cancellation — stops retrying if ctx is done.
func (r *retrier) Do(ctx context.Context, fn func() error) error {
	var lastErr error

	for attempt := 1; attempt <= r.cfg.MaxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		// last attempt — don't sleep, just return error
		if attempt == r.cfg.MaxAttempts {
			break
		}

		backoff := r.backoffDuration(attempt)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			// continue to next attempt
		}
	}

	return lastErr
}

// backoffDuration calculates the exponential backoff for a given attempt number.
// Uses: min(InitialBackoff * Multiplier^(attempt-1), MaxBackoff)
func (r *retrier) backoffDuration(attempt int) time.Duration {
	backoff := float64(r.cfg.InitialBackoff) * math.Pow(r.cfg.BackoffMultiplier, float64(attempt-1))
	if backoff > float64(r.cfg.MaxBackoff) {
		backoff = float64(r.cfg.MaxBackoff)
	}
	return time.Duration(backoff)
}
