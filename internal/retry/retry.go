package retry

import (
	"errors"
	"math/rand/v2"
	"time"
)

type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }

func (e *PermanentError) Unwrap() error { return e.Err }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

type Policy struct {
	Base        time.Duration
	Max         time.Duration
	MaxAttempts int
}

func DefaultPolicy(maxRetries int) Policy {
	return Policy{Base: 500 * time.Millisecond, Max: 5 * time.Minute, MaxAttempts: maxRetries}
}

func (p Policy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	backoff := p.Base
	for i := 1; i < attempt && backoff < p.Max; i++ {
		backoff *= 2
	}
	if backoff > p.Max {
		backoff = p.Max
	}
	jitter := rand.Float64() * 0.3 * float64(backoff)
	return backoff + time.Duration(jitter)
}

func (p Policy) NextDelay(attempt int) time.Duration {
	return p.Backoff(attempt)
}
