// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Unit tests for retryWithBackoff: retry classification, backoff
// cancellation, and OnRetryProgress boundary semantics. No QDB required.
package sink

import (
	"context"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failNTimes returns a push func failing with err for the first n calls,
// succeeding afterwards, counting invocations into *calls.
func failNTimes(n int, err error, calls *int) func() error {
	return func() error {
		*calls++
		if *calls <= n {
			return err
		}

		return nil
	}
}

// qdbErr builds the error shape writer.Push returns, so the retry loop is
// tested against the same chain it classifies in production.
func qdbErr(code qdb.ErrorType) error {
	return &qdb.Error{Code: code, Operation: "push"}
}

func TestRetryWithBackoffRetryableThenSuccess(t *testing.T) {
	pushes, progress := 0, 0
	// ErrTryAgain is retryable: the cluster asks for the same request later.
	push := failNTimes(2, qdbErr(qdb.ErrTryAgain), &pushes)

	err := retryWithBackoff(context.Background(), 3, time.Millisecond, 4*time.Millisecond,
		func() { progress++ }, push)

	require.NoError(t, err)
	assert.Equal(t, 3, pushes)
	// 2 retryable classifications + 2 post-backoff wakes.
	assert.Equal(t, 4, progress)
}

func TestRetryWithBackoffExhaustsAttempts(t *testing.T) {
	pushes, progress := 0, 0
	push := failNTimes(3, qdbErr(qdb.ErrTimeout), &pushes)

	err := retryWithBackoff(context.Background(), 3, time.Millisecond, 4*time.Millisecond,
		func() { progress++ }, push)

	require.Error(t, err)
	assert.ErrorIs(t, err, qdb.ErrTimeout)
	assert.True(t, qdb.IsClusterUnavailable(err))
	assert.Equal(t, 3, pushes)
	// No progress call after the final classification: 2 + 2.
	assert.Equal(t, 4, progress)
}

func TestRetryWithBackoffNonRetryableFailsImmediately(t *testing.T) {
	pushes, progress := 0, 0
	push := failNTimes(3, qdbErr(qdb.ErrInvalidArgument), &pushes)

	err := retryWithBackoff(context.Background(), 3, time.Millisecond, 4*time.Millisecond,
		func() { progress++ }, push)

	require.Error(t, err)
	assert.ErrorIs(t, err, qdb.ErrInvalidArgument)
	assert.Equal(t, 1, pushes)
	assert.Equal(t, 0, progress)
}

func TestRetryWithBackoffNilProgressCallback(t *testing.T) {
	pushes := 0
	push := failNTimes(2, qdbErr(qdb.ErrTryAgain), &pushes)

	assert.NotPanics(t, func() {
		err := retryWithBackoff(context.Background(), 3, time.Millisecond, 4*time.Millisecond, nil, push)
		assert.NoError(t, err)
	})
	assert.Equal(t, 3, pushes)
}

func TestRetryWithBackoffContextCancelledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pushes := 0
	push := func() error {
		pushes++
		cancel() // cancellation lands while the loop sleeps before attempt 2

		return qdbErr(qdb.ErrTimeout)
	}

	start := time.Now()
	err := retryWithBackoff(ctx, 3, 10*time.Second, 20*time.Second, nil, push)

	require.Error(t, err)
	assert.ErrorContains(t, err, "context cancelled during retry backoff")
	assert.Equal(t, 1, pushes)
	assert.Less(t, time.Since(start), time.Second)
}
