package batch

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatcher_SizeBased(t *testing.T) {
	// Create input channel
	input := make(chan []byte, 10)

	// Create batcher with size 3
	batcher, output := NewBatcher(3, 1*time.Second, input)

	// Start batcher
	ctx := context.Background()
	err := batcher.Start(ctx)
	require.NoError(t, err)

	// Send 5 messages
	messages := [][]byte{[]byte("msg1"), []byte("msg2"), []byte("msg3"), []byte("msg4"), []byte("msg5")}
	go func() {
		defer close(input)
		for _, msg := range messages {
			input <- msg
		}
	}()

	// Expect first batch with 3 messages
	batch1 := <-output
	assert.Equal(t, 3, batch1.Size)
	assert.Len(t, batch1.Messages, 3)
	assert.Equal(t, []byte("msg1"), batch1.Messages[0])
	assert.Equal(t, []byte("msg2"), batch1.Messages[1])
	assert.Equal(t, []byte("msg3"), batch1.Messages[2])

	// Expect second batch with 2 messages
	batch2 := <-output
	assert.Equal(t, 2, batch2.Size)
	assert.Len(t, batch2.Messages, 2)
	assert.Equal(t, []byte("msg4"), batch2.Messages[0])
	assert.Equal(t, []byte("msg5"), batch2.Messages[1])

	// Channel should be closed
	_, ok := <-output
	assert.False(t, ok)
}

func TestBatcher_TimeoutBased(t *testing.T) {
	// Create input channel
	input := make(chan []byte, 10)

	// Create batcher with small timeout
	batcher, output := NewBatcher(10, 50*time.Millisecond, input)

	// Start batcher
	ctx := context.Background()
	err := batcher.Start(ctx)
	require.NoError(t, err)

	// Send 2 messages (less than batch size)
	go func() {
		input <- []byte("msg1")
		input <- []byte("msg2")
		// Don't close immediately to test timeout
		time.Sleep(100 * time.Millisecond)
		close(input)
	}()

	// Expect batch due to timeout
	batch := <-output
	assert.Equal(t, 2, batch.Size)
	assert.Len(t, batch.Messages, 2)
	assert.Equal(t, []byte("msg1"), batch.Messages[0])
	assert.Equal(t, []byte("msg2"), batch.Messages[1])
}

func TestBatcher_ContextCancellation(t *testing.T) {
	// Create input channel
	input := make(chan []byte, 10)

	// Create batcher
	batcher, output := NewBatcher(10, 1*time.Second, input)

	// Create cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Start batcher
	err := batcher.Start(ctx)
	require.NoError(t, err)

	// Send messages and cancel context
	go func() {
		input <- []byte("msg1")
		input <- []byte("msg2")
		time.Sleep(10 * time.Millisecond)
		cancel() // Cancel context
	}()

	// Expect final batch due to context cancellation
	batch := <-output
	assert.Equal(t, 2, batch.Size)
	assert.Len(t, batch.Messages, 2)

	// Channel should be closed
	_, ok := <-output
	assert.False(t, ok)
}

func TestBatcher_EmptyInput(t *testing.T) {
	// Create and immediately close input channel
	input := make(chan []byte)
	close(input)

	// Create batcher
	batcher, output := NewBatcher(5, 100*time.Millisecond, input)

	// Start batcher
	ctx := context.Background()
	err := batcher.Start(ctx)
	require.NoError(t, err)

	// Output channel should be closed immediately
	_, ok := <-output
	assert.False(t, ok)
}

func TestBatcher_NilInput(t *testing.T) {
	// Create batcher with nil input
	batcher, _ := NewBatcher(5, 100*time.Millisecond, nil)

	// Start should return error
	ctx := context.Background()
	err := batcher.Start(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "input channel is nil")
}
