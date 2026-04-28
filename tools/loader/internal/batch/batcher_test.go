package batch

import (
	"context"
	"testing"
	"time"

	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatcher_Losslessness(t *testing.T) {
	// Test that all input messages appear in output batches
	input := make(chan internal.Message, 10)
	batcher, output := NewBatcher(3, 1*time.Second, input)

	ctx := context.Background()
	err := batcher.Start(ctx)
	require.NoError(t, err)

	// Send 5 messages + tombstone
	messages := [][]byte{[]byte("msg1"), []byte("msg2"), []byte("msg3"), []byte("msg4"), []byte("msg5")}
	go func() {
		defer close(input)
		for _, msg := range messages {
			input <- internal.Message{
				Data:   msg,
				Format: internal.FormatJSONLines,
				Type:   internal.MessageTypeData,
			}
		}
		// Send tombstone
		input <- internal.Message{
			Data:   nil,
			Format: internal.FormatJSONLines,
			Type:   internal.MessageTypeTombstone,
		}
	}()

	// Collect all output messages
	var outputMsgs [][]byte
	var tombstoneReceived bool
	for batch := range output {
		if batch.IsTombstone {
			tombstoneReceived = true

			break
		}
		outputMsgs = append(outputMsgs, batch.Messages...)
	}

	// Verify all input messages are present in output
	assert.Equal(t, len(messages), len(outputMsgs))
	assert.True(t, tombstoneReceived, "Tombstone should be received")
	for i, msg := range messages {
		assert.Equal(t, msg, outputMsgs[i])
	}
}

func TestBatcher_MaxBatchSize(t *testing.T) {
	// Test that batch size never exceeds configured maximum
	input := make(chan internal.Message, 20)
	maxSize := 3
	batcher, output := NewBatcher(maxSize, 1*time.Second, input)

	ctx := context.Background()
	err := batcher.Start(ctx)
	require.NoError(t, err)

	// Send many messages + tombstone
	go func() {
		defer close(input)
		for range 10 {
			input <- internal.Message{
				Data:   []byte("msg"),
				Format: internal.FormatJSONLines,
				Type:   internal.MessageTypeData,
			}
		}
		// Send tombstone
		input <- internal.Message{
			Data:   nil,
			Format: internal.FormatJSONLines,
			Type:   internal.MessageTypeTombstone,
		}
	}()

	// Verify no batch exceeds max size
	for batch := range output {
		if batch.IsTombstone {
			break
		}
		assert.LessOrEqual(t, batch.Size, maxSize, "Batch size exceeded maximum")
		assert.LessOrEqual(t, len(batch.Messages), maxSize, "Messages count exceeded maximum")
	}
}
