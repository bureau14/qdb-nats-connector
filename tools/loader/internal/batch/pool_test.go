package batch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferManager_GetPut(t *testing.T) {
	// Create buffer manager
	bm := NewBufferManager(3, 1024)

	// Get buffer (should create new one)
	buf1 := bm.Get()
	assert.NotNil(t, buf1)
	assert.Equal(t, 0, len(buf1))
	assert.Equal(t, 1024, cap(buf1))

	// Use buffer
	buf1 = append(buf1, []byte("test data")...)
	assert.Equal(t, 9, len(buf1))

	// Put buffer back
	bm.Put(buf1)

	// Get buffer again (should reuse)
	buf2 := bm.Get()
	assert.NotNil(t, buf2)
	assert.Equal(t, 0, len(buf2)) // Should be reset
	assert.Equal(t, 1024, cap(buf2))
}

func TestBufferManager_PoolFull(t *testing.T) {
	// Create small buffer manager
	bm := NewBufferManager(2, 1024)

	// Fill the pool
	buf1 := bm.Get()
	buf2 := bm.Get()
	buf3 := bm.Get()

	// Put all buffers back
	bm.Put(buf1)
	bm.Put(buf2)
	bm.Put(buf3) // This should be dropped as pool is full (size 2)

	// Get buffers (should get 2 from pool, 1 new)
	retrieved1 := bm.Get()
	retrieved2 := bm.Get()
	retrieved3 := bm.Get() // This should be newly created

	assert.Equal(t, 1024, cap(retrieved1))
	assert.Equal(t, 1024, cap(retrieved2))
	assert.Equal(t, 1024, cap(retrieved3))
}

func TestBufferManager_WrongCapacity(t *testing.T) {
	bm := NewBufferManager(2, 1024)

	// Create buffer with wrong capacity
	wrongBuf := make([]byte, 0, 512)

	// Put wrong capacity buffer (should be ignored)
	bm.Put(wrongBuf)

	// Get buffer (should create new one, not reuse wrong capacity)
	buf := bm.Get()
	assert.Equal(t, 1024, cap(buf))
}

func TestBufferManager_NilBuffer(t *testing.T) {
	bm := NewBufferManager(2, 1024)

	// Put nil buffer (should not panic)
	bm.Put(nil)

	// Get buffer should still work
	buf := bm.Get()
	assert.NotNil(t, buf)
	assert.Equal(t, 1024, cap(buf))
}

func TestBufferManager_DefaultValues(t *testing.T) {
	// Test with zero/negative values
	bm := NewBufferManager(0, 0)

	buf := bm.Get()
	assert.NotNil(t, buf)
	assert.Equal(t, 0, len(buf))
	assert.Equal(t, 1024, cap(buf)) // Should use default
}
