package batch

// BufferManager provides simple buffer reuse to reduce allocations
type BufferManager struct {
	buffers chan []byte
	size    int
}

// NewBufferManager creates a new buffer manager with the specified pool size and buffer size
func NewBufferManager(poolSize, bufferSize int) *BufferManager {
	if poolSize <= 0 {
		poolSize = 10 // Default pool size
	}
	if bufferSize <= 0 {
		bufferSize = 1024 // Default buffer size
	}

	return &BufferManager{
		buffers: make(chan []byte, poolSize),
		size:    bufferSize,
	}
}

// Get retrieves a buffer from the pool or creates a new one if pool is empty
func (bm *BufferManager) Get() []byte {
	select {
	case buf := <-bm.buffers:
		return buf
	default:
		// Pool is empty, create new buffer
		return make([]byte, 0, bm.size)
	}
}

// Put returns a buffer to the pool after resetting it
// The buffer is only returned to the pool if it has the expected capacity
func (bm *BufferManager) Put(buf []byte) {
	if buf == nil {
		return
	}

	// Reset the buffer but keep the capacity
	buf = buf[:0]

	// Only return buffers with expected capacity to prevent pool pollution
	if cap(buf) == bm.size {
		select {
		case bm.buffers <- buf:
			// Buffer returned to pool successfully
		default:
			// Pool is full, let buffer be garbage collected
		}
	}
}
