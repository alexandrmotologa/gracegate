package proxy

import "sync"

const (
	// DefaultBufferSize represents the standard buffer size for stream copying (32KB).
	DefaultBufferSize = 32 * 1024
)

// BufferPool manages reusable byte slices to minimize heap allocations and GC pressure.
type BufferPool struct {
	pool sync.Pool
	size int
}

// NewBufferPool creates a pool that yields byte slices of the specified size.
func NewBufferPool(size int) *BufferPool {
	if size <= 0 {
		size = DefaultBufferSize
	}
	return &BufferPool{
		size: size,
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, size)
				return &b
			},
		},
	}
}

// Get retrieves a buffer pointer from the pool.
func (p *BufferPool) Get() *[]byte {
	return p.pool.Get().(*[]byte)
}

// Put returns a buffer pointer to the pool.
func (p *BufferPool) Put(b *[]byte) {
	if b == nil || len(*b) != p.size {
		return
	}
	p.pool.Put(b)
}
