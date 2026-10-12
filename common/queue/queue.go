// Package queue provides an unbounded FIFO between goroutines that must not
// block each other: producers push without waiting, and one consumer drains.
//
// It exists for gRPC streams. Only one goroutine may call Send on a stream,
// and whoever produces a message (often while holding a lock) must not wait
// for a slow peer.
package queue

import "sync"

// Queue is an unbounded FIFO. The zero value is not usable; call New.
type Queue[T any] struct {
	mu    sync.Mutex
	items []T
	wake  chan struct{}
}

// New returns an empty queue.
func New[T any]() *Queue[T] {
	return &Queue[T]{wake: make(chan struct{}, 1)}
}

// Push appends v. It never blocks.
func (q *Queue[T]) Push(v T) {
	q.mu.Lock()
	q.items = append(q.items, v)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// Wake is signalled after a Push. A consumer waits on it and then calls Drain;
// one signal may stand for several pushes.
func (q *Queue[T]) Wake() <-chan struct{} { return q.wake }

// Drain removes and returns everything queued, oldest first.
func (q *Queue[T]) Drain() []T {
	q.mu.Lock()
	defer q.mu.Unlock()
	items := q.items
	q.items = nil
	return items
}
