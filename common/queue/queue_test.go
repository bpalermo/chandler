package queue

import (
	"sync"
	"testing"
	"time"
)

func TestQueueKeepsOrderAndNeverBlocks(t *testing.T) {
	q := New[int]()
	for i := range 100 {
		q.Push(i) // nobody is draining; must not block
	}
	select {
	case <-q.Wake():
	default:
		t.Fatal("no wake after pushes")
	}
	got := q.Drain()
	if len(got) != 100 {
		t.Fatalf("drained %d, want 100", len(got))
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("item %d = %d", i, v)
		}
	}
	if len(q.Drain()) != 0 {
		t.Error("second drain is not empty")
	}
}

func TestQueueDeliversEverythingToAWaitingConsumer(t *testing.T) {
	q := New[int]()
	const producers, each = 8, 200
	var wg sync.WaitGroup
	for range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				q.Push(i)
			}
		}()
	}
	seen := 0
	deadline := time.After(10 * time.Second)
	for seen < producers*each {
		select {
		case <-q.Wake():
			seen += len(q.Drain())
		case <-deadline:
			t.Fatalf("saw %d of %d", seen, producers*each)
		}
	}
	wg.Wait()
}
