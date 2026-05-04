/*

Hinted handoff stores writes that failed for a specific node. When that node recovers (gossip fires
NotifyJoin again), we replay all buffered writes to it. This is how DynamoDB guarantees eventual
consistency even when nodes restart.

*/

package cluster

import (
	"fmt"
	"sync"
	"time"
)

// Hint represents one missed write for a specific node.

type Hint struct {
	Key       string
	Value     string
	Op        string    // "PUT" or "delete"
	Timestamp time.Time // Time when hint was created
}

type Hintedhandoff struct {
	mu     sync.Mutex
	hints  map[string][]Hint
	maxAge time.Duration
}

func NewHintedHandoff(maxAge time.Duration) *Hintedhandoff {
	return &Hintedhandoff{
		hints:  make(map[string][]Hint),
		maxAge: maxAge,
	}
}

func (h *Hintedhandoff) Queue(nodeName, key, value string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hints[nodeName] = append(h.hints[nodeName], Hint{
		Key:       key,
		Value:     value,
		Op:        "put",
		Timestamp: time.Now(),
	})

	if len(h.hints[nodeName]) > 10000 {
		h.hints[nodeName] = h.hints[nodeName][1000:]
	}
}

func (h *Hintedhandoff) QueueDelete(nodeName, key string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.hints[nodeName] = append(h.hints[nodeName], Hint{
		Key:       key,
		Op:        "delete",
		Timestamp: time.Now(),
	})
}

func (h *Hintedhandoff) Pending(nodeName string) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.hints[nodeName])
}

/*
 VERY important when node comes back from failure state then the data (hints) which was yet to be delivered
 to that node gets delivered. And jo stale hints the pending vale unhe delete kr do..

*/

func (h *Hintedhandoff) Replay(nodeName string, cl interface {
	Put(key, value string) error
	Delete(key string) error
}) error {

	h.mu.Lock()
	pending := h.hints[nodeName]

	delete(h.hints, nodeName) // deletes map and key

	h.mu.Unlock()

	if len(pending) == 0 {
		return nil
	}

	cutoff := time.Now().Add(-h.maxAge)
	replayed, skipped := 0, 0

	for _, hint := range pending {
		if hint.Timestamp.Before(cutoff) {
			skipped++
			continue
		}

		var err error
		switch hint.Op {
		case "put":
			err = cl.Put(hint.Key, hint.Value)
		case "delete":
			err = cl.Delete(hint.Key)
		}

		if err != nil {
			h.mu.Lock()
			h.hints[nodeName] = append(h.hints[nodeName], hint)
			h.mu.Unlock()
			return fmt.Errorf("replay hint for %s: %w", nodeName, err)
		}

		replayed++
	}

	fmt.Printf("[handoff] replayed %d hints to %s (%d stale skipped)\n", replayed, nodeName, skipped)
	return nil
}
