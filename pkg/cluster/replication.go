package cluster

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// Replication constants — DynamoDB defaults.
// N=3: every key is stored on 3 nodes.
// W=2: a write needs 2 acks to be considered durable.
// R=2: a read queries 2 nodes and takes the freshest value.
// W + R = 4 > N = 3 -> reads always overlap with at least one written node.

const (
	ReplicationN = 3
	WriteQuorum  = 2
	ReadQuorum   = 2
)

// writeResult carries the outcome of one node's write attempt

type writeResult struct {
	node string
	err  error
}

// readResult carries the value returned by one node during a quorum read.

type readResult struct {
	node      string
	value     string
	found     bool
	timestamp int64
	err       error
}

// PutQuorum writes key=value to N nodes and waits for W acknowledgements.
// Returns nil if W writes succeed, error if fewer than W succeed.
// Failed nodes are automatically queued for hinted handoff.

func (c *Cluster) PutQuorum(key, value string) error {
	// Step 1: find the N nodes responsible for this key
	targets := c.ring.GetN(key, ReplicationN)
	if len(targets) == 0 {
		return fmt.Errorf("ring is empty")
	}

	// Step 2: fan out writes to all N nodes concurrently
	results := make(chan writeResult, len(targets))
	for _, nodeName := range targets {
		nodeName := nodeName

		go func() {
			err := c.writeToNode(nodeName, key, value)
			results <- writeResult{node: nodeName, err: err}
		}()

	}

	// Step 3: collect results, return as soon as quorum is reached

	success := 0
	failures := 0
	var failedNodes []string

	for range targets {
		r := <-results
		if r.err == nil {
			success++
			if success >= WriteQuorum {
				// Quorum reached — drain remaining results in background
				// so goroutines don't leak
				go func() {
					remaining := len(targets) - success - failures
					for i := 0; i < remaining; i++ {
						res := <-results
						if res.err != nil {
							c.handoff.Queue(res.node, key, value)
						}

					}
				}()
				return nil
			}
		} else {
			failures++
			failedNodes = append(failedNodes, r.node)
			// Queue hinted handoff immediately — don't wait for quorum decision
			c.handoff.Queue(r.node, key, value)

		}
	}

	return fmt.Errorf("write quorum failed: only %d/%d writes succeeded (need %d)", success, len(targets), WriteQuorum)

}

// writeToNode writes key=value to one node — self or a remote peer.

func (c *Cluster) writeToNode(nodeName, key, value string) error {
	if nodeName == c.selfName {
		return c.store.Put(key, value)
	}
	cl := c.PeerClient(nodeName)
	if cl == nil {
		return fmt.Errorf("no connection to %s", nodeName)
	}
	return cl.Put(key, value)
}

// GetQuorum reads from R nodes and returns the freshest value.
// Returns ("", false, nil) if the key does not exist on any node.

func (c *Cluster) GetQuorum(key string) (string, bool, error) {
	targets := c.ring.GetN(key, ReplicationN)
	if len(targets) == 0 {
		return "", false, fmt.Errorf("ring is empty")
	}

	// Query all N nodes concurrently (over-read for better availability)
	results := make(chan readResult, len(targets))
	for _, nodeName := range targets {
		nodeName := nodeName
		go func() {
			results <- c.readFromNode(nodeName, key)
		}()
	}

	// Collect R responses and pick the freshest value

	var response []readResult

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for len(response) < ReadQuorum {
		select {
		case r := <-results:
			if r.err == nil {
				response = append(response, r)
			}
		case <-timeoutCtx.Done():
			return "", false, fmt.Errorf("read quorum timeout: got %d/%d responses",
				len(response), ReadQuorum)

		}
	}

	//Sort by timestamp descending pick the most recent

	sort.Slice(response, func(i, j int) bool {
		return response[i].timestamp > response[j].timestamp
	})

	best := response[0]

	return best.value, best.found, nil

}

// Read from node reads as a single key from one node
// returns a readResult with the current unix nanosecond timestamps.

func (c *Cluster) readFromNode(nodeName, key string) readResult {
	ts := time.Now().UnixNano()
	if nodeName == c.selfName {
		val, err := c.store.Get(key)
		if err != nil {
			return readResult{node: nodeName, found: false, timestamp: ts}
		}
		return readResult{node: nodeName, found: true, value: val, timestamp: ts}
	}

	cl := c.PeerClient(nodeName)
	if cl == nil {
		return readResult{node: nodeName, err: fmt.Errorf("no client for %s", nodeName)}
	}

	val, found, err := cl.Get(key)
	if err != nil {
		return readResult{node: nodeName, err: err}
	}
	return readResult{node: nodeName, value: val, found: found, timestamp: ts}
}

// DeleteQuorum removes key from W nodes (quorum delete).
// Same fan-out pattern as PutQuorum.

func (c *Cluster) DeleteQuorum(key string) error {
	targets := c.ring.GetN(key, ReplicationN)
	if len(targets) == 0 {
		return fmt.Errorf("ring is empty")
	}

	results := make(chan writeResult, len(targets))
	for _, nodeName := range targets {
		nodeName := nodeName

		go func() {
			var err error
			if nodeName == c.selfName {
				c.store.Delete(key)
			} else {
				cl := c.PeerClient(nodeName)
				if cl == nil {
					err = fmt.Errorf("no client for %s", nodeName)
				} else {
					err = cl.Delete(key)
				}
			}
			results <- writeResult{node: nodeName, err: err}
		}()
	}
	successes := 0
	for range targets {
		r := <-results
		if r.err == nil {
			successes++
			if successes >= WriteQuorum {
				return nil
			}
		}
	}
	return fmt.Errorf("delete quorum failed: %d/%d", successes, len(targets))
}
