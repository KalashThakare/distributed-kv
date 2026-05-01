/*

Hinted handoff stores writes that failed for a specific node. When that node recovers (gossip fires
NotifyJoin again), we replay all buffered writes to it. This is how DynamoDB guarantees eventual
consistency even when nodes restart.

*/

package cluster

import "time"

// Hint represents one missed write for a specific node.

type Hint struct {
	Key       string
	Value     string
	Op        string    // "PUT" or "delete"
	Timestamp time.Time // Time when hint was created
}


