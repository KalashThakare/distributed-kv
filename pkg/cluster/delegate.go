// ================================ Responsible for updating the hash ring and manage gRPC connections====================================

package cluster

import (
	"fmt"

	"github.com/hashicorp/memberlist"
)

type eventDelegate struct {
	cluster *Cluster
}

type nodeDelegate struct {
	meta []byte // gRPC address, e.g. "192.168.1.1:7001"
}

// NotifyJoin

func (d *eventDelegate) NotifyJoin(n *memberlist.Node) {
	if n.Name == d.cluster.selfName {
		return
	}

	grpcAddr := string(n.Meta)

	if grpcAddr == "" {
		fmt.Printf("[%s] WARNING: node %s joined with no gRPC address\n",
			d.cluster.selfName, n.Name)
		return
	}
	fmt.Printf("[%s] node joined: %s at %s\n",
		d.cluster.selfName, n.Name, grpcAddr)

	// notify the cluster via go routine for keeping things non blocking.

	go d.cluster.onNodeJoin(n.Name, grpcAddr)
}

func (d *eventDelegate) NotifyLeave(n *memberlist.Node) {

	if n.Name == d.cluster.selfName {
		return
	}

	fmt.Printf("[%s] node left: %s\n", d.cluster.selfName, n.Name)
	go d.cluster.onNodeLeave(n.Name)

}

func (d *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	// No-op for now. Day 5 uses this for replication state updates.
}

func (d *nodeDelegate) NodeMeta(limit int) []byte {

	if len(d.meta) > limit {
		return d.meta[:limit]
	}
	return d.meta

}

func (d *nodeDelegate) NotifyMsg([]byte)                           {}
func (d *nodeDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (d *nodeDelegate) LocalState(join bool) []byte                { return nil }
func (d *nodeDelegate) MergeRemoteState(buf []byte, join bool)     {}
