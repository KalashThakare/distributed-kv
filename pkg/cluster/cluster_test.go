package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/KalashThakare/distributed-kv/pkg/ring"
	"github.com/KalashThakare/distributed-kv/pkg/store"
)

type testNode struct {
	cluster    *Cluster
	gossipPort int
	gossipAddr string
}

// grpcAddr is a fake address (no real gRPC server in this test)

func newTestNode(t *testing.T, name string, gossipPort int, grpcAddr string) *testNode {
	t.Helper()

	st, err := store.Open(store.Config{})
	if err != nil {
		t.Fatalf("open store for %s: %v", name, err)
	}

	r := ring.New()
	r.AddNode(name)

	c, err := New(Config{
		Name:        name,
		Ring:        r,
		GossipPort:  gossipPort,
		GRPCaddress: grpcAddr,
		Store:       st,
	})

	if err != nil {
		t.Fatalf("new cluster %s: %v", name, err)
	}

	t.Cleanup(func() {
		_ = c.Leave(500 * time.Millisecond)
		_ = c.Shutdown()
		_ = st.Close()
	})

	return &testNode{
		cluster:    c,
		gossipPort: gossipPort,
		gossipAddr: fmt.Sprintf("127.0.0.1:%d", gossipPort),
	}
}

//================== Test: 3-node cluster converges =======================

/*

Three nodes start independently, NodeB and NodeC join via
NodeA's seed address, and we verify that all three see each other in their membership lists within 2
seconds.

*/

func TestCluster_ThreeNodeConvergence(t *testing.T) {

	// strt 3 independent nodes(Servers)

	nodeA := newTestNode(t, "NodeA", 17001, "127.0.0.1:7001")
	nodeB := newTestNode(t, "NodeB", 17002, "127.0.0.1:7002")
	nodeC := newTestNode(t, "NodeC", 17003, "127.0.0.1:7003")

	// NodeB and NodeC joins via NodeA's gossip address

	if err := nodeB.cluster.Join([]string{nodeA.gossipAddr}); err != nil {
		t.Fatalf("NodeB join: %v", err)
	}

	if err := nodeC.cluster.Join([]string{nodeA.gossipAddr}); err != nil {
		t.Fatalf("NodeC join: %v", err)
	}

	// Wait for gossip to propagate

	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		aCount := nodeA.cluster.MembersCount()
		bCount := nodeB.cluster.MembersCount()
		cCount := nodeC.cluster.MembersCount()

		if aCount == 3 && bCount == 3 && cCount == 3 {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	// Verify all three nodes see all three members
	for _, node := range []*testNode{nodeA, nodeB, nodeC} {
		count := node.cluster.MembersCount()
		if count != 3 {
			t.Errorf("node %s sees %d members, want 3",
				node.cluster.selfName, count)
		}
	}

	// Verify the ring on each node has all three

	for _, node := range []*testNode{nodeA, nodeB, nodeC} {
		size := node.cluster.ring.Size()
		if size != 3 {
			t.Errorf("node %s ring has %d nodes, want 3",
				node.cluster.selfName, size)
		}
	}

	t.Logf("3-node cluster converged. NodeA members: %v", nodeA.cluster.Members())
}

//=========================  Test: node leaves, ring shrinks ===================================

func TestCluster_NodeLeave(t *testing.T) {
	nodeA := newTestNode(t, "NodeA", 17011, "127.0.0.1:7011")
	nodeB := newTestNode(t, "NodeB", 17012, "127.0.0.1:7012")
	if err := nodeB.cluster.Join([]string{nodeA.gossipAddr}); err != nil {
		t.Fatalf("join: %v", err)
	}
	// Wait for convergence to 2 members
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if nodeA.cluster.MembersCount() == 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if nodeA.cluster.MembersCount() != 2 {
		t.Fatal("cluster did not converge to 2 members")
	}
	// NodeB leaves gracefully
	if err := nodeB.cluster.Leave(500 * time.Millisecond); err != nil {
		t.Fatalf("leave: %v", err)
	}
	// Wait for NodeA to detect the departure
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if nodeA.cluster.MembersCount() == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := nodeA.cluster.MembersCount(); got != 1 {
		t.Errorf("after leave: NodeA sees %d members, want 1", got)
	}
	if got := nodeA.cluster.ring.Size(); got != 1 {
		t.Errorf("after leave: ring has %d nodes, want 1", got)
	}
	t.Logf("NodeB left. NodeA ring size: %d", nodeA.cluster.ring.Size())
}
func TestCluster_SingleNode(t *testing.T) {
	node := newTestNode(t, "NodeA", 17021, "127.0.0.1:7021")
	// Single node with no peers — should work fine
	if err := node.cluster.Join(nil); err != nil {
		t.Fatalf("join (empty peers): %v", err)
	}
	if node.cluster.MembersCount() != 1 {
		t.Errorf("want 1 member, got %d", node.cluster.MembersCount())
	}
	if node.cluster.ring.Size() != 1 {
		t.Errorf("want ring size 1, got %d", node.cluster.ring.Size())
	}
}
