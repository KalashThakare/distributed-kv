package cluster

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KalashThakare/distributed-kv/pkg/pb"
	"github.com/KalashThakare/distributed-kv/pkg/ring"
	"github.com/KalashThakare/distributed-kv/pkg/server"
	"github.com/KalashThakare/distributed-kv/pkg/store"
	"google.golang.org/grpc"
)

type replicationNode struct {
	name       string
	cluster    *Cluster
	ring       *ring.Ring
	store      *store.Store
	grpcAddr   string
	grpcLis    net.Listener
	grpcServer *grpc.Server
	gossipPort int
	gossipAddr string
}

func newReplicationNode(t *testing.T, name string) *replicationNode {
	t.Helper()

	st, err := store.Open(store.Config{})
	if err != nil {
		t.Fatalf("open store for %s: %v", name, err)
	}

	r := ring.New()
	r.AddNode(name)

	node := &replicationNode{
		name:       name,
		store:      st,
		ring:       r,
		gossipPort: freeUDPPort(t),
	}
	node.gossipAddr = fmt.Sprintf("127.0.0.1:%d", node.gossipPort)

	node.startGRPC(t)

	cl, err := New(Config{
		Name:        name,
		GossipPort:  node.gossipPort,
		GRPCaddress: node.grpcAddr,
		Store:       st,
		Ring:        r,
	})
	if err != nil {
		t.Fatalf("new cluster %s: %v", name, err)
	}

	node.cluster = cl

	t.Cleanup(func() {
		node.stopGRPC()
		_ = cl.Leave(500 * time.Millisecond)
		_ = cl.Shutdown()
		_ = st.Close()
	})

	return node
}

func (n *replicationNode) startGRPC(t *testing.T) {
	t.Helper()

	var err error
	if n.grpcAddr == "" {
		n.grpcLis, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen gRPC for %s: %v", n.name, err)
		}
		n.grpcAddr = n.grpcLis.Addr().String()
	} else {
		n.grpcLis, err = net.Listen("tcp", n.grpcAddr)
		if err != nil {
			t.Fatalf("listen gRPC for %s: %v", n.name, err)
		}
	}

	n.grpcServer = grpc.NewServer()
	handler := server.New(server.Config{
		Name:  n.name,
		Store: n.store,
		Ring:  n.ring,
	})
	pb.RegisterKVStoreServer(n.grpcServer, handler)

	go func() {
		_ = n.grpcServer.Serve(n.grpcLis)
	}()
}

func (n *replicationNode) stopGRPC() {
	if n.grpcServer != nil {
		n.grpcServer.Stop()
		n.grpcServer = nil
	}
	if n.grpcLis != nil {
		_ = n.grpcLis.Close()
		n.grpcLis = nil
	}
}

func buildCluster(t *testing.T, n int) []*replicationNode {
	t.Helper()
	if n < 1 {
		t.Fatalf("invalid node count: %d", n)
	}

	nodes := make([]*replicationNode, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("Node%c", 'A'+i)
		nodes = append(nodes, newReplicationNode(t, name))
	}

	seed := nodes[0].gossipAddr
	for i := 1; i < n; i++ {
		if err := nodes[i].cluster.Join([]string{seed}); err != nil {
			t.Fatalf("join %s: %v", nodes[i].name, err)
		}
	}

	waitConverge(t, nodes, 5*time.Second)
	return nodes
}

func waitConverge(t *testing.T, nodes []*replicationNode, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if allConverged(nodes) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("cluster did not converge within %s: %s", timeout, convergeSummary(nodes))
}

func allConverged(nodes []*replicationNode) bool {
	n := len(nodes)
	for _, node := range nodes {
		if node.cluster.MembersCount() != n {
			return false
		}
		if node.cluster.ring.Size() != n {
			return false
		}
	}
	return true
}

func convergeSummary(nodes []*replicationNode) string {
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		parts = append(parts, fmt.Sprintf("%s members=%d ring=%d", node.name, node.cluster.MembersCount(), node.cluster.ring.Size()))
	}
	return strings.Join(parts, "; ")
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("%s", msg)
}

func waitForValue(t *testing.T, st *store.Store, key, want string, timeout time.Duration) {
	t.Helper()

	waitUntil(t, timeout, func() bool {
		val, err := st.Get(key)
		return err == nil && val == want
	}, fmt.Sprintf("timed out waiting for %q=%q", key, want))
}

func nodeByName(nodes []*replicationNode, name string) *replicationNode {
	for _, node := range nodes {
		if node.name == name {
			return node
		}
	}
	return nil
}

func dropPeer(c *Cluster, name string) {
	c.mu.Lock()
	if cl, ok := c.peers[name]; ok {
		_ = cl.Close()
		delete(c.peers, name)
	}
	c.mu.Unlock()
}

func freeUDPPort(t *testing.T) int {
	t.Helper()

	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen UDP: %v", err)
	}
	defer conn.Close()

	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("unexpected UDP addr type: %T", conn.LocalAddr())
	}
	return addr.Port
}

type mockHandoffClient struct {
	mu      sync.Mutex
	puts    map[string]string
	deletes map[string]bool
}

func (m *mockHandoffClient) Put(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.puts == nil {
		m.puts = make(map[string]string)
	}
	m.puts[key] = value
	return nil
}

func (m *mockHandoffClient) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deletes == nil {
		m.deletes = make(map[string]bool)
	}
	m.deletes[key] = true
	return nil
}

func TestQuorum_BasicPutGet(t *testing.T) {
	nodes := buildCluster(t, 3)

	key := "basic-key"
	value := "value-1"

	if err := nodes[0].cluster.PutQuorum(key, value); err != nil {
		t.Fatalf("put quorum: %v", err)
	}

	got, found, err := nodes[2].cluster.GetQuorum(key)
	if err != nil {
		t.Fatalf("get quorum: %v", err)
	}
	if !found {
		t.Fatalf("key not found, want %q", value)
	}
	if got != value {
		t.Fatalf("unexpected value: got %q want %q", got, value)
	}
}

func TestQuorum_Delete(t *testing.T) {
	nodes := buildCluster(t, 3)

	key := "delete-key"
	value := "delete-value"

	if err := nodes[0].cluster.PutQuorum(key, value); err != nil {
		t.Fatalf("put quorum: %v", err)
	}

	if err := nodes[1].cluster.DeleteQuorum(key); err != nil {
		t.Fatalf("delete quorum: %v", err)
	}

	waitUntil(t, 2*time.Second, func() bool {
		val, found, err := nodes[2].cluster.GetQuorum(key)
		return err == nil && !found && val == ""
	}, "expected key to be deleted from quorum")
}

func TestHintedHandoff_Replay(t *testing.T) {
	handoff := NewHintedHandoff(1 * time.Hour)
	handoff.Queue("NodeB", "k1", "v1")
	handoff.QueueDelete("NodeB", "k2")

	mock := &mockHandoffClient{}
	if err := handoff.Replay("NodeB", mock); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if got := mock.puts["k1"]; got != "v1" {
		t.Fatalf("unexpected put value: got %q want %q", got, "v1")
	}
	if !mock.deletes["k2"] {
		t.Fatalf("expected delete to be called for k2")
	}
	if pending := handoff.Pending("NodeB"); pending != 0 {
		t.Fatalf("expected no pending hints, got %d", pending)
	}
}

func TestChaos_KillNodeMidWrite(t *testing.T) {
	nodes := buildCluster(t, 3)
	writer := nodes[0]
	victim := nodes[1]
	reader := nodes[2]

	victim.stopGRPC()
	dropPeer(writer.cluster, victim.name)

	key := "chaos-key"
	value := "chaos-value"

	if err := writer.cluster.PutQuorum(key, value); err != nil {
		t.Fatalf("put quorum: %v", err)
	}

	got, found, err := reader.cluster.GetQuorum(key)
	if err != nil {
		t.Fatalf("get quorum: %v", err)
	}
	if !found || got != value {
		t.Fatalf("unexpected read after failure: found=%v value=%q", found, got)
	}

	waitUntil(t, 3*time.Second, func() bool {
		return writer.cluster.handoff.Pending(victim.name) > 0
	}, "expected hinted handoff to queue a write")

	victim.startGRPC(t)
	writer.cluster.onNodeJoin(victim.name, victim.grpcAddr)

	waitForValue(t, victim.store, key, value, 5*time.Second)
	waitUntil(t, 2*time.Second, func() bool {
		return writer.cluster.handoff.Pending(victim.name) == 0
	}, "expected hinted handoff to drain after replay")
}

func TestAntiEntropy_RepairsReplica(t *testing.T) {
	nodes := buildCluster(t, 3)

	key := "entropy-key"
	value := "entropy-value"

	primaryName := nodes[0].cluster.ring.GetNode(key)
	primary := nodeByName(nodes, primaryName)
	if primary == nil {
		t.Fatalf("primary %s not found", primaryName)
	}

	if err := primary.store.Put(key, value); err != nil {
		t.Fatalf("direct put: %v", err)
	}

	primary.cluster.ae.runSync()

	targets := primary.cluster.ring.GetN(key, ReplicationN)
	for _, nodeName := range targets {
		if nodeName == primary.name {
			continue
		}
		node := nodeByName(nodes, nodeName)
		if node == nil {
			t.Fatalf("replica %s not found", nodeName)
		}
		waitForValue(t, node.store, key, value, 3*time.Second)
	}
}
