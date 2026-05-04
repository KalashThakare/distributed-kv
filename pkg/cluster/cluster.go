package cluster

import (
	"fmt"
	"sync"
	"time"

	"github.com/KalashThakare/distributed-kv/pkg/client"
	"github.com/KalashThakare/distributed-kv/pkg/ring"
	"github.com/KalashThakare/distributed-kv/pkg/store"
	"github.com/hashicorp/memberlist"
)

type Cluster struct {
	selfName string
	selfAddr string
	ring     *ring.Ring
	store    *store.Store
	handoff  *Hintedhandoff
	mu       sync.RWMutex
	ml       *memberlist.Memberlist
	peers    map[string]*client.Client
}

type Config struct {
	Name        string
	GossipPort  int
	GRPCaddress string
	Store       *store.Store
	Ring        *ring.Ring
}

func New(cfg Config) (*Cluster, error) {
	c := &Cluster{
		selfName: cfg.Name,
		selfAddr: cfg.GRPCaddress,
		ring:     cfg.Ring,
		store:    cfg.Store,
		handoff:  NewHintedHandoff(1 * time.Hour),
		peers:    make(map[string]*client.Client),
	}

	mlCfg := memberlist.DefaultLANConfig() // sensible defaults for LAN clusters
	mlCfg.Name = cfg.Name                  // node name (must be unique in cluster)
	mlCfg.BindPort = cfg.GossipPort        // UDP port to listen on
	mlCfg.AdvertisePort = cfg.GossipPort
	// Suppress memberlist's verbose logging in production.

	mlCfg.LogOutput = logSink{}

	// Register delegates:
	// nodeDelegate: provides metadata (gRPC addr) to gossip
	// eventDelegate: reacts to join/leave events

	mlCfg.Delegate = &nodeDelegate{
		meta: []byte(cfg.GRPCaddress),
	}
	mlCfg.Events = &eventDelegate{
		cluster: c,
	}

	// Create the memberlist instance — this starts the gossip listener

	ml, err := memberlist.Create(mlCfg)
	if err != nil {
		return nil, fmt.Errorf("create memberlist: %w", err)
	}

	c.ml = ml
	return c, nil
}

// logSink discards memberlist's internal log output.
// Replace with a real writer (os.Stderr) for debugging gossip issues.
type logSink struct{}

func (logSink) Write(p []byte) (int, error) {
	return len(p), nil
}

func (c *Cluster) onNodeJoin(name, grpcAddr string) {
	c.ring.AddNode(name)

	cl, err := client.New(grpcAddr)
	if err != nil {
		fmt.Printf("[%s] WARNING: could not dial %s (%s): %v\n", c.selfName, name, grpcAddr, err)
		return
	}

	c.mu.Lock()
	c.peers[name] = cl
	c.mu.Unlock()

	go func ()  {
		if err := c.handoff.Replay(name, cl); err != nil{
			fmt.Printf("[%s] hint replay for %s: %v\n", c.selfName, name, err)
		}
	}()

	fmt.Printf("[%s] ring now has %d nodes\n", c.selfName, c.ring.Size())
}

func (c *Cluster) onNodeLeave(name string) {
	c.ring.RemoveNode(name)

	c.mu.Lock()
	if cl, ok := c.peers[name]; ok {
		_ = cl.Close()
		delete(c.peers, name)
	}
	c.mu.Unlock()

	fmt.Printf("[%s] ring now has %d nodes\n", c.selfName, c.ring.Size())
}

// Join connects this node to an existing cluster by contacting seedAddrs.
// seedAddrs are gossip addresses (host:port) of any existing node(s).
// You only need ONE seed — gossip will propagate the full membership.
// Returns nil immediately if seedAddrs is empty (single-node mode).

func (c *Cluster) Join(seedAddres []string) error {

	if len(seedAddres) == 0 {
		return nil
	}

	n, err := c.ml.Join(seedAddres)
	if err != nil {
		return fmt.Errorf("join cluster: %w", err)
	}

	fmt.Printf("[%s] joined cluster, synced with %d nodes\n", c.selfName, n)
	return nil

}

// Broadcasts a "leaving" message so peers know immediately (vs waiting fortimeout).
// timeout is how long to wait for the broadcast to propagate.
func (c *Cluster) Leave(timeout time.Duration) error {

	return c.ml.Leave(timeout)

}

// Shutting down tears down the gossip listener. Call after Leave().

func (c *Cluster) Shutdown() error {
	// Closing all peer gRPC connections
	c.mu.Lock()
	for name, cl := range c.peers {
		_ = cl.Close()
		delete(c.peers, name)
	}
	c.mu.Unlock()

	return c.ml.Shutdown()

}

func (c *Cluster) put(key, value string) error {
	owner := c.ring.GetNode(key)

	if owner == c.selfName || owner == "" {
		return c.store.Put(key, value)
	}

	// Another node owns the key — forward over gRPC

	c.mu.RLock()
	cl, ok := c.peers[owner]
	c.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no connection to node %s (owner of %q)", owner, key)
	}

	return cl.Put(key, value)

}

func (c *Cluster) Get(key string) (string, bool, error) {
	owner := c.ring.GetNode(key)

	if owner == c.selfName || owner == "" {
		val, err := c.store.Get(key)
		if err != nil {
			return "", false, nil
		}

		return val, true, nil
	}

	c.mu.RLock()
	cl, ok := c.peers[owner]
	c.mu.RUnlock()

	if !ok {
		return "", false, fmt.Errorf("no connection to node %s", owner)
	}

	return cl.Get(key)

}

func (c *Cluster) Delete(key string) error {
	owner := c.ring.GetNode(key)

	if owner == c.selfName || owner == "" {
		return c.store.Delete(key)
	}

	c.mu.RLock()
	cl, ok := c.peers[owner]
	c.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no connection to node %s", owner)
	}

	return cl.Delete(key)

}

//====================  Members and utility methods =====================

func (c *Cluster) Members() []string {
	nodes := c.ml.Members()
	names := make([]string, 0, len(nodes))

	for _, n := range nodes {
		names = append(names, n.Name)
	}

	return names
}

func (c *Cluster) MembersCount() int {
	count := c.ml.NumMembers()

	return count
}

// LocalNode returns the memberlist.Node for this node.
// Useful for logging the gossip address.
func (c *Cluster) LocalNode() *memberlist.Node {
	return c.ml.LocalNode()
}

// PeerClient returns the gRPC client for a named peer.
// Returns nil if the peer is unknown or not connected.
// Used by the replication layer on Day 5.
func (c *Cluster) PeerClient(name string) *client.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.peers[name]
}
