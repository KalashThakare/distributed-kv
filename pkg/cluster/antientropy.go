package cluster

import (
	"fmt"
	"time"
)

type AntiEntropy struct {
	cluster  *Cluster
	interval time.Duration
	stop     chan struct{}
}

func NewAntiEntropy(c *Cluster, interval time.Duration) *AntiEntropy {
	return &AntiEntropy{
		cluster:  c,
		interval: interval,
		stop:     make(chan struct{}),
	}
}

func (ae *AntiEntropy) Start() {
	go ae.loop()
}

func (ae *AntiEntropy) Stop() {
	close(ae.stop)
}

func (ae *AntiEntropy) loop() {
	ticker := time.NewTicker(ae.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ae.runSync()
		case <-ae.stop:
			return
		}
	}
}

func (ae *AntiEntropy) runSync() {
	c := ae.cluster

	localKeys := c.store.Keys()
	if len(localKeys) == 0 {
		return
	}

	synced, missing := 0, 0

	for _, key := range localKeys {
		primary := c.ring.GetNode(key)
		if primary != c.selfName {
			continue
		}

		localVal, err := c.store.Get(key)
		if err != nil {
			continue
		}

		targets := c.ring.GetN(key, ReplicationN)
		for _, nodeName := range targets {
			if nodeName == c.selfName {
				continue
			}

			cl := c.PeerClient(nodeName)
			if cl == nil {
				continue
			}

			replicaVal, found, err := cl.Get(key)
			if err != nil {
				continue
			}

			if !found || replicaVal != localVal {
				if err := cl.Put(key, localVal); err == nil {
					synced++
					if !found {
						missing++
					}
				}
			}
		}

	}

	if synced > 0 {
		fmt.Printf("[%s] anti-entropy: synced %d keys (%d were missing)\n", c.selfName, synced, missing)
	}
}
